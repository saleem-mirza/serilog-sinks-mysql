using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Serilog;
using Serilog.Debugging;

namespace SmokeTest;

internal static class Program
{
    private static string ConnectionString;

    // Points at a port that is not listening so every write fails fast.
    private static string BadConnectionString =>
        "Server=127.0.0.1;Port=3307;Database=nonexistent;Uid=nobody;Pwd=bad;" +
        "AllowPublicKeyRetrieval=True;SslMode=None;Connect Timeout=2;";

    private static int Main()
    {
        Console.WriteLine("=== Serilog MySQL Sink Smoke Test ===");
        Console.WriteLine("Press Enter to accept the default value shown in [brackets].\n");

        var server   = Prompt("Server",   "localhost");
        var port     = Prompt("Port",     "3306");
        var database = Prompt("Database", "serilog_smoketest");
        var user     = Prompt("Username", "root");
        var password = PromptPassword("Password", "P@ssword");

        ConnectionString =
            $"Server={server};Port={port};Database={database};Uid={user};Pwd={password};" +
            "AllowPublicKeyRetrieval=True;SslMode=None;";

        if (!EnsureDatabase(server, port, database, user, password)) return 1;

        var scenarios = new (string Name, Func<bool> Run)[]
        {
            ("happy-path",          HappyPath),
            ("table-name-quoting",  TableNameQuoting),
            ("connection-disposal", ConnectionDisposal),
            ("stress-50",           Stress50),
            ("timer-flush",         TimerFlush),
            ("partial-batch-dispose", PartialBatchDispose),
            ("concurrent-emitters", ConcurrentEmitters),
            ("high-burst",          HighBurst),
            ("exception-column",    ExceptionColumn),
            ("retry-backoff",       RetryBackoff),
            ("batch-drop",          BatchDrop),
        };

        var allOk = true;
        foreach (var (name, run) in scenarios)
        {
            Console.WriteLine($"\n=== {name} ===");
            try
            {
                if (!run())
                {
                    allOk = false;
                    Console.Error.WriteLine($"--- {name} FAILED ---");
                }
            }
            catch (Exception ex)
            {
                allOk = false;
                Console.Error.WriteLine($"--- {name} THREW: {ex.Message} ---");
            }
        }

        Console.WriteLine(allOk ? "\nSMOKE PASS" : "\nSMOKE FAIL");
        return allOk ? 0 : 1;
    }

    // -------------------------------------------------------------------------
    // Scenario 1: happy path
    // Validates bug fix: StringWriter object was set as @msg parameter value
    // instead of .ToString(), so Message stored "System.IO.StringWriter".
    // -------------------------------------------------------------------------
    private static bool HappyPath()
    {
        const string table = "smoke_happy";
        const int count = 1_000;
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: 100)
            .CreateLogger();

        for (var i = 0; i < count; i++)
            logger.Information("Smoke event {Index} with {@Payload}", i, new { A = i, B = $"text-{i}" });

        ((IDisposable)logger).Dispose();

        using var conn = OpenConnection();
        return
            Check(conn, $"SELECT COUNT(*) FROM `{table}`;", v =>
            {
                var n = Convert.ToInt32(v);
                return n == count ? null : $"expected {count} rows, got {n}";
            }) &&
            Check(conn, $"SELECT Message FROM `{table}` LIMIT 1;", v =>
            {
                var s = v as string;
                if (string.IsNullOrEmpty(s))         return "Message column is empty";
                if (s.Contains("StringWriter"))      return $"Message contains StringWriter type name: {s}";
                if (!s.Contains("Smoke event"))      return $"Message not rendered correctly: {s}";
                return null;
            }) &&
            Check(conn, $"SELECT Properties FROM `{table}` WHERE Properties IS NOT NULL LIMIT 1;", v =>
            {
                var s = v as string;
                if (string.IsNullOrEmpty(s))    return "Properties column is empty";
                if (!s.Contains("\"Index\""))   return $"missing Index field in: {s}";
                if (!s.Contains("\"Payload\"")) return $"missing Payload field in: {s}";
                return null;
            });
    }

    // -------------------------------------------------------------------------
    // Scenario 2: table name quoting
    // Validates bug fix: tableName was interpolated unquoted into SQL, allowing
    // injection. Both CREATE TABLE and INSERT now backtick-quote the identifier.
    //
    // batchSize:1 is intentional — with only 1 event, EventPump flushes
    // immediately on enqueue, sidestepping the known partial-batch dispose race.
    // -------------------------------------------------------------------------
    private static bool TableNameQuoting()
    {
        var cases = new[]
        {
            ("space in name",    "my log table"),
            ("reserved keyword", "select"),
            ("hyphenated",       "my-log-table"),
            ("backtick escaped", "logs`v2"),
        };

        var ok = true;
        foreach (var (label, table) in cases)
        {
            DropTable(table);

            var logger = new LoggerConfiguration()
                .WriteTo.MySQL(ConnectionString, table, batchSize: 1)
                .CreateLogger();

            logger.Information("Quoting test for {Label}", label);
            ((IDisposable)logger).Dispose();

            using var conn = OpenConnection();
            var pass = Check(conn,
                $"SELECT COUNT(*) FROM `{table.Replace("`", "``")}`;",
                v => Convert.ToInt32(v) == 1 ? null : $"expected 1 row, got {v}");

            if (!pass) ok = false;
        }
        return ok;
    }

    // -------------------------------------------------------------------------
    // Scenario 3: connection disposal
    // Validates bug fix: constructor opened MySqlConnection for CreateTable but
    // never disposed it, leaking one connection per sink instance and eventually
    // exhausting the connection pool.
    // -------------------------------------------------------------------------
    private static bool ConnectionDisposal()
    {
        const int instances = 60;

        for (var i = 0; i < instances; i++)
            DropTable($"smoke_conn_{i}");

        for (var i = 0; i < instances; i++)
        {
            // batchSize:1 so the single event is flushed immediately by EventPump.
            using var logger = new LoggerConfiguration()
                .WriteTo.MySQL(ConnectionString, $"smoke_conn_{i}", batchSize: 1)
                .CreateLogger();
            logger.Information("Connection disposal test {Instance}", i);
        }

        // Verify last instance actually wrote its row.
        using var conn = OpenConnection();
        return Check(conn, $"SELECT COUNT(*) FROM `smoke_conn_{instances - 1}`;",
            v => Convert.ToInt32(v) == 1 ? null : $"expected 1 row in last table, got {v}");
    }

    // -------------------------------------------------------------------------
    // Scenario 4: stress — 50 create/write/dispose cycles
    // Validates all three fixes hold under repeated cycling.
    // -------------------------------------------------------------------------
    private static bool Stress50()
    {
        const int cycles    = 50;
        const int perCycle  = 20;
        const int batchSize = 10;  // perCycle is an exact multiple — no partial-batch flush needed

        for (var c = 0; c < cycles; c++)
            DropTable($"smoke_stress_{c}");

        var passed = new List<int>();
        var failed = new List<int>();

        for (var c = 0; c < cycles; c++)
        {
            var table = $"smoke_stress_{c}";

            var logger = new LoggerConfiguration()
                .WriteTo.MySQL(ConnectionString, table, batchSize: batchSize)
                .CreateLogger();

            for (var i = 0; i < perCycle; i++)
                logger.Information("Stress cycle {Cycle} event {Index}", c, i);

            ((IDisposable)logger).Dispose();

            using var conn = OpenConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"SELECT Message FROM `{table}`;";
            using var reader = cmd.ExecuteReader();

            var rowCount = 0;
            var stringWriterFound = false;
            while (reader.Read())
            {
                rowCount++;
                var msg = reader.GetString(0);
                if (msg.Contains("StringWriter")) stringWriterFound = true;
            }

            if (rowCount == perCycle && !stringWriterFound)
                passed.Add(c);
            else
                failed.Add(c);
        }

        Console.WriteLine($"OK   stress cycles: {passed.Count}/{cycles} passed");
        if (failed.Count > 0)
            Console.Error.WriteLine($"FAIL cycles: {string.Join(", ", failed)}");

        return failed.Count == 0;
    }

    // -------------------------------------------------------------------------
    // Scenario 5: timer flush
    // Emits fewer events than batchSize so EventPump never self-flushes.
    // Waits for TimerPump (fires every 10s) to flush the partial batch.
    // Validates the timer path — all existing scenarios use batchSize=1 or exact
    // multiples and never exercise this code path.
    // -------------------------------------------------------------------------
    private static bool TimerFlush()
    {
        const string table     = "smoke_timer";
        const int    batchSize = 50;
        const int    count     = batchSize - 1;  // 49 events — EventPump never self-flushes
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: batchSize)
            .CreateLogger();

        for (var i = 0; i < count; i++)
            logger.Information("Timer flush event {Index}", i);

        // TimerPump threshold is 10s; wait 12s to give it a comfortable margin.
        Console.WriteLine("    waiting 12s for TimerPump...");
        Thread.Sleep(TimeSpan.FromSeconds(12));

        ((IDisposable)logger).Dispose();

        using var conn = OpenConnection();
        return Check(conn, $"SELECT COUNT(*) FROM `{table}`;", v =>
        {
            var n = Convert.ToInt32(v);
            return n == count ? null : $"expected {count} rows, got {n}";
        });
    }

    // -------------------------------------------------------------------------
    // Scenario 6: partial batch on immediate dispose
    // Emits a non-multiple of batchSize then disposes with no delay.
    // Exercises the FlushAndCloseEventHandlers partial-batch path directly.
    // This is the scenario that failed before the single-writer dispose race fix.
    // -------------------------------------------------------------------------
    private static bool PartialBatchDispose()
    {
        const string table     = "smoke_partial";
        const int    batchSize = 10;
        const int    count     = 35;  // 3 full batches + 5 remainder
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: batchSize)
            .CreateLogger();

        for (var i = 0; i < count; i++)
            logger.Information("Partial batch event {Index}", i);

        ((IDisposable)logger).Dispose();  // immediate — no sleep

        using var conn = OpenConnection();
        return Check(conn, $"SELECT COUNT(*) FROM `{table}`;", v =>
        {
            var n = Convert.ToInt32(v);
            return n == count ? null : $"expected {count} rows, got {n}";
        });
    }

    // -------------------------------------------------------------------------
    // Scenario 7: concurrent emitters
    // Multiple threads emit into the same logger simultaneously.
    // Validates PushEvent/_numMessages thread safety under contention.
    // -------------------------------------------------------------------------
    private static bool ConcurrentEmitters()
    {
        const string table     = "smoke_concurrent";
        const int    threads   = 8;
        const int    perThread = 500;
        const int    total     = threads * perThread;
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: 100)
            .CreateLogger();

        var tasks = new Task[threads];
        for (var t = 0; t < threads; t++)
        {
            var threadId = t;
            tasks[t] = Task.Run(() =>
            {
                for (var i = 0; i < perThread; i++)
                    logger.Information("Concurrent event thread={Thread} index={Index}", threadId, i);
            });
        }
        Task.WaitAll(tasks);

        ((IDisposable)logger).Dispose();

        using var conn = OpenConnection();
        return Check(conn, $"SELECT COUNT(*) FROM `{table}`;", v =>
        {
            var n = Convert.ToInt32(v);
            return n == total ? null : $"expected {total} rows, got {n}";
        });
    }

    // -------------------------------------------------------------------------
    // Scenario 8: high burst
    // Emits 10k events as fast as possible with a moderate batchSize.
    // Validates no deadlocks, no hangs on dispose, and all events land
    // (10k is well within the default 25k buffer so no drops expected).
    // -------------------------------------------------------------------------
    private static bool HighBurst()
    {
        const string table     = "smoke_burst";
        const int    count     = 10_000;
        const int    batchSize = 500;
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: batchSize)
            .CreateLogger();

        for (var i = 0; i < count; i++)
            logger.Information("Burst event {Index} with {@Payload}", i, new { X = i, Y = i * 2 });

        ((IDisposable)logger).Dispose();

        using var conn = OpenConnection();
        return Check(conn, $"SELECT COUNT(*) FROM `{table}`;", v =>
        {
            var n = Convert.ToInt32(v);
            return n == count ? null : $"expected {count} rows, got {n}";
        });
    }

    // -------------------------------------------------------------------------
    // Scenario 9: exception column
    // Validates the DBNull.Value fix for null/non-null Exception column values.
    // Before the fix, null exceptions stored "" instead of SQL NULL so
    // WHERE Exception IS NULL would return no rows.
    // -------------------------------------------------------------------------
    private static bool ExceptionColumn()
    {
        const string table = "smoke_exception";
        DropTable(table);

        var logger = new LoggerConfiguration()
            .WriteTo.MySQL(ConnectionString, table, batchSize: 1)
            .CreateLogger();

        logger.Information("Event without exception");
        logger.Error(new InvalidOperationException("test error"), "Event with exception");

        ((IDisposable)logger).Dispose();

        using var conn = OpenConnection();
        return
            Check(conn, $"SELECT COUNT(*) FROM `{table}` WHERE Exception IS NULL;", v =>
                Convert.ToInt32(v) == 1 ? null : $"expected 1 row with NULL exception, got {v}") &&
            Check(conn, $"SELECT COUNT(*) FROM `{table}` WHERE Exception IS NOT NULL;", v =>
                Convert.ToInt32(v) == 1 ? null : $"expected 1 row with non-NULL exception, got {v}") &&
            Check(conn, $"SELECT Exception FROM `{table}` WHERE Exception IS NOT NULL;", v =>
            {
                var s = v as string;
                if (string.IsNullOrEmpty(s))                    return "Exception column is empty";
                if (!s.Contains("InvalidOperationException"))   return $"unexpected exception text: {s}";
                return null;
            });
    }

    // -------------------------------------------------------------------------
    // Scenario 10: retry backoff timing
    // Triggers write failures by pointing at a non-listening port and verifies
    // that PumpAsync logs retry messages with increasing delays. We wait 8s —
    // enough to observe attempts 1 (delay 5s) and 2 (delay 10s) without
    // running the full 135s retry cycle.
    // -------------------------------------------------------------------------
    private static bool RetryBackoff()
    {
        var log = new System.Collections.Concurrent.ConcurrentQueue<(string Msg, DateTime At)>();
        SelfLog.Enable(msg => log.Enqueue((msg, DateTime.UtcNow)));

        try
        {
            var start = DateTime.UtcNow;

            var logger = new LoggerConfiguration()
                .WriteTo.MySQL(BadConnectionString, "smoke_retry_backoff", batchSize: 1)
                .CreateLogger();

            logger.Information("Retry backoff test");

            Console.WriteLine("    waiting 8s to observe first two retry attempts...");
            Thread.Sleep(TimeSpan.FromSeconds(8));

            ((IDisposable)logger).Dispose();

            var entries = log.ToArray();
            var a1 = Array.Find(entries, e => e.Msg.Contains("attempt 1/5"));
            var a2 = Array.Find(entries, e => e.Msg.Contains("attempt 2/5"));

            if (a1.Msg == null) {
                Console.Error.WriteLine("FAIL SelfLog missing 'attempt 1/5' message");
                return false;
            }
            if (a2.Msg == null) {
                Console.Error.WriteLine("FAIL SelfLog missing 'attempt 2/5' message (delay may not have elapsed yet)");
                return false;
            }

            var delta = (a2.At - a1.At).TotalSeconds;

            // Delay before attempt 2 is 5s; allow ±2s for CI scheduling jitter.
            if (delta < 3.0 || delta > 9.0) {
                Console.Error.WriteLine($"FAIL expected ~5s between attempts 1 and 2, got {delta:F1}s");
                return false;
            }

            Console.WriteLine($"OK   attempt 1/5 at +{(a1.At - start).TotalSeconds:F1}s");
            Console.WriteLine($"OK   attempt 2/5 at +{(a2.At - start).TotalSeconds:F1}s (delta {delta:F1}s ≈ 5s expected)");
            return true;
        }
        finally
        {
            SelfLog.Disable();
        }
    }

    // -------------------------------------------------------------------------
    // Scenario 11: batch drop after max retries
    // Runs the full backoff cycle (5 + 10 + 20 + 40 + 60 = 135s) and verifies
    // that PumpAsync drops the batch with a clear SelfLog message and logs all
    // five attempt messages. This test takes ~160s by design.
    // -------------------------------------------------------------------------
    private static bool BatchDrop()
    {
        // 5 + 10 + 20 + 40 + 60 = 135s of retry delays, plus the 6 write attempts
        // themselves: each connect to the dead port costs ~Connect Timeout (2s),
        // so ~12s on top of the delays. The drop fires at ~147s; wait 160s to clear
        // it with margin for task/CI scheduling jitter.
        const int waitSeconds = 160;

        var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
        SelfLog.Enable(msg => log.Enqueue(msg));

        try
        {
            var logger = new LoggerConfiguration()
                .WriteTo.MySQL(BadConnectionString, "smoke_batch_drop", batchSize: 1)
                .CreateLogger();

            logger.Information("Batch drop test event");

            Console.WriteLine($"    waiting {waitSeconds}s for all 5 retry attempts to exhaust...");
            Thread.Sleep(TimeSpan.FromSeconds(waitSeconds));

            ((IDisposable)logger).Dispose();

            var entries = log.ToArray();

            // All five attempt messages must be present.
            for (var i = 1; i <= 5; i++)
            {
                if (Array.Find(entries, m => m.Contains($"attempt {i}/5")) == null) {
                    Console.Error.WriteLine($"FAIL SelfLog missing 'attempt {i}/5' message");
                    return false;
                }
                Console.WriteLine($"OK   attempt {i}/5 logged");
            }

            // Drop message must appear after the fifth retry.
            var dropMsg = Array.Find(entries, m => m.Contains("Dropping batch"));
            if (dropMsg == null) {
                Console.Error.WriteLine("FAIL SelfLog missing 'Dropping batch' message");
                return false;
            }

            Console.WriteLine($"OK   {dropMsg}");
            return true;
        }
        finally
        {
            SelfLog.Disable();
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    private static string Prompt(string label, string defaultValue)
    {
        Console.Write($"  {label} [{defaultValue}]: ");
        var input = Console.ReadLine()?.Trim();
        return string.IsNullOrEmpty(input) ? defaultValue : input;
    }

    private static string PromptPassword(string label, string defaultValue)
    {
        Console.Write($"  {label} [{new string('*', defaultValue.Length)}]: ");

        if (Console.IsInputRedirected)
        {
            var line = Console.ReadLine()?.Trim();
            return string.IsNullOrEmpty(line) ? defaultValue : line;
        }

        var input = new System.Text.StringBuilder();
        while (true)
        {
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter)
            {
                Console.WriteLine();
                break;
            }
            if (key.Key == ConsoleKey.Backspace)
            {
                if (input.Length > 0)
                {
                    input.Remove(input.Length - 1, 1);
                    Console.Write("\b \b");
                }
            }
            else if (!char.IsControl(key.KeyChar))
            {
                input.Append(key.KeyChar);
                Console.Write('*');
            }
        }
        var entered = input.ToString();
        return string.IsNullOrEmpty(entered) ? defaultValue : entered;
    }

    private static bool EnsureDatabase(string server, string port, string database, string user, string password)
    {
        try
        {
            var adminCs = $"Server={server};Port={port};Uid={user};Pwd={password};" +
                          "AllowPublicKeyRetrieval=True;SslMode=None;";
            using var conn = new MySqlConnection(adminCs);
            conn.Open();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"CREATE DATABASE IF NOT EXISTS `{database}`";
            cmd.ExecuteNonQuery();
            Console.WriteLine($"\nOK   database '{database}' ready");
            return true;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"FAIL cannot connect to MySQL: {ex.Message}");
            return false;
        }
    }

    private static MySqlConnection OpenConnection()
    {
        var conn = new MySqlConnection(ConnectionString);
        conn.Open();
        return conn;
    }

    private static void DropTable(string table)
    {
        try
        {
            using var conn = OpenConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = $"DROP TABLE IF EXISTS `{table.Replace("`", "``")}`";
            cmd.ExecuteNonQuery();
        }
        catch { /* best-effort */ }
    }

    private static bool Check(MySqlConnection conn, string sql, Func<object, string> validator)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var result = cmd.ExecuteScalar();
        var msg = validator(result);
        if (msg != null)
        {
            Console.Error.WriteLine($"FAIL ({sql}): {msg}");
            return false;
        }
        Console.WriteLine($"OK   ({sql}) -> {result}");
        return true;
    }
}
