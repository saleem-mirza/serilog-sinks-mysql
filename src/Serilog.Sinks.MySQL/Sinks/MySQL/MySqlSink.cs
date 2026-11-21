// Copyright 2019-2026 Zethian Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using MySqlConnector;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.MySQL
{
    internal class MySqlSink : BatchProvider, ILogEventSink
    {
        // Indexed by (int)LogEventLevel — Verbose=0 … Fatal=5 (contiguous, stable)
        private static readonly string[] LevelNames = { "Verbose", "Debug", "Information", "Warning", "Error", "Fatal" };

        private readonly string _connectionString;
        private readonly bool _storeTimestampInUtc;
        private readonly string _tableName;
        private readonly string _insertPrefix;
        private readonly string _createTableSql;

        // Only BatchProvider's single RunAsync loop touches these, never concurrently.
        private readonly StringBuilder _sb = new StringBuilder();
        private bool _tableReady;

        public MySqlSink(
            string connectionString,
            string tableName = "Logs",
            bool storeTimestampInUtc = false,
            uint batchSize = 100) : base((int) batchSize)
        {
            _connectionString    = connectionString;
            _tableName           = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;

            var escaped = tableName.Replace("`", "``");
            _insertPrefix   = $"INSERT INTO `{escaped}` (Timestamp,Level,Template,Message,Exception,Properties) VALUES ";
            _createTableSql = $"CREATE TABLE IF NOT EXISTS `{escaped}` (" +
                              "id INT NOT NULL AUTO_INCREMENT PRIMARY KEY," +
                              "Timestamp VARCHAR(100)," +
                              "Level VARCHAR(15)," +
                              "Template TEXT," +
                              "Message TEXT," +
                              "Exception TEXT," +
                              "Properties TEXT," +
                              "_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        private async Task<MySqlConnection> GetSqlConnectionAsync()
        {
            MySqlConnection conn = null;
            try {
                conn = new MySqlConnection(_connectionString);
                await conn.OpenAsync().ConfigureAwait(false);

                return conn;
            }
            catch (Exception ex) {
                conn?.Dispose();
                SelfLog.WriteLine("Failed to open MySQL connection: {0}", ex);

                return null;
            }
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            try {
                var n = logEventsBatch.Count;

                // Phase 1: format all column values before opening the connection
                var timestamps = new string[n];
                var levels     = new string[n];
                var templates  = new string[n];
                var messages   = new string[n];
                var exceptions = new string[n];
                var properties = new string[n];

                var i = 0;
                foreach (var logEvent in logEventsBatch) {
                    timestamps[i] = _storeTimestampInUtc
                        ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                        : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz");
                    var lv        = (int)logEvent.Level;
                    levels[i]     = (uint)lv < (uint)LevelNames.Length ? LevelNames[lv] : logEvent.Level.ToString();
                    templates[i]  = logEvent.MessageTemplate.Text;
                    messages[i]   = logEvent.RenderMessage();
                    exceptions[i] = logEvent.Exception?.ToString();
                    properties[i] = SerializeProperties(logEvent);
                    i++;
                }

                // Phase 2: build single multi-row INSERT using pooled StringBuilder
                var sb = _sb;
                sb.Clear();
                sb.Append(_insertPrefix);
                for (var r = 0; r < n; r++) {
                    if (r > 0) sb.Append(',');
                    sb.Append($"(@t{r},@l{r},@tm{r},@m{r},@x{r},@p{r})");
                }

                // Phase 3: one connection, one command, one round-trip — no transaction needed
                using (var sqlCon = await GetSqlConnectionAsync().ConfigureAwait(false)) {
                    if (sqlCon == null) return false;

                    // Created lazily so a DB that was down at startup still gets its table.
                    if (!_tableReady) {
                        using (var create = sqlCon.CreateCommand()) {
                            create.CommandText = _createTableSql;
                            await create.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }
                        _tableReady = true;
                    }

                    using (var cmd = sqlCon.CreateCommand()) {
                        cmd.CommandText = sb.ToString();

                        for (var r = 0; r < n; r++) {
                            cmd.Parameters.Add(new MySqlParameter($"@t{r}",  MySqlDbType.VarChar) { Value = timestamps[r] });
                            cmd.Parameters.Add(new MySqlParameter($"@l{r}",  MySqlDbType.VarChar) { Value = levels[r] });
                            cmd.Parameters.Add(new MySqlParameter($"@tm{r}", MySqlDbType.Text)    { Value = templates[r] });
                            cmd.Parameters.Add(new MySqlParameter($"@m{r}",  MySqlDbType.Text)    { Value = messages[r] });
                            cmd.Parameters.Add(new MySqlParameter($"@x{r}",  MySqlDbType.Text)    { Value = (object)exceptions[r] ?? DBNull.Value });
                            cmd.Parameters.Add(new MySqlParameter($"@p{r}",  MySqlDbType.Text)    { Value = (object)properties[r] ?? DBNull.Value });
                        }

                        await cmd.ExecuteNonQueryAsync().ConfigureAwait(false);
                        return true;
                    }
                }
            }
            catch (MySqlException ex) when (ex.ErrorCode == MySqlErrorCode.NoSuchTable) {
                SelfLog.WriteLine("Log table missing, will recreate on retry: {0}", ex);
                _tableReady = false;

                return false;
            }
            catch (MySqlException ex) when (IsPoisonBatch(ex.ErrorCode)) {
                // Retrying can never succeed; return true ("handled") so BatchProvider moves on.
                SelfLog.WriteLine("Dropping batch of {0} events, error is not retryable: {1}", logEventsBatch.Count, ex);

                return true;
            }
            catch (Exception ex) {
                SelfLog.WriteLine("Failed to write batch: {0}", ex);

                return false;
            }
        }

        // Allowlist, not !IsTransient: anything unlisted (e.g. a network drop mid-query) keeps retrying.
        private static bool IsPoisonBatch(MySqlErrorCode code) =>
            code == MySqlErrorCode.DataTooLong ||
            code == MySqlErrorCode.ParseError ||
            code == MySqlErrorCode.BadFieldError ||
            code == MySqlErrorCode.TruncatedWrongValueForField;

        private static string SerializeProperties(LogEvent logEvent)
        {
            if (logEvent.Properties.Count == 0) return null;
            try {
                return logEvent.Properties.Json();
            }
            catch (Exception ex) {
                SelfLog.WriteLine("Properties serialization failed, storing NULL: {0}", ex);

                return null;
            }
        }
    }
}
