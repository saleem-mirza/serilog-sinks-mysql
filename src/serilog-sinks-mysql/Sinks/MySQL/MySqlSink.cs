using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using Serilog.Events;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Extensions;
using Serilog.Sinks.Batch;

namespace Serilog.Sinks.MySQL
{
    internal class MySqlSink : BatchProvider, ILogEventSink
    {
        private readonly string _connectionString;
        private readonly string _tableName;
        private readonly bool _storeTimestampInUtc;

        public MySqlSink(string connectionString,
            string tableName = "Logs",
            bool storeTimestampInUtc = false) : base(500, Environment.ProcessorCount)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;

            var sqlConnection = GetSqlConnection();
            CreateTable(sqlConnection);
        }

        private MySqlConnection GetSqlConnection()
        {
            try
            {
                var conn = new MySqlConnection(_connectionString);
                conn.Open();
                return conn;
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
                return null;
            }
        }

        private MySqlCommand GetInsertCommand(MySqlConnection sqlConnection)
        {
            var tableCommandBuilder = new StringBuilder();
            tableCommandBuilder.Append($"INSERT INTO  {_tableName} (");
            tableCommandBuilder.Append("Timestamp, Level, Message, Exception, Properties) ");
            tableCommandBuilder.Append("VALUES (@ts, @lvel, @msg, @ex, @prop)");

            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = tableCommandBuilder.ToString();

            cmd.Parameters.Add(new MySqlParameter("@ts", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@lvel", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@msg", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@ex", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@prop", MySqlDbType.VarChar));

            return cmd;
        }

        private void CreateTable(MySqlConnection sqlConnection)
        {
            try
            {
                var tableCommandBuilder = new StringBuilder();
                tableCommandBuilder.Append($"CREATE TABLE IF NOT EXISTS {_tableName} (");
                tableCommandBuilder.Append("id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,");
                tableCommandBuilder.Append("Timestamp VARCHAR(100),");
                tableCommandBuilder.Append("Level VARCHAR(15),");
                tableCommandBuilder.Append("Message TEXT,");
                tableCommandBuilder.Append("Exception TEXT,");
                tableCommandBuilder.Append("Properties TEXT,");
                tableCommandBuilder.Append("_ts TIMESTAMP)");

                var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = tableCommandBuilder.ToString();
                cmd.ExecuteNonQuery();

            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        protected override void WriteLogEvent(ICollection<LogEvent> logEventsBatch)
        {
            try
            {
                using (var sqlCon = GetSqlConnection())
                {
                    using (var tr = sqlCon.BeginTransaction())
                    {
                        var insertCommand = GetInsertCommand(sqlCon);
                        insertCommand.Transaction = tr;

                        foreach (var logEvent in logEventsBatch)
                        {
                            insertCommand.Parameters["@ts"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString("o")
                                : logEvent.Timestamp.ToString("o");

                            insertCommand.Parameters["@lvel"].Value = logEvent.Level.ToString();
                            insertCommand.Parameters["@msg"].Value = logEvent.MessageTemplate.ToString();
                            insertCommand.Parameters["@ex"].Value = logEvent.Exception?.ToString();
                            insertCommand.Parameters["@prop"].Value = logEvent.Properties.Json();

                            insertCommand.ExecuteNonQuery();
                        }
                        tr.Commit();
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }
    }
}
