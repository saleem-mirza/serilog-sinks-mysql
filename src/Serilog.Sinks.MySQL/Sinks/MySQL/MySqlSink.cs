// Copyright 2019 Zethian Inc.
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
using System.ComponentModel.Design.Serialization;
using System.IO;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.MySQL
{
    internal class MySqlSink : BatchProvider, ILogEventSink
    {
        private readonly string _connectionString;
        private readonly bool _storeTimestampInUtc;
        private readonly string _tableName;
         
        private Func<DateTime, string> _FuncGetTable = null;
        private string _InsertSqlTemplate = @"INSERT INTO {0} (Timestamp, Level, Template, Message, Exception, Properties) VALUES (@ts, @level,@template, @msg, @ex, @prop)";

        private string _CreateTableTemplate = @"CREATE TABLE `{0}` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`Timestamp` VARCHAR(100) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`Level` VARCHAR(15) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`Template` TEXT(65535) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`Message` TEXT(65535) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`Exception` TEXT(65535) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`Properties` TEXT(65535) NULL DEFAULT NULL COLLATE 'utf8mb4_general_ci',
	`_ts` TIMESTAMP NOT NULL DEFAULT current_timestamp(),
	PRIMARY KEY (`id`) USING BTREE
)
COLLATE='utf8mb4_general_ci'
ENGINE=MyISAM
AUTO_INCREMENT=1
;";

        private Dictionary<string, string> _LogDateInsertItems = new Dictionary<string, string>();

        public MySqlSink(
            string connectionString,
            string tableName = "Logs",
            string insertSql = "",
            string createTable = "",
            bool storeTimestampInUtc = false,
            uint batchSize = 100) : base((int)batchSize)
        {
            _connectionString = connectionString;
            _tableName = tableName;
            _storeTimestampInUtc = storeTimestampInUtc;

            if (string.IsNullOrEmpty(_tableName) == false && _tableName.IndexOf("{") >= 0)
            {
                _FuncGetTable = (date) => string.Format(_tableName, date);
            }

            if (!string.IsNullOrEmpty(insertSql))
            {
                _InsertSqlTemplate = insertSql;
            }
            if (!string.IsNullOrEmpty(createTable))
            {
                _CreateTableTemplate = createTable;
            }

            var sqlConnection = GetSqlConnection();
            if (_FuncGetTable == null)
            {
                CreateTable(sqlConnection, _tableName);
            }
        }

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
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

        private MySqlCommand GetInsertCommand(MySqlConnection sqlConnection, string tableName)
        {
            var sqlText = "";
            if (!_LogDateInsertItems.TryGetValue(tableName, out sqlText))
            {
                lock (_LogDateInsertItems)
                {
                    if (!_LogDateInsertItems.TryGetValue(tableName, out sqlText))
                    {
                        sqlText = string.Format(_InsertSqlTemplate, tableName);
                        _LogDateInsertItems[tableName] = sqlText;
                        CreateTable(sqlConnection, tableName);
                    }
                }
            }

            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = sqlText;

            cmd.Parameters.Add(new MySqlParameter("@ts", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@level", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@template", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@msg", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@ex", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@prop", MySqlDbType.VarChar));

            return cmd;
        }

        private string GetTableName(DateTime date)
        {
            return _FuncGetTable != null ? _FuncGetTable(date) : _tableName; ;
        }

        private void CreateTable(MySqlConnection sqlConnection, string tableName)
        {
            try
            {
                var cmd = sqlConnection.CreateCommand();
                cmd.CommandText = string.Format(_CreateTableTemplate, tableName);
                cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
            }
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            try
            {
                using (var sqlCon = GetSqlConnection())
                {
                    using (var tr = await sqlCon.BeginTransactionAsync().ConfigureAwait(false))
                    {
                        MySqlCommand insertCommand = null;
                        var date = DateTime.Now;

                        foreach (var logEvent in logEventsBatch)
                        {
                            if (insertCommand == null || date != logEvent.Timestamp.Date)
                            {
                                var tableName = GetTableName(logEvent.Timestamp.Date);
                                insertCommand = GetInsertCommand(sqlCon, tableName);
                                insertCommand.Transaction = tr;
                            }

                            var logMessageString = new StringWriter(new StringBuilder());
                            logEvent.RenderMessage(logMessageString);

                            insertCommand.Parameters["@ts"].Value = _storeTimestampInUtc
                                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz");

                            insertCommand.Parameters["@level"].Value = logEvent.Level.ToString();
                            insertCommand.Parameters["@template"].Value = logEvent.MessageTemplate.ToString();
                            insertCommand.Parameters["@msg"].Value = logMessageString;

                            //singba:reset dbnull with string.Empty 
                            insertCommand.Parameters["@ex"].Value = logEvent.Exception == null ? string.Empty : logEvent.Exception.ToString();
                            //singba:change Properties allways have json format
                            insertCommand.Parameters["@prop"].Value = logEvent.Properties.Count > 0
                                ? logEvent.Properties.Json()
                                : "{}";

                            await insertCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                        }

                        tr.Commit();

                        return true;
                    }
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine(ex.Message);

                return false;
            }
        }
    }
}
