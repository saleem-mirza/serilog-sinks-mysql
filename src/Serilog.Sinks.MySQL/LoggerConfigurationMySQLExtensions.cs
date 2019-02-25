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
using Serilog.Configuration;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.MySQL;

namespace Serilog
{
    /// <summary>
    ///     Adds the WriteTo.MySQL() extension method to <see cref="LoggerConfiguration" />.
    /// </summary>
    public static class LoggerConfigurationMySqlExtensions
    {
        /// <summary>
        ///     Adds a sink that writes log events to a MySQL database.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="connectionString">The connection string to MySQL database.</param>
        /// <param name="tableName">The name of the MySQL table to store log.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="storeTimestampInUtc">Store timestamp in UTC format</param>
        /// <param name="batchSize">Number of log messages to be sent as batch. Supported range is between 1 and 1000</param>
        /// <param name="levelSwitch">
        /// A switch allowing the pass-through minimum level to be changed at runtime.
        /// </param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        public static LoggerConfiguration MySQL(
            this LoggerSinkConfiguration loggerConfiguration,
            string connectionString,
            string tableName = "Logs",
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            bool storeTimestampInUtc = false,
            uint batchSize = 100,
            LoggingLevelSwitch levelSwitch = null)
        {
            if (loggerConfiguration == null)
                throw new ArgumentNullException(nameof(loggerConfiguration));

            if (string.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException(nameof(connectionString));

            if (batchSize < 1 || batchSize > 1000)
                throw new ArgumentOutOfRangeException("[batchSize] argument must be between 1 and 1000 inclusive");

            try {
                return loggerConfiguration.Sink(
                    new MySqlSink(connectionString, tableName, storeTimestampInUtc, batchSize),
                    restrictedToMinimumLevel,
                    levelSwitch);
            }
            catch (Exception ex) {
                SelfLog.WriteLine(ex.Message);

                throw;
            }
        }
    }
}
