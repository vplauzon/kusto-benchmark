using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Cloud.Platform.Utils;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole
{
    internal class KustoClient
    {
        private readonly ICslAdminProvider _commandProvider;
        private readonly ICslQueryProvider _queryProvider;
        private readonly string _realDbName;
        private readonly string _tableName;
        private readonly string _timestampColumn;

        #region Constructors
        private KustoClient(
            ICslAdminProvider commandProvider,
            ICslQueryProvider queryProvider,
            string realDbName,
            string tableName,
            string timestampColumn)
        {
            _commandProvider = commandProvider;
            _queryProvider = queryProvider;
            _realDbName = realDbName;
            _tableName = tableName;
            _timestampColumn = timestampColumn;
        }

        public static async Task<KustoClient> CreateAsync(
            Uri dbUri,
            string tableName,
            string timestampColumn,
            TokenCredential credential)
        {
            var dbName = dbUri.Segments[1];
            var clusterUri = new Uri($"{dbUri.Scheme}://{dbUri.Host}");
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credential);
            var commandProvider = KustoClientFactory.CreateCslAdminProvider(builder);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);
            //  In Fabric, databases have GUID as name
            var reader = await commandProvider.ExecuteControlCommandAsync(
                dbName,
                ".show database | project DatabaseName");
            var realDbName = (string)reader.ToDataSet().Tables[0].Rows[0][0];

            return new KustoClient(commandProvider, queryProvider, realDbName, tableName, timestampColumn);
        }
        #endregion

        public async Task<long> FetchStreamingFailureCountAsync(
            DateTime start,
            DateTime end,
            CancellationToken ct)
        {
            var startText = start.ToUtc().ToString();
            var endText = end.ToUtc().ToString();
            var command = $@"
.show streamingingestion failures
| where LastFailureOn between (datetime({startText}) .. datetime({endText}))
| where Database == ""{_realDbName}""
| where Table == ""{_tableName}""
| summarize sum(Count)";

            try
            {
                var reader = await _commandProvider.ExecuteControlCommandAsync(_realDbName, command);
                var count = (long)reader.ToDataSet().Tables[0].Rows[0][0];

                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching streaming failure count: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"Command:  {command}");

                throw;
            }
        }

        public async Task<long> FetchLatencyFailureCountAsync(
            DateTime start,
            DateTime end,
            CancellationToken ct)
        {
            var startText = start.ToUtc().ToString();
            var endText = end.ToUtc().ToString();
            var command = $@"
{_tableName}
| project Delta = ingestion_time()-{_timestampColumn}
| where Delta > 3s
| count";

            try
            {
                var reader = await _queryProvider.ExecuteQueryAsync(_realDbName, command, new(), ct);
                var count = (long)reader.ToDataSet().Tables[0].Rows[0][0];

                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching latency failure count: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"Command:  {command}");

                throw;
            }
        }

        public async Task ClearTableAsync(CancellationToken ct)
        {
            var command = $@".clear table {_tableName} data";

            try
            {
                var reader = await _commandProvider.ExecuteControlCommandAsync(_realDbName, command);
                var success = (string)reader.ToDataSet().Tables[0].Rows[0][0];
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error clearing table: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                Console.WriteLine($"Command:  {command}");

                throw;
            }
        }
    }
}