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
    internal class KustoCommandClient
    {
        private readonly ICslAdminProvider _commandProvider;
        private readonly string _realDbName;
        private readonly string _tableName;

        #region Constructors
        private KustoCommandClient(
            ICslAdminProvider commandProvider,
            string realDbName,
            string tableName)
        {
            _commandProvider = commandProvider;
            _realDbName = realDbName;
            _tableName = tableName;
        }

        public static async Task<KustoCommandClient> CreateAsync(
            Uri dbUri,
            string tableName,
            TokenCredential credential)
        {
            var dbName = dbUri.Segments[1];
            var clusterUri = new Uri($"{dbUri.Scheme}://{dbUri.Host}");
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credential);
            var commandProvider = KustoClientFactory.CreateCslAdminProvider(builder);
            //  In Fabric, databases have GUID as name
            var reader = await commandProvider.ExecuteControlCommandAsync(
                dbName,
                ".show database | project DatabaseName");
            var realDbName = (string)reader.ToDataSet().Tables[0].Rows[0][0];

            return new KustoCommandClient(commandProvider, realDbName, tableName);
        }
        #endregion

        public async Task<long> FetchStreamingFailureCountAsync(
            DateTime start,
            DateTime end,
            CancellationToken ct)
        {
            try
            {
                var startText = start.ToUtc().ToString();
                var endText = end.ToUtc().ToString();
                var command = $@"
.show streamingingestion failures
| where LastFailureOn between (datetime({startText}) .. datetime({endText}))
| where Database == ""{_realDbName}""
| where Table == ""{_tableName}""
| summarize sum(Count)";
                var reader = await _commandProvider.ExecuteControlCommandAsync(_realDbName, command);
                var count = (long)reader.ToDataSet().Tables[0].Rows[0][0];

                return count;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error fetching batch count: {ex.Message}");
                Console.WriteLine(ex.StackTrace);

                throw;
            }
        }

        public async Task ClearTableAsync(CancellationToken ct)
        {
            try
            {
                var command = $@".clear table {_tableName} data";
                var reader = await _commandProvider.ExecuteControlCommandAsync(_realDbName, command);
                var success = (string)reader.ToDataSet().Tables[0].Rows[0][0];
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error clearing table: {ex.Message}");
                Console.WriteLine(ex.StackTrace);

                throw;
            }
        }
    }
}