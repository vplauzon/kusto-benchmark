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
        private readonly string _dbName;
        private readonly string _tableName;

        #region Constructors
        public KustoCommandClient(Uri dbUri, string tableName, TokenCredential credential)
        {
            var dbName = dbUri.Segments[1];
            var clusterUri = new Uri($"{dbUri.Scheme}://{dbUri.Host}");
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credential);
            var commandProvider = KustoClientFactory.CreateCslAdminProvider(builder);

            _commandProvider = commandProvider;
            _dbName = dbName;
            _tableName = tableName;
        }
        #endregion

        public async Task<long> FetchBatchCountAsync(DateTime start, DateTime end, CancellationToken ct)
        {
            try
            {
                var startText = start.ToUtc().ToString();
                var endText = end.ToUtc().ToString();
                var command = $@"
.show data operations
| where Timestamp between (datetime({startText}) .. {endText})
| where Database == ""{_dbName}""
| where Table == ""{_tableName}""
| where OperationKind == ""BatchIngest""
| count";
                var reader = await _commandProvider.ExecuteControlCommandAsync(_dbName, command);
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
    }
}