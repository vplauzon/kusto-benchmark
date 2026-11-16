using Azure.Core;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest.V2;
using System.Net;

namespace StreamingApiConsole
{
    internal class KustoStreamingIngestionClient : IDisposable
    {
        private readonly string _dbName;
        private readonly string _tableName;
        private readonly bool _isOutputCompressed;
        private readonly IIngest _ingestClient;

        public KustoStreamingIngestionClient(
            Uri tableUri,
            bool isOutputCompressed,
            TokenCredential credentials)
        {
            var dbName = tableUri.Segments[1];
            var tableName = tableUri.Segments[2];
            var clusterUri = new Uri($"{tableUri.Scheme}://{tableUri.Host}");
            var ingestClient = StreamingIngestClientBuilder.Create(clusterUri)
                .WithAuthentication(credentials)
                .Build();

            _dbName = dbName;
            _tableName = tableName;
            _isOutputCompressed = isOutputCompressed;
            _ingestClient = ingestClient;
        }

        void IDisposable.Dispose()
        {
            _ingestClient.Dispose();
        }

        public async Task SendAsync(MemoryStream outputStream)
        {
            var source = new StreamSource(
                outputStream,
                _isOutputCompressed ? DataSourceCompressionType.GZip : DataSourceCompressionType.None,
                DataSourceFormat.json);
            
            await _ingestClient.IngestAsync(source, _dbName, _tableName);
        }
    }
}