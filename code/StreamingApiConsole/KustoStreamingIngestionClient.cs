using Azure.Core;
using Kusto.Data.Common;
using Kusto.Ingest.V2;

namespace StreamingApiConsole
{
    internal class KustoStreamingIngestionClient : IDisposable
    {
        private readonly string _dbName;
        private readonly string _tableName;
        private readonly DataSourceFormat _dataFormat;
        private readonly string _dataMapping;
        private readonly bool _isOutputCompressed;
        private readonly IIngest _ingestClient;

        public KustoStreamingIngestionClient(
            Uri tableUri,
            DataSourceFormat dataFormat,
            string dataMapping,
            bool isOutputCompressed,
            TokenCredential credentials)
        {
            var parts = tableUri.PathAndQuery.Split('/');
            var dbName = parts[1];
            var tableName = parts[2];
            var clusterUri = new Uri($"{tableUri.Scheme}://{tableUri.Host}");

            Console.WriteLine($"KustoStreamingIngestionClient ; Cluster:  {clusterUri}");
            Console.WriteLine($"KustoStreamingIngestionClient ; DB:  {dbName}");
            Console.WriteLine($"KustoStreamingIngestionClient ; Table:  {tableName}");

            var ingestClient = StreamingIngestClientBuilder.Create(clusterUri)
                .WithAuthentication(credentials)
                .Build();

            _dbName = dbName;
            _tableName = tableName;
            _dataFormat = dataFormat;
            _dataMapping = dataMapping;
            _isOutputCompressed = isOutputCompressed;
            _ingestClient = ingestClient;
        }

        void IDisposable.Dispose()
        {
            _ingestClient.Dispose();
        }

        public async Task WarmupAsync(CancellationToken ct)
        {   //  Do an empty ingestion to warm up the streaming ingestion client
            var stream = new MemoryStream();

            using(var writer = new StreamWriter(stream, leaveOpen: true))
            {
                await writer.WriteAsync("{}");
            }
            stream.Position = 0;
            var source = new StreamSource(
                stream,
                DataSourceCompressionType.None,
                DataSourceFormat.json);

            await _ingestClient.IngestAsync(source, _dbName, _tableName);
        }

        public async Task SendAsync(Stream stream)
        {
            var source = new StreamSource(
                stream,
                _isOutputCompressed
                ? DataSourceCompressionType.GZip
                : DataSourceCompressionType.None,
                _dataFormat,
                leaveOpen: true);
            var ingestProperties = new IngestProperties
            {
                IngestionMappingReference = _dataMapping
            };

            await _ingestClient.IngestAsync(source, _dbName, _tableName, ingestProperties);
        }
    }
}