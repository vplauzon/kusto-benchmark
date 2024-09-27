using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestorConsole
{
    internal class KustoClient
    {
        private static readonly ClientRequestProperties DEFAULT_PROPERTIES =
            new ClientRequestProperties();

        private readonly ICslQueryProvider _queryProvider;
        private readonly IKustoQueuedIngestClient _ingestProvider;
        private readonly string _dbName;
        private readonly string _ingestionTable;
        private readonly DataSourceFormat _ingestionFormat;
        private readonly Kusto.Data.Ingestion.IngestionMappingKind _ingestionMappingKind;
        private readonly string _ingestionMapping;

        #region Constructors
        public KustoClient(
            string dbUri,
            string ingestionTable,
            string ingestionFormat,
            string ingestionMapping,
            TokenCredential credential)
        {
            var uri = new Uri(dbUri);
            var dbName = uri.Segments[1];
            var clusterUri = new Uri($"{uri.Scheme}://{uri.Host}");
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credential);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);
            var ingestProvider = KustoIngestFactory.CreateQueuedIngestClient(builder);

            _queryProvider = queryProvider;
            _ingestProvider = ingestProvider;
            _dbName = dbName;
            _ingestionTable = ingestionTable;
            _ingestionFormat = ParseFormat(ingestionFormat);
            _ingestionMappingKind = MapFormatToKind(_ingestionFormat);
            _ingestionMapping = ingestionMapping;
        }

        private static DataSourceFormat ParseFormat(string ingestionFormat)
        {
            if (Enum.TryParse<DataSourceFormat>(ingestionFormat, true, out var parsedFormat))
            {
                return parsedFormat;
            }
            else
            {
                throw new FormatException($"Can't parse ingestion format '{ingestionFormat}'");
            }
        }

        private static IngestionMappingKind MapFormatToKind(DataSourceFormat ingestionFormat)
        {
            switch (ingestionFormat)
            {
                case DataSourceFormat.json:
                case DataSourceFormat.multijson:
                    return IngestionMappingKind.Json;

                default:
                    throw new NotImplementedException($"Format '{ingestionFormat}'");
            }
        }
        #endregion

        public async Task<string> FetchTemplateAsync(
            string templateTableName,
            CancellationToken ct)
        {
            var reader = await _queryProvider.ExecuteQueryAsync(
                _dbName,
                templateTableName,
                DEFAULT_PROPERTIES,
                ct);
            var template = (string)reader.ToDataSet().Tables[0].Rows[0][0];

            return template;
        }

        public async Task<IImmutableDictionary<string, IImmutableList<string>>> LoadReferenceValuesAsync(
            string tableName,
            IEnumerable<string> groupNames,
            CancellationToken ct)
        {
            var names = string.Join(", ", groupNames.Select(g => $"'{g}'"));
            var query = $@"
{tableName}
| where GroupName in ({names})
| project GroupName, Value";
            var reader = await _queryProvider.ExecuteQueryAsync(
                _dbName,
                query,
                DEFAULT_PROPERTIES,
                ct);
            var groups = reader.ToDataSet().Tables[0].Rows
                .Cast<DataRow>()
                .Select(r => new
                {
                    GroupName = (string)r[0],
                    Value = (string)r[1]
                })
                .GroupBy(o => o.GroupName);
            var map = groups.ToImmutableDictionary(
                g => g.Key,
                g => (IImmutableList<string>)g.Select(g => g.Value).ToImmutableList());

            return map;
        }

        public async Task IngestAsync(Stream stream, CancellationToken ct)
        {
            var properties = new KustoIngestionProperties(_dbName, _ingestionTable);

            if (!string.IsNullOrWhiteSpace(_ingestionMapping))
            {
                properties.IngestionMapping = new IngestionMapping
                {
                    IngestionMappingKind = _ingestionMappingKind,
                    IngestionMappingReference = _ingestionMapping
                };
            }
            properties.Format = _ingestionFormat;

            await _ingestProvider.IngestFromStreamAsync(
                stream,
                properties,
                new StreamSourceOptions
                {
                    CompressionType = DataSourceCompressionType.GZip,
                    LeaveOpen = true
                });
        }
    }
}