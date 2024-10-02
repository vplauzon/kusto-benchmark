using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Ingestion;
using Kusto.Data.Net.Client;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BenchmarkLib
{
    public class KustoEngineClient
    {
        private static readonly ClientRequestProperties DEFAULT_PROPERTIES =
            new ClientRequestProperties();

        private readonly ICslQueryProvider _queryProvider;
        private readonly string _dbName;

        #region Constructors
        public KustoEngineClient(string dbUri, TokenCredential credential)
        {
            var uri = new Uri(dbUri);
            var dbName = uri.Segments[1];
            var clusterUri = new Uri($"{uri.Scheme}://{uri.Host}");
            var builder = new KustoConnectionStringBuilder(clusterUri.ToString())
                .WithAadAzureTokenCredentialsAuthentication(credential);
            var queryProvider = KustoClientFactory.CreateCslQueryProvider(builder);

            _queryProvider = queryProvider;
            _dbName = dbName;
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
    }
}