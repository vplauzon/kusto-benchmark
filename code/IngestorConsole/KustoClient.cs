﻿using Azure.Core;
using Kusto.Cloud.Platform.Data;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;
using Kusto.Ingest;
using System;
using System.Collections.Generic;
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

        public KustoClient(string dbUri, TokenCredential credential)
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
        }

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

        public async Task IngestAsync(string tableName, Stream stream, CancellationToken ct)
        {
            Trace.WriteLine("Before ingest");
            var properties = new KustoIngestionProperties(_dbName, tableName);

            properties.Format = DataSourceFormat.multijson;

            await _ingestProvider.IngestFromStreamAsync(
                stream,
                properties,
                new StreamSourceOptions
                {
                    CompressionType = DataSourceCompressionType.GZip
                });
        }
    }
}