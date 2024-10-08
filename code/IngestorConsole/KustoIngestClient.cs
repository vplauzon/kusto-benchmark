﻿using Azure.Core;
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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IngestorConsole
{
    internal class KustoIngestClient
    {
        private static readonly ClientRequestProperties DEFAULT_PROPERTIES =
            new ClientRequestProperties();

        private readonly IKustoQueuedIngestClient _ingestProvider;
        private readonly string _dbName;
        private readonly string _ingestionTable;
        private readonly DataSourceFormat _ingestionFormat;
        private readonly Kusto.Data.Ingestion.IngestionMappingKind _ingestionMappingKind;
        private readonly string _ingestionMapping;

        #region Constructors
        public KustoIngestClient(
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
            var ingestProvider = KustoIngestFactory.CreateQueuedIngestClient(builder);

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