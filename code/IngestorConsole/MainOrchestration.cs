
using Azure.Core;
using Azure.Identity;
using System.Diagnostics;
using System.IO.Compression;

namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly EventGenerator _generator;
        private readonly KustoClient _kustoClient;
        private readonly string _ingestTableName;
        private readonly int _blobSizeInBytes;

        #region Constructors
        private MainOrchestration(
            EventGenerator generator,
            KustoClient kustoClient,
            string ingestTableName,
            int blobSizeInBytes)
        {
            if (blobSizeInBytes < 1000000)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(blobSizeInBytes),
                    "Must be at least 1MB");
            }
            _generator = generator;
            _kustoClient = kustoClient;
            _ingestTableName = ingestTableName;
            _blobSizeInBytes = blobSizeInBytes;
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CreateCredentials(options.Authentication);
            var kustoClient = new KustoClient(options.DbUri, credentials);
            var template = await kustoClient.FetchTemplateAsync(options.TemplateTable, ct);
            var generator = await EventGenerator.CreateAsync(template, kustoClient, ct);

            Trace.WriteLine("Template:");
            Trace.WriteLine(template);
            Trace.WriteLine();

            return new MainOrchestration(
                generator,
                kustoClient,
                options.IngestionTable,
                options.BlobSize * 1000000);
        }

        private static TokenCredential CreateCredentials(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication)
                || string.Compare(authentication.Trim(), "azcli", true) == 0)
            {
                return new DefaultAzureCredential();
            }
            else
            {
                return new ManagedIdentityCredential();
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                using (var stream = GenerateBlob(ct))
                {
                    stream.Position = 0;

                    await _kustoClient.IngestAsync(_ingestTableName, stream, ct);
                    Trace.WriteLine($"Writing {stream.Length} bytes");
                }
            }
        }

        private MemoryStream GenerateBlob(CancellationToken ct)
        {
            var compressedStream = new MemoryStream();

            using (var compressingStream =
                new GZipStream(compressedStream, CompressionLevel.Fastest, true))
            using (var writer = new StreamWriter(compressingStream))
            {
                while (compressedStream.Length < _blobSizeInBytes && !ct.IsCancellationRequested)
                {
                    _generator.GenerateEvent(writer);
                }

                return compressedStream;
            }
        }
    }
}