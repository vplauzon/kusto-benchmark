
using Azure.Core;
using Azure.Identity;
using Kusto.Data.Common;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;

namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly EventGenerator _generator;
        private readonly KustoClient _kustoClient;
        private readonly int _blobSizeInBytes;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _uploadTaskQueue = new();

        #region Constructors
        private MainOrchestration(
            EventGenerator generator,
            KustoClient kustoClient,
            int blobSizeInBytes,
            int parallelStreams)
        {
            if (blobSizeInBytes < 1000000)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(blobSizeInBytes),
                    "Must be at least 1MB");
            }
            _generator = generator;
            _kustoClient = kustoClient;
            _blobSizeInBytes = blobSizeInBytes;
            _streamQueue = new(Enumerable
                .Range(0, parallelStreams)
                .Select(i => new MemoryStream()));
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CreateCredentials(options.Authentication);
            var kustoClient = new KustoClient(
                options.DbUri,
                options.IngestionTable,
                options.IngestionFormat,
                options.IngestionMapping,
                credentials);
            var template = await kustoClient.FetchTemplateAsync(options.TemplateTable, ct);
            var generator = await EventGenerator.CreateAsync(template, kustoClient, ct);

            return new MainOrchestration(
                generator,
                kustoClient,
                options.BlobSize * 1000000,
                options.ParallelStreams);
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
            await Task.WhenAll(_uploadTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                while (!_streamQueue.Any())
                {
                    if (_uploadTaskQueue.TryDequeue(out var uploadTask))
                    {
                        await uploadTask;
                    }
                    else
                    {
                        throw new InvalidOperationException(
                            "There should be uploads since there are no stream");
                    }
                }
                if (_streamQueue.TryDequeue(out var stream))
                {
                    GenerateBlob(stream, ct);
                    _uploadTaskQueue.Enqueue(UploadDataAsync(stream, ct));
                }
                else
                {
                    throw new InvalidOperationException("There should be stream available here");
                }
            }
        }

        private async Task UploadDataAsync(MemoryStream stream, CancellationToken ct)
        {
            stream.Position = 0;

            await _kustoClient.IngestAsync(stream, ct);
            Trace.WriteLine($"{DateTime.Now}:  {stream.Length} bytes");
            stream.SetLength(0);
            _streamQueue.Enqueue(stream);
        }

        private void GenerateBlob(MemoryStream compressedStream, CancellationToken ct)
        {
            using (var compressingStream =
                new GZipStream(compressedStream, CompressionLevel.Fastest, true))
            using (var writer = new StreamWriter(compressingStream))
            {
                while (compressedStream.Length < _blobSizeInBytes && !ct.IsCancellationRequested)
                {
                    _generator.GenerateEvent(writer);
                }
            }
        }
    }
}