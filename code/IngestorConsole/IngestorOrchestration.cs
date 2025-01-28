using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using BenchmarkLib;

namespace IngestorConsole
{
    internal partial class IngestorOrchestration : IAsyncDisposable
    {
        private readonly ExpressionGenerator _generator;
        private readonly KustoEngineClient _kustoEngineClient;
        private readonly KustoIngestClient _kustoIngestClient;
        private readonly long _rowCount;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _uploadTaskQueue = new();

        #region Constructors
        private IngestorOrchestration(
            ExpressionGenerator generator,
            KustoEngineClient kustoEngineClient,
            KustoIngestClient kustoIngestClient,
            long rowCount,
            int parallelStreams)
        {
            if (rowCount < 100)
            {
                throw new ArgumentOutOfRangeException(nameof(rowCount), "Must be at least 100");
            }
            _generator = generator;
            _kustoEngineClient = kustoEngineClient;
            _kustoIngestClient = kustoIngestClient;
            _rowCount = rowCount;
            _streamQueue = new(Enumerable
                .Range(0, parallelStreams)
                .Select(i => new MemoryStream()));
        }

        public static async Task<IngestorOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CredentialFactory.CreateCredentials(options.Authentication);
            var kustoEngineClient = new KustoEngineClient(options.DbUri, credentials);
            var kustoIngestClient = new KustoIngestClient(
                options.DbUri,
                options.IngestionTable,
                options.IngestionFormat,
                options.IngestionMapping,
                credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(options.TemplateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);

            return new IngestorOrchestration(
                generator,
                kustoEngineClient,
                kustoIngestClient,
                options.RowCount,
                options.ParallelStreams);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.WhenAll(_uploadTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var metricWriter = new IngestionMetricWriter();

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
                    var stopwatch = new Stopwatch();

                    stopwatch.Start();

                    (var uncompressedSize, var rowCount) = GenerateBlob(stream, ct);

                    _uploadTaskQueue.Enqueue(UploadDataAsync(
                        metricWriter,
                        stream,
                        stopwatch,
                        uncompressedSize,
                        rowCount,
                        ct));
                }
                else
                {
                    throw new InvalidOperationException("There should be stream available here");
                }
            }
        }

        private async Task UploadDataAsync(
            IngestionMetricWriter metricWriter,
            MemoryStream stream,
            Stopwatch stopwatch,
            long uncompressedSize,
            long rowCount,
            CancellationToken ct)
        {
            var compressedSize = stream.Length;

            stream.Position = 0;

            await _kustoIngestClient.IngestAsync(stream, ct);
            stream.SetLength(0);
            _streamQueue.Enqueue(stream);
            metricWriter.Write(stopwatch.Elapsed, uncompressedSize, compressedSize, rowCount);
        }

        private (long uncompressedSize, long rowCount) GenerateBlob(
            MemoryStream compressedStream,
            CancellationToken ct)
        {
            long size = 0;
            long rowCount = 0;

            using (var compressingStream =
                new GZipStream(compressedStream, CompressionLevel.Fastest, true))
            using (var writer = new StreamWriter(compressingStream))
            {
                while (rowCount < _rowCount && !ct.IsCancellationRequested)
                {
                    size += _generator.GenerateExpression(writer);
                    ++rowCount;
                }
            }

            return (size, rowCount);
        }
    }
}