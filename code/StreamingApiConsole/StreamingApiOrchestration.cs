using BenchmarkLib;
using Kusto.Data.Common;
using Kusto.Ingest.V2;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Compression;
using System.Text;

namespace StreamingApiConsole
{
    public class StreamingApiOrchestration : IAsyncDisposable
    {
        #region Inner Types
        private record BatchSendingOutput(long VolumeSent, Task SendingTask);
        #endregion

        private const long BATCH_SIZE = 6 * 1000000;
        private const string BATCH_COUNT = "BatchCount";
        private const string EVENT_COUNT = "EventCount";
        private const string RECORD_COUNT = "RecordCount";
        private const string UNCOMPRESSED_SIZE = "UncompressedSize";
        private const string COMPRESSED_SIZE = "CompressedSize";
        private static readonly TimeSpan PAUSE_DURATION = TimeSpan.FromSeconds(0.1);

        private readonly IImmutableList<string> _dimensionNames;
        private readonly IImmutableList<string> _dimensionValues;
        private readonly ExpressionGenerator _generator;
        private readonly KustoStreamingIngestionClient _kustoStreamingIngestionClient;
        private readonly double _targetBytePerMinute;
        private readonly double _targetBytePerBatch;
        private readonly bool _isOutputCompressed;
        private readonly DateTime _endTime;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private StreamingApiOrchestration(
            IEnumerable<string> dimensionNames,
            IEnumerable<string> dimensionValues,
            ExpressionGenerator generator,
            KustoStreamingIngestionClient kustoStreamingIngestionClient,
            double targetMbPerMinute,
            bool isOutputCompressed,
            DateTime endTime)
        {
            var targetBytePerMinute = targetMbPerMinute * 1000000d;
            var targetBytePerSecond = targetBytePerMinute / 60d;
            var fractionalBatchCount = targetBytePerSecond / BATCH_SIZE;
            var batchCount = (int)fractionalBatchCount + 1;
            var targetBytePerBatch = targetBytePerSecond / batchCount;

            _dimensionNames = dimensionNames.ToImmutableArray();
            _dimensionValues = dimensionValues.ToImmutableArray();
            _generator = generator;
            _kustoStreamingIngestionClient = kustoStreamingIngestionClient;
            _targetBytePerMinute = targetBytePerMinute;
            _targetBytePerBatch = Math.Max(1d, targetBytePerBatch);
            _isOutputCompressed = isOutputCompressed;
            _endTime = endTime;
            //  We provision twice as much streams as prescribed in case delays are introduced
            _streamQueue = new(Enumerable
                .Range(0, 2 * batchCount)
                .Select(i => new MemoryStream()));
            Console.WriteLine($"Target byte per minute:  {_targetBytePerMinute}");
            Console.WriteLine($"Target byte per second:  {targetBytePerSecond}");
            Console.WriteLine($"Target byte per batch:  {_targetBytePerBatch}");
        }

        public static async Task<StreamingApiOrchestration> CreateAsync(
            IEnumerable<string> dimensionNames,
            IEnumerable<string> dimensionValues,
            string authentication,
            Uri dbUri,
            string templateName,
            Uri tableUri,
            double targetMbPerMinute,
            DataSourceFormat dataFormat,
            string dataMapping,
            bool isOutputCompressed,
            DateTime endTime,
            CancellationToken ct)
        {
            var credentials = await CredentialFactory.CreateCredentialsAsync(authentication);
            var kustoEngineClient = new KustoEngineClient(dbUri, credentials);
            var kustoStreamingIngestionClient = new KustoStreamingIngestionClient(
                tableUri,
                dataFormat,
                dataMapping,
                isOutputCompressed,
                credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(templateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);

            Console.WriteLine($"Template:  {template}");
            Console.WriteLine("Warming up...");
            await kustoStreamingIngestionClient.WarmupAsync(ct);
            Console.WriteLine("Warmed up");

            return new StreamingApiOrchestration(
                dimensionNames,
                dimensionValues,
                generator,
                kustoStreamingIngestionClient,
                targetMbPerMinute,
                isOutputCompressed,
                endTime);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            ((IDisposable)_kustoStreamingIngestionClient).Dispose();
            await Task.WhenAll(_sendTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            await using var metricWriter = new MetricWriter(
                _dimensionNames,
                [BATCH_COUNT, EVENT_COUNT, RECORD_COUNT, UNCOMPRESSED_SIZE, COMPRESSED_SIZE],
                TimeSpan.FromMinutes(1));
            var watch = new Stopwatch();
            var volume = (long)0;
            var lastBatch = DateTime.MinValue;

            watch.Start();
            while (!ct.IsCancellationRequested && DateTime.Now < _endTime)
            {
                await ObserveSendTasksAsync();

                var expectedVolume =
                    (long)(watch.Elapsed / TimeSpan.FromMinutes(1) * _targetBytePerMinute);
                var deltaVolume = expectedVolume - volume;
                var deltaTime = DateTime.Now - lastBatch;

                if (deltaVolume > _targetBytePerBatch && TryDequeueStream(out var stream))
                {
                    var sendingOutput =
                        await SendDataAsync(deltaVolume, stream, metricWriter, ct);

                    volume += sendingOutput.VolumeSent;
                    lastBatch = DateTime.Now;
                    _sendTaskQueue.Enqueue(sendingOutput.SendingTask);
                }
                else
                {
                    await Task.WhenAny(_sendTaskQueue);
                }
            }
        }

        private bool TryDequeueStream([NotNullWhen(true)] out MemoryStream? stream)
        {
            if (_streamQueue.TryDequeue(out stream))
            {
                return true;
            }
            else
            {
                Console.WriteLine("Stream queue throttling");

                return false;
            }
        }

        private async Task ObserveSendTasksAsync()
        {
            while (_sendTaskQueue.Any())
            {
                if (_sendTaskQueue.TryPeek(out var sendTask))
                {
                    if (sendTask.IsCompleted)
                    {
                        if (_sendTaskQueue.TryDequeue(out var sendTask2))
                        {
                            await sendTask2;
                        }
                    }
                    else
                    {
                        return;
                    }
                }
                else
                {
                    throw new InvalidOperationException(
                        "There should be sends since there are no stream");
                }
            }
        }

        private async Task<BatchSendingOutput> SendDataAsync(
            long targetVolume,
            MemoryStream outputStream,
            MetricWriter metricWriter,
            CancellationToken ct)
        {
            long uncompressedVolume = 0;
            long compressedVolume = 0;
            long eventCount = 0;
            var stopwatch = new Stopwatch();
            Stream payloadStream = _isOutputCompressed
                ? new GZipStream(outputStream, CompressionLevel.Fastest, leaveOpen: true)
                : outputStream;

            stopwatch.Start();
            outputStream.SetLength(0);
            using (var writer = new StreamWriter(payloadStream, leaveOpen: true))
            {
                while (uncompressedVolume < targetVolume && uncompressedVolume < _targetBytePerBatch)
                {
                    uncompressedVolume += _generator.GenerateExpression(writer);
                    ++eventCount;
                }
            }
            if (_isOutputCompressed)
            {
                payloadStream.Flush();
                payloadStream.Dispose();
            }
            compressedVolume += outputStream.Length;
            outputStream.Position = 0;

            var sendingTask = SendBatchAsync(outputStream);

            metricWriter.WriteMetric(_dimensionValues, BATCH_COUNT, 1);
            metricWriter.WriteMetric(_dimensionValues, EVENT_COUNT, eventCount);
            metricWriter.WriteMetric(_dimensionValues, RECORD_COUNT, eventCount);
            metricWriter.WriteMetric(_dimensionValues, UNCOMPRESSED_SIZE, uncompressedVolume);
            metricWriter.WriteMetric(_dimensionValues, COMPRESSED_SIZE, compressedVolume);

            return new BatchSendingOutput(uncompressedVolume, sendingTask);
        }

        private async Task SendBatchAsync(MemoryStream stream)
        {
            await _kustoStreamingIngestionClient.SendAsync(stream);
            _streamQueue.Enqueue(stream);
        }
    }
}