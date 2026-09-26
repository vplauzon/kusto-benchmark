using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace EventHubConsole
{
    public class EventHubOrchestration : IAsyncDisposable
    {
        #region Inner Types
        private record BatchSendingOutput(long VolumeSent, Task SendingTask);
        #endregion

        //  Hard coded constant, just to better exploit networking capacity
        private const int PARALLEL_PARTITION = 5;
        private const string BATCH_COUNT = "BatchCount";
        private const string RECORD_COUNT = "RecordCount";
        private const string UNCOMPRESSED_SIZE = "UncompressedSize";
        private const string COMPRESSED_SIZE = "CompressedSize";
        private static readonly TimeSpan PAUSE_DURATION = TimeSpan.FromMicroseconds(0.1);

        private readonly IImmutableList<string> _dimensionNames;
        private readonly IImmutableList<string> _dimensionValues;
        private readonly ExpressionGenerator _generator;
        private readonly EventHubProducerClient _eventHubProducerClient;
        private readonly int _targetBytePerMinute;
        private readonly int _targetBytePerBatch;
        private readonly bool _isOutputCompressed;
        private readonly DateTime _endTime;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            IEnumerable<string> dimensionNames,
            IEnumerable<string> dimensionValues,
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            int targetMbPerMinute,
            bool isOutputCompressed,
            DateTime endTime)
        {
            var targetBytePerMinute = targetMbPerMinute * 1000000;
            var targetBytePerSecond = targetBytePerMinute / 60;
            var targetBytePerBatch = targetBytePerSecond / 10;

            _dimensionNames = dimensionNames.ToImmutableArray();
            _dimensionValues = dimensionValues.ToImmutableArray();
            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _targetBytePerMinute = targetBytePerMinute;
            _targetBytePerBatch = Math.Min(1, (int)targetBytePerBatch);
            _isOutputCompressed = isOutputCompressed;
            _endTime = endTime;
            _streamQueue = new(Enumerable
                .Range(0, PARALLEL_PARTITION)
                .Select(i => new MemoryStream()));
            Console.WriteLine($"Target byte per minute:  {_targetBytePerMinute}");
            Console.WriteLine($"Target byte per batch:  {_targetBytePerBatch}");
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            IEnumerable<string> dimensionNames,
            IEnumerable<string> dimensionValues,
            string authentication,
            Uri dbUri,
            string templateName,
            string eventHubConnectionString,
            string eventHubFqdn,
            string eventHubName,
            int targetMbPerMinute,
            bool isOutputCompressed,
            DateTime endTime,
            CancellationToken ct)
        {
            var credentials = await CredentialFactory.CreateCredentialsAsync(authentication);
            var kustoEngineClient = new KustoEngineClient(dbUri, credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(templateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);
            var eventHubProducerClient = string.IsNullOrEmpty(eventHubConnectionString)
                ? new EventHubProducerClient(eventHubFqdn, eventHubName, credentials)
                : new EventHubProducerClient(eventHubConnectionString);

            Console.WriteLine($"Template:  {template}");

            return new EventHubOrchestration(
                dimensionNames,
                dimensionValues,
                generator,
                eventHubProducerClient,
                targetMbPerMinute,
                isOutputCompressed,
                endTime);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _eventHubProducerClient.DisposeAsync();
            await Task.WhenAll(_sendTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            await using var metricWriter = new MetricWriter(
                _dimensionNames,
                [BATCH_COUNT, RECORD_COUNT, UNCOMPRESSED_SIZE, COMPRESSED_SIZE],
                TimeSpan.FromSeconds(15));
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

                if (deltaVolume > _targetBytePerBatch && _streamQueue.TryDequeue(out var stream))
                {
                    var sendingOutput =
                        await SendDataAsync(deltaVolume, stream, metricWriter, ct);

                    volume += sendingOutput.VolumeSent;
                    lastBatch = DateTime.Now;
                    _sendTaskQueue.Enqueue(sendingOutput.SendingTask);
                }
                else
                {
                    Console.WriteLine("Stream queue throttling");
                    await Task.Delay(PAUSE_DURATION, ct);
                }
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
            var eventBatch = await _eventHubProducerClient.CreateBatchAsync(ct);
            long uncompressedVolume = 0;
            long compressedVolume = 0;
            long rowCount = 0;
            var isBatchSealed = false;
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            while (!isBatchSealed && uncompressedVolume < targetVolume)
            {
                Stream payloadStream = _isOutputCompressed
                    ? new GZipStream(outputStream, CompressionLevel.Fastest, true)
                    : outputStream;
                long payloadUncompressedVolume = 0;

                outputStream.SetLength(0);
                using (var writer = new StreamWriter(payloadStream, leaveOpen: true))
                {
                    payloadUncompressedVolume += _generator.GenerateExpression(writer);
                }
                isBatchSealed = !eventBatch.TryAdd(new EventData(outputStream.ToArray()));
                if (_isOutputCompressed)
                {
                    payloadStream.Dispose();
                }
                if (!isBatchSealed)
                {
                    uncompressedVolume += payloadUncompressedVolume;
                    compressedVolume += outputStream.Length;
                    ++rowCount;
                }
                else
                {
                    Console.WriteLine($"Can't add event #{rowCount} to batch");
                }
            }

            var sendingTask = SendBatchAsync(eventBatch, outputStream);

            metricWriter.WriteMetric(_dimensionValues, BATCH_COUNT, 1);
            metricWriter.WriteMetric(_dimensionValues, RECORD_COUNT, rowCount);
            metricWriter.WriteMetric(_dimensionValues, UNCOMPRESSED_SIZE, uncompressedVolume);
            metricWriter.WriteMetric(_dimensionValues, COMPRESSED_SIZE, compressedVolume);

            return new BatchSendingOutput(uncompressedVolume, sendingTask);
        }

        private async Task SendBatchAsync(EventDataBatch eventBatch, MemoryStream outputStream)
        {
            await _eventHubProducerClient.SendAsync(eventBatch);
            _streamQueue.Enqueue(outputStream);
            eventBatch.Dispose();
        }
    }
}