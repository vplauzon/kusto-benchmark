using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.Compression;
using System.Text;

namespace EventHubConsole
{
    internal class EventHubOrchestration : IAsyncDisposable
    {
        #region Inner Types
        private record BatchSendingOutput(long VolumeSent, Task SendingTask);
        #endregion

        //  Hard coded constant, just to better exploit networking capacity
        private const int PARALLEL_PARTITION = 5;
        private static readonly TimeSpan PAUSE_DURATION = TimeSpan.FromMicroseconds(0.1);

        private readonly ExpressionGenerator _generator;
        private readonly EventHubProducerClient _eventHubProducerClient;
        private readonly int _targetBytePerMinute;
        private readonly int _recordsPerPayload;
        private readonly TimeSpan _maxTimeBetweenBatches;
        private readonly int _maxBatchSize;
        private readonly bool _isOutputCompressed;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            int targetMbPerMinute,
            int recordsPerPayload,
            TimeSpan maxTimeBetweenBatches,
            int maxBatchSize,
            bool isOutputCompressed)
        {
            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _targetBytePerMinute = targetMbPerMinute * 1000000;
            _recordsPerPayload = recordsPerPayload;
            _maxTimeBetweenBatches = maxTimeBetweenBatches;
            _maxBatchSize = maxBatchSize;
            _isOutputCompressed = isOutputCompressed;
            _streamQueue = new(Enumerable
                .Range(0, PARALLEL_PARTITION)
                .Select(i => new MemoryStream()));
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = await CredentialFactory.CreateCredentialsAsync(options.Authentication);
            var kustoEngineClient = new KustoEngineClient(options.DbUri, credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(options.TemplateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);
            var eventHubProducerClient = string.IsNullOrEmpty(options.EventHubConnectionString)
                ? new EventHubProducerClient(options.Fqdn, options.EventHub, credentials)
                : new EventHubProducerClient(options.EventHubConnectionString);

            Console.WriteLine($"Template:  {template}");

            return new EventHubOrchestration(
                generator,
                eventHubProducerClient,
                options.TargetThroughput,
                options.RecordsPerPayload,
                options.MaxTimeBetweenBatches,
                options.MaxBatchSize,
                options.IsOutputCompressed);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _eventHubProducerClient.DisposeAsync();
            await Task.WhenAll(_sendTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            await using var metricWriter = new IngestionMetricWriter();
            var watch = new Stopwatch();
            var volume = (long)0;
            var lastBatch = DateTime.MinValue;

            watch.Start();
            while (!ct.IsCancellationRequested)
            {
                await ObserveSendTasksAsync();

                var expectedVolume =
                    (long)(watch.Elapsed / TimeSpan.FromMinutes(1) * _targetBytePerMinute);
                var deltaVolume = expectedVolume - volume;
                var deltaTime = DateTime.Now - lastBatch;

                if (deltaVolume > 0
                    && (deltaVolume > _maxBatchSize || deltaTime > _maxTimeBetweenBatches)
                    && _streamQueue.TryDequeue(out var stream))
                {
                    var sendingOutput =
                        await SendDataAsync(deltaVolume, stream, metricWriter, ct);

                    volume += sendingOutput.VolumeSent;
                    lastBatch = DateTime.Now;
                    _sendTaskQueue.Enqueue(sendingOutput.SendingTask);
                }
                else
                {
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
            IngestionMetricWriter metricWriter,
            CancellationToken ct)
        {
            var eventBatch = await _eventHubProducerClient.CreateBatchAsync(ct);
            long uncompressedVolume = 0;
            long compressedVolume = 0;
            long rowCount = 0;
            var stopwatch = new Stopwatch();

            stopwatch.Start();
            for (var i = 0; (i < _maxBatchSize && uncompressedVolume < targetVolume); ++i)
            {
                Stream payloadStream = _isOutputCompressed
                    ? new GZipStream(outputStream, CompressionLevel.Fastest, true)
                    : outputStream;
                long payloadUncompressedVolume = 0;
                long payloadRowCount = 0;

                outputStream.SetLength(0);
                using (var writer = new StreamWriter(payloadStream, leaveOpen: true))
                {
                    for (var j = 0; j != _recordsPerPayload; ++j)
                    {
                        payloadUncompressedVolume += _generator.GenerateExpression(writer);
                        ++payloadRowCount;
                    }
                }
                if (eventBatch.TryAdd(new EventData(outputStream.ToArray())))
                {
                    uncompressedVolume += payloadUncompressedVolume;
                    rowCount += payloadRowCount;
                    compressedVolume += outputStream.Length;
                }
                else
                {
                    Console.WriteLine($"Can't add event #{i} to batch");
                }
                if (_isOutputCompressed)
                {
                    payloadStream.Dispose();
                }
            }

            var sendingTask = SendBatchAsync(eventBatch, outputStream);

            metricWriter.Write(
                stopwatch.Elapsed,
                uncompressedVolume,
                compressedVolume,
                rowCount);

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