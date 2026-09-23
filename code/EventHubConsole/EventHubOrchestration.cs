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
        private readonly int _targetBytePerBatch;
        private readonly bool _isOutputCompressed;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            int targetMbPerMinute,
            bool isOutputCompressed)
        {
            var targetBytePerMinute = targetMbPerMinute * 1000000;
            var targetBytePerSecond = targetBytePerMinute / 60;
            var targetBytePerBatch = targetBytePerSecond / 10;

            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _targetBytePerMinute = targetBytePerMinute;
            _targetBytePerBatch = Math.Min(1, (int)targetBytePerBatch);
            _isOutputCompressed = isOutputCompressed;
            _streamQueue = new(Enumerable
                .Range(0, PARALLEL_PARTITION)
                .Select(i => new MemoryStream()));
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            string authentication,
            Uri dbUri,
            string templateName,
            string eventHubConnectionString,
            string eventHubFqdn,
            string eventHubName,
            int targetMbPerMinute,
            bool isOutputCompressed,
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
                generator,
                eventHubProducerClient,
                targetMbPerMinute,
                isOutputCompressed);
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
            var isBatchSealed = false;
            var stopwatch = new Stopwatch();
            var i = 0;

            stopwatch.Start();
            while (isBatchSealed && uncompressedVolume < targetVolume)
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
                isBatchSealed = eventBatch.TryAdd(new EventData(outputStream.ToArray()));
                if (_isOutputCompressed)
                {
                    payloadStream.Dispose();
                }
                if (isBatchSealed)
                {
                    uncompressedVolume += payloadUncompressedVolume;
                    compressedVolume += outputStream.Length;
                }
                else
                {
                    Console.WriteLine($"Can't add event #{i} to batch");
                }
                ++i;
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