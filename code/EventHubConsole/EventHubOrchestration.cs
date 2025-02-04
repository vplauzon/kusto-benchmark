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
        private record BatchSendingOutput(double VolumeSent, Task SendingTask);
        #endregion

        private readonly ExpressionGenerator _generator;
        private readonly EventHubProducerClient _eventHubProducerClient;
        private readonly int _recordsPerPayload;
        private readonly int _batchSize;
        private readonly int _targetMbPerMinute;
        private readonly bool _isOutputCompressed;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            int recordsPerPayload,
            int batchSize,
            int parallelPartitions,
            int targetMbPerMinute,
            bool isOutputCompressed)
        {
            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _recordsPerPayload = recordsPerPayload;
            _batchSize = batchSize;
            _targetMbPerMinute = targetMbPerMinute;
            _isOutputCompressed = isOutputCompressed;
            _streamQueue = new(Enumerable
                .Range(0, parallelPartitions)
                .Select(i => new MemoryStream()));
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CredentialFactory.CreateCredentials(options.Authentication);
            var kustoEngineClient = new KustoEngineClient(options.DbUri, credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(options.TemplateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);
            var eventHubProducerClient = string.IsNullOrEmpty(options.EventHubConnectionString)
                ? new EventHubProducerClient(options.Fqdn, options.EventHub, credentials)
                : new EventHubProducerClient(options.EventHubConnectionString);

            return new EventHubOrchestration(
                generator,
                eventHubProducerClient,
                options.RecordsPerPayload,
                options.BatchSize,
                options.ParallelPartitions,
                options.TargetThroughput,
                options.IsOutputCompressed!.Value);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _eventHubProducerClient.DisposeAsync();
            await Task.WhenAll(_sendTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var minuteWatch = new Stopwatch();
            double minuteVolume = 0;
            await using var metricWriter = new IngestionMetricWriter();

            minuteWatch.Start();
            while (!ct.IsCancellationRequested)
            {
                await AwaitStreamAvailabilityAsync();

                if (minuteWatch.Elapsed > TimeSpan.FromMinutes(1))
                {
                    Console.WriteLine(
                        $"In {minuteWatch.Elapsed}, we sent {minuteVolume / 1000000} MBs");
                    minuteVolume -= GetTargetVolume(minuteWatch.Elapsed);
                    if (minuteVolume < -1000000000)
                    {
                        Console.WriteLine($"Can't keep up, volume is {minuteVolume}");
                    }
                    minuteWatch.Restart();
                }

                var deltaVolume = GetTargetVolume(minuteWatch.Elapsed) - minuteVolume;

                if (deltaVolume > 0 && _streamQueue.TryDequeue(out var stream))
                {
                    var sendingOutput =
                        await SendDataAsync(deltaVolume, stream, metricWriter, ct);

                    minuteVolume += sendingOutput.VolumeSent;
                    _sendTaskQueue.Enqueue(sendingOutput.SendingTask);
                }
            }
        }

        private async Task AwaitStreamAvailabilityAsync()
        {
            while (!_streamQueue.Any())
            {
                if (_sendTaskQueue.TryDequeue(out var sendTask))
                {
                    await sendTask;
                }
                else
                {
                    throw new InvalidOperationException(
                        "There should be sends since there are no stream");
                }
            }
        }

        private double GetTargetVolume(TimeSpan deltaTime)
        {
            return (deltaTime.TotalSeconds / 60 * _targetMbPerMinute) * 1000000;
        }

        private async Task<BatchSendingOutput> SendDataAsync(
            double targetVolume,
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
            for (var i = 0; (i != _batchSize && uncompressedVolume < targetVolume); ++i)
            {
                var payloadStream = CreatePayloadStream(outputStream);

                outputStream.SetLength(0);
                using (var writer = new StreamWriter(payloadStream, leaveOpen: true))
                {
                    long payloadUncompressedVolume = 0;
                    long payloadRowCount = 0;

                    for (var j = 0; j != _recordsPerPayload; ++j)
                    {
                        payloadUncompressedVolume += _generator.GenerateExpression(writer);
                        writer.WriteLine();
                        ++payloadRowCount;
                    }
                    writer.Flush();
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
            eventBatch.Dispose();
            _streamQueue.Enqueue(outputStream);
        }

        private Stream CreatePayloadStream(MemoryStream outputStream)
        {
            return _isOutputCompressed
                ? new GZipStream(outputStream, CompressionLevel.Fastest, true)
                : outputStream;
        }
    }
}