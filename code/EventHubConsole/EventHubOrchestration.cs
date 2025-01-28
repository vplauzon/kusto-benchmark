using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace EventHubConsole
{
    internal class EventHubOrchestration : IAsyncDisposable
    {
        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(10);

        private readonly ExpressionGenerator _generator;
        private readonly EventHubProducerClient _eventHubProducerClient;
        private readonly long _rate;
        private readonly int _recordsPerPayload;
        private readonly int _batchSize;
        private readonly ConcurrentQueue<MemoryStream> _streamQueue;
        private readonly ConcurrentQueue<Task> _sendTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            long rate,
            int recordsPerPayload,
            int batchSize,
            int parallelPartitions)
        {
            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _rate = rate;
            _recordsPerPayload = recordsPerPayload;
            _batchSize = batchSize;
            _streamQueue = new(Enumerable
                .Range(0, parallelPartitions)
                .Select(i => new MemoryStream()));
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CredentialFactory.CreateCredentials(options.Authentication);
            var template = options.TemplateText;
            var generator = await ExpressionGenerator.CreateAsync(template, null, ct);
            var eventHubProducerClient = new EventHubProducerClient(
                options.Fqdn,
                options.EventHub,
                credentials);

            return new EventHubOrchestration(
                generator,
                eventHubProducerClient,
                options.Rate,
                options.RecordsPerPayload,
                options.BatchSize,
                options.ParallelPartitions);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _eventHubProducerClient.DisposeAsync();
            await Task.WhenAll(_sendTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var metricWriter = new IngestionMetricWriter();

            while (!ct.IsCancellationRequested)
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
                if (_streamQueue.TryDequeue(out var stream))
                {
                    _sendTaskQueue.Enqueue(SendBatchAsync(stream, metricWriter, ct));
                }
            }
        }

        private async Task SendBatchAsync(
            MemoryStream compressedStream,
            IngestionMetricWriter metricWriter,
            CancellationToken ct)
        {
            using (EventDataBatch eventBatch = await _eventHubProducerClient.CreateBatchAsync())
            {
                long uncompressedVolume = 0;
                long compressedVolume = 0;
                long rowCount = 0;
                var stopwatch = new Stopwatch();

                stopwatch.Start();
                for (var i = 0; i != _batchSize; ++i)
                {
                    compressedStream.SetLength(0);
                    using (var compressingStream =
                        new GZipStream(compressedStream, CompressionLevel.Fastest, true))
                    using (var writer = new StreamWriter(compressingStream))
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
                        if (eventBatch.TryAdd(new EventData(compressedStream.ToArray())))
                        {
                            uncompressedVolume += payloadUncompressedVolume;
                            rowCount += payloadRowCount;
                            compressedVolume += compressedStream.Length;
                        }
                    }
                }

                // Send the batch of events
                await _eventHubProducerClient.SendAsync(eventBatch);

                _streamQueue.Enqueue(compressedStream);
                metricWriter.Write(
                    stopwatch.Elapsed,
                    uncompressedVolume,
                    compressedVolume,
                    rowCount);
            }
        }
    }
}