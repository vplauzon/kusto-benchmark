using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
        private readonly ConcurrentQueue<Task> _queryTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator,
            EventHubProducerClient eventHubProducerClient,
            long rate,
            int recordsPerPayload,
            int batchSize)
        {
            _generator = generator;
            _eventHubProducerClient = eventHubProducerClient;
            _rate = rate;
            _recordsPerPayload = recordsPerPayload;
            _batchSize = batchSize;
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
                options.BatchSize);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await _eventHubProducerClient.DisposeAsync();
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            using (var stream = new MemoryStream())
            {
                while (!ct.IsCancellationRequested)
                {
                    var volume = await SendBatchAsync(stream, ct);
                }
            }
        }

        private async Task<long> SendBatchAsync(MemoryStream compressedStream, CancellationToken ct)
        {
            long volume = 0;

            using (EventDataBatch eventBatch = await _eventHubProducerClient.CreateBatchAsync())
            {
                for (var i = 0; i != _batchSize; ++i)
                {
                    compressedStream.SetLength(0);
                    using (var compressingStream =
                        new GZipStream(compressedStream, CompressionLevel.Fastest, true))
                    using (var writer = new StreamWriter(compressingStream))
                    {
                        for (var j = 0; j != _recordsPerPayload; ++j)
                        {
                            volume += _generator.GenerateExpression(writer);
                            writer.WriteLine();
                            ++volume;
                        }
                        writer.Flush();
                        eventBatch.TryAdd(new EventData(compressedStream.ToArray()));
                    }
                }

                // Send the batch of events
                await _eventHubProducerClient.SendAsync(eventBatch);

                return volume;
            }
        }
    }
}