using Azure.Core;
using Azure.Identity;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Text;

namespace QueryConsole
{
    internal class QueryOrchestration : IAsyncDisposable
    {
        private readonly ExpressionGenerator _generator;
        private readonly KustoEngineClient _kustoEngineClient;
        private readonly int _queriesPerMinute;
        private readonly ConcurrentQueue<Task> _queryTaskQueue = new();

        #region Constructors
        private QueryOrchestration(
            ExpressionGenerator generator,
            KustoEngineClient kustoEngineClient,
            int queriesPerMinute)
        {
            if (queriesPerMinute < 1)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(queriesPerMinute),
                    "Must be at least 1");
            }
            _generator = generator;
            _kustoEngineClient = kustoEngineClient;
            _queriesPerMinute = queriesPerMinute;
        }

        public static async Task<QueryOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CreateCredentials(options.Authentication);
            var kustoEngineClient = new KustoEngineClient(options.DbUri, credentials);
            var template = await kustoEngineClient.FetchTemplateAsync(options.TemplateName, ct);
            var generator = await ExpressionGenerator.CreateAsync(template, kustoEngineClient, ct);

            return new QueryOrchestration(generator, kustoEngineClient, options.QueriesPerMinute);
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
            await Task.WhenAll(_queryTaskQueue);
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var builder = new StringBuilder();
            var minuteStart = DateTime.Now;
            var queryCount = 0;
            var errorCount = 0;

            await Task.CompletedTask;

            while (!ct.IsCancellationRequested)
            {
                _queryTaskQueue.Enqueue(InvokeQueryAsync(builder, ct));
                ++queryCount;
                errorCount += await CleanQueueAsync();

                var delta = DateTime.Now - minuteStart;

                if (delta < TimeSpan.FromMinutes(1))
                {
                    var elapsedPercent = delta / TimeSpan.FromMinutes(1);
                    var expectedHits = elapsedPercent * _queriesPerMinute;

                    if (expectedHits <= queryCount)
                    {   //  We're good, let's go to sleep
                        var nextHitTimespan =
                            (float)queryCount / _queriesPerMinute * TimeSpan.FromMinutes(1);
                        var timeToGo = nextHitTimespan.Subtract(delta);

                        if (timeToGo > TimeSpan.FromMilliseconds(100))
                        {
                            await Task.Delay(timeToGo);
                        }
                        else
                        {   //  Not enough time to wait for, let's just go
                        }
                    }
                    else
                    {   //  We are falling behind, no break, let's query again
                    }
                }
                else
                {   //  Minute is completed
                    Console.WriteLine(
                        $"#metric# Timestamp={minuteStart}, QueryCount={queryCount}, "
                        + $"ErrorCount={errorCount}");
                    minuteStart += TimeSpan.FromMinutes(1);
                    queryCount = 0;
                    errorCount = 0;
                }
            }
        }

        private async Task InvokeQueryAsync(StringBuilder builder, CancellationToken ct)
        {
            builder.Clear();
            using (var writer = new StringWriter(builder))
            {
                _generator.GenerateExpression(writer);
                writer.Flush();

                var query = builder.ToString();

                await _kustoEngineClient.QueryAsync(query, ct);
            }
        }

        private async Task<int> CleanQueueAsync()
        {
            var errorCount = 0;

            while (_queryTaskQueue.TryPeek(out var task) && task.IsCompleted)
            {
                if (_queryTaskQueue.TryDequeue(out var task2))
                {
                    try
                    {
                        await task2;
                    }
                    catch
                    {
                        ++errorCount;
                    }
                }
                else
                {
                    throw new NotSupportedException("Queue should still have a task");
                }
            }

            return errorCount;
        }
    }
}