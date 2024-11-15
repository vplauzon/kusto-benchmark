using Azure.Core;
using Azure.Identity;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection.Emit;
using System.Text;
using static System.Net.Mime.MediaTypeNames;

namespace QueryConsole
{
    internal class QueryOrchestration : IAsyncDisposable
    {
        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(10);

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
            var reportStart = DateTime.Now;
            var reportQueryCount = 0;
            var reportErrorCount = 0;

            await Task.CompletedTask;

            while (!ct.IsCancellationRequested)
            {
                reportErrorCount += await CleanQueueAsync();

                _queryTaskQueue.Enqueue(InvokeQueryAsync(builder, ct));
                ++queryCount;
                ++reportQueryCount;
                if (DateTime.Now - reportStart > PERIOD)
                {   //  Let's report
                    var reportStartText = reportStart.ToString("yyyy-MM-dd HH:mm:ss.ffff");

                    Console.WriteLine(
                        $"#metric# Timestamp={reportStartText}, QueryCount={reportQueryCount}, "
                        + $"ErrorCount={reportErrorCount}");
                    reportStart += PERIOD;
                    reportQueryCount = 0;
                    reportErrorCount = 0;
                }

                var t = DateTime.Now - minuteStart;

                if (t >= TimeSpan.FromMinutes(1) || queryCount >= _queriesPerMinute)
                {
                    minuteStart += TimeSpan.FromMinutes(1);
                    queryCount = 0;
                    await WaitUntilAsync(minuteStart - DateTime.Now);
                }
                else
                {
                    var expectedTime = TimeSpan.FromMinutes(1) * queryCount / _queriesPerMinute;

                    await WaitUntilAsync(expectedTime - t);
                }
            }
        }

        private async Task WaitUntilAsync(TimeSpan delta)
        {
            if (delta > TimeSpan.Zero)
            {
                await Task.Delay(delta);
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