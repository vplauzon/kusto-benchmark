using Azure.Core;
using Azure.Identity;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Reflection.Emit;

namespace QueryConsole
{
    internal class QueryOrchestration: IAsyncDisposable
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
            await Task.CompletedTask;
        }
    }
}