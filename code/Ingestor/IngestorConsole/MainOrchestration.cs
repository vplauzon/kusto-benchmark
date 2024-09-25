
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly EventGenerator _generator;
        private readonly KustoClient _kustoClient;

        #region Constructors
        private MainOrchestration(EventGenerator generator, KustoClient kustoClient)
        {
            _generator = generator;
            _kustoClient = kustoClient;
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CreateCredentials(options.Authentication);
            var kustoClient = new KustoClient(options.DbUri, credentials);
            var template = await kustoClient.FetchTemplateAsync(options.TemplateTable, ct);
            var generator = await EventGenerator.CreateAsync(template, kustoClient, ct);

            return new MainOrchestration(generator, kustoClient);
        }

        private static TokenCredential CreateCredentials(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication))
            {
                return new DefaultAzureCredential();
            }
            else
            {
                throw new NotImplementedException();
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            await Task.CompletedTask;
        }
    }
}