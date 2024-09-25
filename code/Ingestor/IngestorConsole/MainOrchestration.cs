
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly string _sampleText;

        #region Constructors
        private MainOrchestration(string sampleText)
        {
            _sampleText = sampleText;
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CreateCredentials(options.Authentication);
            var engineClient = new EngineClient(options.DbUri, credentials);
            var template = await engineClient.FetchTemplateAsync(options.TemplateTable, ct);
            var generator = await EventGenerator.CreateAsync(template, engineClient, ct);

            throw new NotImplementedException();
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