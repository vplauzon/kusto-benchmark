using BenchmarkLib;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal class ExperimentOrchestration : IAsyncDisposable
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;

        #region Constructors
        private ExperimentOrchestration(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient)
        {
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
        }

        public static async Task<ExperimentOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            Uri GetFolderUri(string blobUri)
            {
                var builder = new UriBuilder(blobUri);

                builder.Path = string.Join('/', builder.Path.Split('/').SkipLast(1));

                return builder.Uri;
            }

            Uri GetLogUri(Uri folderUri)
            {
                var builder = new UriBuilder(folderUri);

                builder.Path = $"{builder.Path}/logs.json";

                return builder.Uri;
            }

            var folderUri = GetFolderUri(options.ConfigUri);
            var folderName = folderUri.Segments.Last();
            var logUri = GetLogUri(folderUri);
            var credential = await CredentialFactory.CreateCredentialsAsync(options.Authentication);
            var config = await ExperimentConfig.LoadAsync(
                options.ConfigUri,
                credential,
                ct);
            var logBlobClient =
                await LogBlobClient<LogItem>.CreateAsync(logUri, CompactLogItems, credential, ct);

            return new ExperimentOrchestration(folderName, config, logBlobClient);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            ((IDisposable)_logBlobClient).Dispose();

            await ValueTask.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await RegisterAsync(ct);
                throw new NotImplementedException();
            }
        }

        private async Task RegisterAsync(CancellationToken ct)
        {
            var result = await _logBlobClient.LoadAllAsync(ct);

            throw new NotImplementedException();
        }

        private static IEnumerable<LogItem> CompactLogItems(IEnumerable<LogItem> items)
        {
            return items;
        }
    }
}