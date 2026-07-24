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
                var nodeId = Guid.NewGuid();

                await using (var registration =
                    await RegistrationManager.RegisterAsync(_logBlobClient, nodeId, ct))
                {
                    Console.WriteLine("Experiment configuration:");
                    _config.DisplayConfig();
                    Console.WriteLine();

                    //throw new NotImplementedException();
                    await Task.Delay(TimeSpan.FromDays(1));
                }
            }
        }

        private static IEnumerable<LogItem> CompactLogItems(IEnumerable<LogItem> items)
        {
            var ttlRegistrationItems = items
                .Where(i => i.TtlRegistrationItem != null)
                //  Keep non-expired item
                .Where(i => !i.TtlRegistrationItem!.IsExpired)
                //  Keep last registered item (by experiment name / node index)
                .GroupBy(i => i.TtlRegistrationItem!.NodeItem)
                .Select(g => g.OrderBy(i => i.TtlRegistrationItem!.ExpirationTime).Last());

            return ttlRegistrationItems;
        }
    }
}