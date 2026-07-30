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
        private readonly InstanceManager _instanceManager;

        #region Constructors
        private ExperimentOrchestration(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient,
            InstanceManager instanceManager)
        {
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
            _instanceManager = instanceManager;
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
            var instanceManager = new InstanceManager(config.ContainerAppId, credential);

            return new ExperimentOrchestration(folderName, config, logBlobClient, instanceManager);
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

                Console.WriteLine("Experiment configuration:");
                _config.DisplayConfig();
                Console.WriteLine();

                await using (var registration =
                    await RegistrationManager.RegisterAsync(_logBlobClient, nodeId, ct))
                {
                    if (registration.NodeItem == null)
                    {
                        var orchestration = new LeaderOrchestration(
                            _experimentName,
                            _config,
                            _logBlobClient,
                            _instanceManager);

                        await orchestration.ProcessAsync(ct);
                    }
                    else
                    {
                        var orchestration = new NonLeaderOrchestration(
                            _experimentName,
                            _config,
                            _logBlobClient);

                        await orchestration.ProcessAsync(ct);
                    }
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
            var subExperimentItems = items
                .Where(i => i.SubExperimentItem != null);

            return ttlRegistrationItems
                .Concat(subExperimentItems);
        }
    }
}