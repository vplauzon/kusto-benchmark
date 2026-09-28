using Azure.Core;
using BenchmarkLib;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly InstanceManager _instanceManager;
        private readonly TokenCredential _credential;
        private readonly Guid _nodeId;

        #region Constructors
        private MainOrchestration(
             string experimentName,
             ExperimentConfig config,
             LogBlobClient<LogItem> logBlobClient,
             InstanceManager instanceManager,
             TokenCredential credential,
             Guid nodeId)
        {
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
            _instanceManager = instanceManager;
            _credential = credential;
            _nodeId = nodeId;
        }

        public static async Task<MainOrchestration> CreateAsync(
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
            var nodeId = Guid.NewGuid();

            return new MainOrchestration(
                folderName,
                config,
                logBlobClient,
                instanceManager,
                credential,
                nodeId);
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
                Console.WriteLine("Experiment configuration:");
                _config.DisplayConfig();
                Console.WriteLine();

                await using (var registration =
                    await RegistrationManager.RegisterAsync(_logBlobClient, _nodeId, ct))
                {
                    if (registration.NodeItem == null)
                    {
                        var orchestration = await LeaderOrchestration.CreateAsync(
                            _experimentName,
                            _config,
                            _logBlobClient,
                            _instanceManager,
                            _credential);

                        await orchestration.ProcessAsync(ct);
                    }
                    else
                    {
                        var orchestration = new SubExperimentOrchestration(
                            _experimentName,
                            _config,
                            _logBlobClient,
                            registration.NodeItem);

                        await orchestration.ProcessAsync(ct);
                    }
                }
            }
        }

        private static IEnumerable<LogItem> CompactLogItems(IEnumerable<LogItem> items)
        {
            var ttlRegistrationItems = items
                .Where(i => i.TtlRegistrationItem != null)
                //  Keep last appended item (by experiment name / node index)
                .GroupBy(i => i.TtlRegistrationItem!.NodeItem)
                .Select(g => g.Last())
                //  Keep non-expired item:  a released registration is appended expired
                .Where(i => !i.TtlRegistrationItem!.IsExpired);
            //  We keep all the steps
            var experimentStepItems = items
                .Where(i => i.ExperimentStepItem != null);

            return ttlRegistrationItems
                .Concat(experimentStepItems);
        }
    }
}