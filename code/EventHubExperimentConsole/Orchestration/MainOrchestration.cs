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
            var credential = await CredentialFactory.CreateCredentialsAsync(
                options.Authentication);
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
                    if (registration.IsLeader)
                    {
                        var orchestration = await LeaderOrchestration.CreateAsync(
                            _experimentName,
                            _config,
                            _logBlobClient,
                            _instanceManager,
                            _credential,
                            _nodeId);

                        await orchestration.ProcessAsync(ct);
                    }
                    else
                    {
                        while (!ct.IsCancellationRequested)
                        {
                            var nodeItem = await registration.WaitForAssignmentAsync(ct);
                            if (registration.IsLeader)
                            {
                                var leaderOrchestration = await LeaderOrchestration.CreateAsync(
                                    _experimentName,
                                    _config,
                                    _logBlobClient,
                                    _instanceManager,
                                    _credential,
                                    _nodeId);
                                await leaderOrchestration.ProcessAsync(ct);
                                break;
                            }
                            if (nodeItem == null)
                            {
                                continue;
                            }

                            var orchestration = new SubExperimentOrchestration(
                                _experimentName,
                                _config,
                                _logBlobClient,
                                nodeItem);

                            await orchestration.ProcessAsync(ct);
                        }
                    }
                }
            }
        }

        private static IEnumerable<LogItem> CompactLogItems(IEnumerable<LogItem> items)
        {
            var itemArray = items.ToArray();
            var nodeRegistrationItems = itemArray
                .Where(i => i.NodeRegistrationItem != null)
                .GroupBy(i => i.NodeRegistrationItem!.NodeId)
                .Select(g => g.Last())
                .Where(i => !i.NodeRegistrationItem!.IsExpired)
                .ToArray();
            var activeNodeIds = nodeRegistrationItems
                .Select(i => i.NodeRegistrationItem!.NodeId)
                .ToHashSet();
            var nodeAssignationItems = itemArray
                .Where(i => i.NodeAssignationItem != null)
                .GroupBy(i => i.NodeAssignationItem!.NodeId)
                .Select(g => g.Last())
                .Where(i => activeNodeIds.Contains(i.NodeAssignationItem!.NodeId));
            var experimentStepItems = itemArray
                .Where(i => i.ExperimentStepItem != null);

            return nodeRegistrationItems
                .Concat(nodeAssignationItems)
                .Concat(experimentStepItems);
        }
    }
}