using BenchmarkLib;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal class ExperimentOrchestration : IAsyncDisposable
    {
        private static readonly TimeSpan REGISTRATION_TTL = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan AWAIT_REGISTRATION_DELAY = TimeSpan.FromSeconds(5);

        private readonly Guid _nodeId = Guid.NewGuid();
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

        private async Task<NodeItem> RegisterAsync(CancellationToken ct)
        {
            while (true)
            {
                var result = await _logBlobClient.LoadAllAsync(ct);
                var ttlRegistrationItems = result.Result
                    .Where(r => r.TtlRegistrationItem != null)
                    .Select(r => r.TtlRegistrationItem!);
                var leaderRegistrationItem = ttlRegistrationItems
                    .FirstOrDefault(i => i.NodeItem.SubExperimentName == null);

                if (leaderRegistrationItem != null && !leaderRegistrationItem.IsExpired)
                {
                    throw new NotImplementedException("Look at more than leader");
                }
                else
                {
                    var nodeItem = new NodeItem(null, 0, _nodeId);
                    var success = await _logBlobClient.AppendAsync(
                        LogItem.Create(new TtlRegistrationItem(
                            nodeItem,
                            DateTime.Now.Add(REGISTRATION_TTL))),
                        result.Tag,
                        ct);

                    if (success)
                    {
                        return nodeItem;
                    }
                    else
                    {
                        await Task.Delay(AWAIT_REGISTRATION_DELAY, ct);
                    }
                }
            }
        }

        private static IEnumerable<LogItem> CompactLogItems(IEnumerable<LogItem> items)
        {
            return items
                .Where(i => i.TtlRegistrationItem == null || !i.TtlRegistrationItem.IsExpired);
        }
    }
}