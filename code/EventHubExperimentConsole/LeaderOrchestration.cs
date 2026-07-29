using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal class LeaderOrchestration
    {
        private readonly static TimeSpan BEFORE_EXPERIMENT_DURATION = TimeSpan.FromSeconds(30);

        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly InstanceManager _instanceManager;

        public LeaderOrchestration(
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

        public async Task ProcessAsync(CancellationToken ct)
        {
            var allItems = await _logBlobClient.LoadAllAsync(ct);
            var subExperimentItems = allItems.Result
                .Where(i => i.SubExperimentItem != null)
                .Select(i => i.SubExperimentItem);
            var startTime = DateTime.Now.Add(BEFORE_EXPERIMENT_DURATION);
            var endTime = startTime.Add(_config.SubExperimentDuration);
            var newItems = _config.SubExperiments
                .Select(s => LogItem.Create(new SubExperimentItem(
                    s.SubExperimentName,
                    1,
                    s.ThroughputTargetStart,
                    startTime,
                    endTime)));
            var totalInstanceCount = 1 + newItems.Sum(i => i.SubExperimentItem!.NodeCount);

            Console.WriteLine($"Creating sub experiments with {totalInstanceCount} nodes");
            await _logBlobClient.AppendAsync(newItems, null, ct);
            await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);
            Console.WriteLine($"Sub experiments created");
        }
    }
}