using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
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
            while (await ProcessStepAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private async Task<bool> ProcessStepAsync(CancellationToken ct)
        {
            var allItems = await _logBlobClient.LoadAllAsync(ct);
            var now = DateTime.Now;
            var experimentStepItems = allItems.Result
                .Where(i => i.ExperimentStepItem != null)
                .Select(i => i.ExperimentStepItem!)
                .OrderByDescending(i => i.StartTime)
                .ToArray();

            if (experimentStepItems.Length != 0
                && experimentStepItems[0].EndTime > now + BEFORE_EXPERIMENT_DURATION)
            {
                Console.WriteLine($"Await sub experiments completion");
                await TaskHelper.Until(
                    experimentStepItems[0].EndTime + BEFORE_EXPERIMENT_DURATION,
                    ct);

                return true;
            }
            else
            {
                return await StartExperimentStepAsync(experimentStepItems, ct);
            }
        }

        private async Task<bool> StartExperimentStepAsync(
            ExperimentStepItem[] experimentStepItems,
            CancellationToken ct)
        {
            var startTime = DateTime.Now.Add(BEFORE_EXPERIMENT_DURATION);
            var endTime = startTime.Add(_config.SubExperimentDuration);
            var subExperimentStepItemTasks = _config.SubExperiments
                .Select(s => CreateSubExperimentStepAsync(s, experimentStepItems, ct))
                .ToArray();

            await Task.WhenAll(subExperimentStepItemTasks);

            var subExperimentStepItemPairs = subExperimentStepItemTasks
                .Select(t => t.Result)
                .Where(r => r != null)
                .Select(r => r!.Value)
                .ToArray();

            if (subExperimentStepItemPairs.Length != 0)
            {
                var newItem = LogItem.Create(new ExperimentStepItem(
                    startTime,
                    endTime,
                    subExperimentStepItemPairs.ToDictionary()));
                var totalInstanceCount = 1 + newItem.ExperimentStepItem!
                    .SubExperimentStepItemMap
                    .Values
                    .Sum(s => s.NodeCount);

                Console.WriteLine($"Starting experiment step with {totalInstanceCount} nodes");
                await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);
                await _logBlobClient.AppendAsync(newItem, null, ct);
                Console.WriteLine($"Experiment step created");
                return true;
            }
            else
            {
                return false;
            }
        }

        private async Task<KeyValuePair<string, SubExperimentStepItem>?> CreateSubExperimentStepAsync(
            SubExperimentConfig subExperimentConfig,
            ExperimentStepItem[] experimentStepItems,
            CancellationToken ct)
        {
            await Task.CompletedTask;

            if (experimentStepItems.Length == 0)
            {
                return KeyValuePair.Create(
                    subExperimentConfig.SubExperimentName,
                    new SubExperimentStepItem(1, subExperimentConfig.ThroughputTargetStart));
            }
            else if (experimentStepItems[0].SubExperimentStepItemMap.TryGetValue(
                subExperimentConfig.SubExperimentName,
                out var lastSubExperimentItem))
            {
                return KeyValuePair.Create(
                    subExperimentConfig.SubExperimentName,
                    new SubExperimentStepItem(1, subExperimentConfig.ThroughputTargetStart));
            }
            else
            {
                return null;
            }
        }
    }
}