using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
{
    internal class LeaderOrchestration
    {
        private readonly static TimeSpan BEFORE_EXPERIMENT_DURATION = TimeSpan.FromSeconds(30);

        private readonly static TimeSpan AFTER_EXPERIMENT_DURATION = TimeSpan.FromMinutes(1);

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
            var now = DateTime.UtcNow;
            var experimentStepItems = allItems.Result
                .Where(i => i.ExperimentStepItem != null)
                .Select(i => i.ExperimentStepItem!)
                .OrderByDescending(i => i.StartTime)
                .ToArray();

            if (experimentStepItems.Length != 0
                && experimentStepItems[0].EndTime > now + AFTER_EXPERIMENT_DURATION)
            {
                Console.WriteLine($"Await sub experiments completion");
                await TaskHelper.Until(
                    experimentStepItems[0].EndTime + AFTER_EXPERIMENT_DURATION,
                    ct);

                return true;
            }
            else
            {
                return await ScheduleSubExperimentStepsAsync(experimentStepItems, ct);
            }
        }

        private async Task<bool> ScheduleSubExperimentStepsAsync(
            ExperimentStepItem[] experimentStepItems,
            CancellationToken ct)
        {
            var subExperimentStepItemPairTasks = _config.SubExperiments
                .Select(c => CreateSubExperimentStepItemPairAsync(c, experimentStepItems, ct))
                .ToArray();

            await Task.WhenAll(subExperimentStepItemPairTasks);

            var startTime = DateTime.UtcNow.Add(BEFORE_EXPERIMENT_DURATION);
            var endTime = startTime.Add(_config.SubExperimentDuration);
            var subExperimentStepItemMap = subExperimentStepItemPairTasks
                .Select(t => t.Result)
                .Where(p => p != null)
                .Select(p => p!.Value)
                .ToDictionary();

            if (subExperimentStepItemMap.Count > 0)
            {
                var logItem = LogItem.Create(
                    new ExperimentStepItem(startTime, endTime, subExperimentStepItemMap));
                var totalInstanceCount = 1 + subExperimentStepItemMap.Values.Sum(s => s.NodeCount);

                Console.WriteLine($"Starting experiment step with {totalInstanceCount} nodes");
                await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);
                await _logBlobClient.AppendAsync(logItem, null, ct);
                Console.WriteLine($"Experiment step created");

                return true;
            }
            else
            {
                return false;
            }
        }

        private async Task<KeyValuePair<string, SubExperimentStepItem>?> CreateSubExperimentStepItemPairAsync(
            SubExperimentConfig subExperimentConfig,
            ExperimentStepItem[] experimentStepItems,
            CancellationToken ct)
        {
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
                await Task.CompletedTask;

                var lastThroughputTarget = lastSubExperimentItem.ThroughputTarget;
                var historicalThroughputTargets = experimentStepItems
                    .Select(s => s.SubExperimentStepItemMap[subExperimentConfig.SubExperimentName])
                    .Select(i => i.ThroughputTarget)
                    .Reverse();
                var hasLastSubExperimentSucceeded = true;
                var nextThroughputTarget = new ThroughputPlanner().ComputeNextThroughput(
                    hasLastSubExperimentSucceeded,
                    historicalThroughputTargets,
                    _config.ThroughputPrecision);

                if (nextThroughputTarget != null)
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
            else
            {
                return null;
            }
        }
    }
}