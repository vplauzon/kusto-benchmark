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
            while (await ProcessStepAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private async Task<bool> ProcessStepAsync(CancellationToken ct)
        {
            var allItems = await _logBlobClient.LoadAllAsync(ct);
            var now = DateTime.Now;
            var activeExperimentStepItems = allItems.Result
                .Where(i => i.ExperimentStepItem != null)
                .Select(i => i.ExperimentStepItem!)
                .OrderByDescending(i => i.StartTime)
                .ToArray();

            if (activeExperimentStepItems.Length != 0
                && activeExperimentStepItems[0].EndTime > now + BEFORE_EXPERIMENT_DURATION)
            {
                Console.WriteLine($"Await sub experiments completion");
                await TaskHelper.Until(
                    activeExperimentStepItems[0].EndTime + BEFORE_EXPERIMENT_DURATION,
                    ct);

                return true;
            }
            else
            {
                var startTime = now.Add(BEFORE_EXPERIMENT_DURATION);
                var endTime = startTime.Add(_config.SubExperimentDuration);
                var newItems = new[]
                {
                    LogItem.Create(new ExperimentStepItem(
                        startTime,
                        endTime,
                        _config.SubExperiments
                            .Select(s => new SubExperimentStepItem(
                                s.SubExperimentName,
                                1,
                                s.ThroughputTargetStart))
                            .ToArray()))
                };
                var totalInstanceCount = 1 + newItems.Sum(i => i.ExperimentStepItem!
                    .SubExperimentStepItems
                    .Sum(s => s.NodeCount));

                Console.WriteLine($"Starting experiment step with {totalInstanceCount} nodes");
                await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);
                await _logBlobClient.AppendAsync(newItems, null, ct);
                Console.WriteLine($"Experiment step created");

                return true;
            }
        }
    }
}