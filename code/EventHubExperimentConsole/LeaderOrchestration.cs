using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal class LeaderOrchestration
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;

        public LeaderOrchestration(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient)
        {
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var startTime = DateTime.Now.AddSeconds(30);
            var endTime = startTime.Add(_config.SubExperimentDuration);
            var subExperimentItems = _config.SubExperiments
                .Select(s => LogItem.Create(new SubExperimentItem(
                    s.SubExperimentName,
                    1,
                    s.ThroughputTargetStart,
                    startTime,
                    endTime)));

            await _logBlobClient.AppendAsync(subExperimentItems, null, ct);
        }
    }
}