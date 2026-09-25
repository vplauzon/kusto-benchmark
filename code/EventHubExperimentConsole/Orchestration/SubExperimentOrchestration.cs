using EventHubConsole;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
{
    internal class SubExperimentOrchestration
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly NodeItem _nodeItem;

        public SubExperimentOrchestration(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient,
            NodeItem nodeItem)
        {
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
            _nodeItem = nodeItem;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            var delayStart = _nodeItem.StartTime - DateTime.Now;

            if (delayStart > TimeSpan.Zero)
            {
                await Task.Delay(delayStart);
            }
            ct.ThrowIfCancellationRequested();
            if (_nodeItem.EndTime > DateTime.Now)
            {
                var subExperimentConfig = _config.SubExperiments
                    .Where(c => c.SubExperimentName == _nodeItem.SubExperimentName)
                    .First();
                var eventHubOrchestration = await EventHubOrchestration.CreateAsync(
                    ["ExperimentName", "SubExperimentName", "NodeIndex", "ThroughputTarget"],
                    [
                        _experimentName,
                        _nodeItem.SubExperimentName,
                        _nodeItem.SubExperimentNodeIndex.ToString(),
                        _nodeItem.ThroughputTarget.ToString()],
                    "System",
                    new Uri(_config.TemplateDbUri),
                    _config.TemplateName,
                    subExperimentConfig.EventHubConnectionString,
                    string.Empty,
                    string.Empty,
                    _nodeItem.ThroughputTarget,
                    false,
                    _nodeItem.EndTime - DateTime.Now,
                    ct);

                await eventHubOrchestration.ProcessAsync(ct);
            }
        }
    }
}