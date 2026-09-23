using EventHubConsole;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
{
    internal class BenchmarkOrchestration
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly NodeItem _nodeItem;

        public BenchmarkOrchestration(
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
            while (_nodeItem.EndTime > DateTime.Now)
            {
                var now = DateTime.Now;

                ct.ThrowIfCancellationRequested();
                if (_nodeItem.EndTime > now)
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
                        _nodeItem.EndTime - now,
                        ct);

                    await eventHubOrchestration.ProcessAsync(ct);
                }
            }
        }
    }
}