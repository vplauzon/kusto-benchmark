using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
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
                    await Task.Delay(_nodeItem.EndTime - now, ct);
                }
            }
        }
    }
}