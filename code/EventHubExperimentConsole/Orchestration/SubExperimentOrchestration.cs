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
            var delayStart = _nodeItem.StartTime - DateTime.UtcNow;

            if (delayStart > TimeSpan.Zero)
            {
                await Task.Delay(delayStart, ct);
            }
            ct.ThrowIfCancellationRequested();
            if (_nodeItem.EndTime > DateTime.UtcNow)
            {
                var subExperimentConfig = _config.SubExperiments
                    .Where(c => c.SubExperimentName == _nodeItem.SubExperimentName)
                    .First();
                await using var runner = await SubExperimentRunnerFactory.CreateAsync(
                    _experimentName,
                    _config,
                    subExperimentConfig,
                    _nodeItem,
                    ct);

                await runner.ProcessAsync(ct);
                Console.WriteLine("Completed sub experiment");
            }
        }
    }
}