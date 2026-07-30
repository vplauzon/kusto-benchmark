using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal class NonLeaderOrchestration
    {
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;

        public NonLeaderOrchestration(
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
            while (await ProcessStepAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
            }
        }

        private async Task<bool> ProcessStepAsync(CancellationToken ct)
        {
            await Task.CompletedTask;

            return false;
        }
    }
}