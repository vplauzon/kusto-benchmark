using BenchmarkLib;
using EventHubExperimentConsole.Configuration;

namespace EventHubExperimentConsole
{
    internal class ExperimentOrchestration : IAsyncDisposable
    {
        private readonly ExperimentConfig _config;

        #region Constructors
        private ExperimentOrchestration(ExperimentConfig config)
        {
            _config = config;
        }

        public static async Task<ExperimentOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken token)
        {
            var credential = await CredentialFactory.CreateCredentialsAsync(options.Authentication);
            var config = await ExperimentConfig.LoadAsync(
                options.ConfigUri,
                credential,
                token);

            return new ExperimentOrchestration(config);
        }
        #endregion

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            throw new NotImplementedException();
        }
    }
}