namespace EventHubExperimentConsole
{
    internal class ExperimentOrchestration : IAsyncDisposable
    {
        #region Constructors
        public static async Task<ExperimentOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken token)
        {
            throw new NotImplementedException();
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