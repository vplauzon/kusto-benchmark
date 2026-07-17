namespace EventHubExperimentConsole
{
    internal class ExperimentOrchestration : IAsyncDisposable
    {
        #region Constructors
        private ExperimentOrchestration()
        {
        }

        public static async Task<ExperimentOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken token)
        {
            //  Load options.Path
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