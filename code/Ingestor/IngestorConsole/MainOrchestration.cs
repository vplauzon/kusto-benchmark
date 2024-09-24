
namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        #region Constructors
        private MainOrchestration()
        {
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken token)
        {
            await Task.CompletedTask;

            return new MainOrchestration();
        }
        #endregion

        ValueTask IAsyncDisposable.DisposeAsync()
        {
            throw new NotImplementedException();
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            await Task.CompletedTask;
        }
    }
}