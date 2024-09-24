
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

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            await Task.CompletedTask;
        }
    }
}