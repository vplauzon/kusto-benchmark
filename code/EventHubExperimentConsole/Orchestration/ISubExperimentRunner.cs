namespace EventHubExperimentConsole.Orchestration
{
    internal interface ISubExperimentRunner : IAsyncDisposable
    {
        Task ProcessAsync(CancellationToken ct);
    }
}
