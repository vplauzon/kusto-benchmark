namespace EventHubExperimentConsole.Items
{
    internal record NodeItem(
        string? SubExperimentName,
        int SubExperimentNodeIndex,
        Guid NodeId);
}