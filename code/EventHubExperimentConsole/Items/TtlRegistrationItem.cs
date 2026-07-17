namespace EventHubExperimentConsole.Items
{
    internal record TtlRegistrationItem(
        string? SubExperimentName,
        int SubExperimentNodeIndex,
        Guid NodeId,
        DateTime ExpirationTime);
}