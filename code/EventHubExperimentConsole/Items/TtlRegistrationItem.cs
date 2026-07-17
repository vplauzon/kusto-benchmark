namespace EventHubExperimentConsole.Items
{
    internal record TtlRegistrationItem(
        string Role,
        Guid NodeId,
        DateTime ExpirationTime);
}