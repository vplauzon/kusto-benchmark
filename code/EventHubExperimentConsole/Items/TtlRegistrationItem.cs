namespace EventHubExperimentConsole.Items
{
    internal record TtlRegistrationItem(
        NodeItem? NodeItem,
        Guid NodeId,
        DateTime ExpirationTime)
    {
        public bool IsExpired => ExpirationTime < DateTime.Now;
    }
}