namespace EventHubExperimentConsole.Items
{
    internal record TtlRegistrationItem(NodeItem NodeItem, DateTime ExpirationTime)
    {
        public bool IsExpired => ExpirationTime < DateTime.Now;
    }
}