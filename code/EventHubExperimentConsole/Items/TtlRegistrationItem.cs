using System.Text.Json.Serialization;

namespace EventHubExperimentConsole.Items
{
    internal record TtlRegistrationItem(
        //  Null only for orchestration node ; non-null for experiment node
        NodeItem? NodeItem,
        Guid NodeId,
        DateTime ExpirationTime)
    {
        [JsonIgnore]
        public bool IsExpired => ExpirationTime < DateTime.Now;
    }
}