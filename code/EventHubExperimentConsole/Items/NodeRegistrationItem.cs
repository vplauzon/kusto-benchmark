using System.Text.Json.Serialization;

namespace EventHubExperimentConsole.Items
{
    internal record NodeRegistrationItem(Guid NodeId, DateTime ExpirationTime)
    {
        [JsonIgnore]
        public bool IsExpired => ExpirationTime < DateTime.UtcNow;
    }
}