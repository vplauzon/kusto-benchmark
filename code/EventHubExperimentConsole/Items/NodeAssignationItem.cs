using System.Text.Json.Serialization;

namespace EventHubExperimentConsole.Items
{
    internal record NodeAssignationItem(
        //  Null only for leader node ; non-null for experiment node
        NodeItem? NodeItem,
        Guid NodeId);
}