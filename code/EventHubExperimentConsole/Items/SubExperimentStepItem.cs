using System.Text.Json.Serialization;

namespace EventHubExperimentConsole.Items
{
    internal record SubExperimentStepItem(
        double AggregateThroughputTarget,
        int NodeCount)
    {
        [JsonIgnore]
        public double NodeThroughputTarget => AggregateThroughputTarget / NodeCount;
    }
}