using SharpYaml.Serialization;

namespace EventHubExperimentConsole.Items
{
    internal record SubExperimentStepItem(
        double AggregateThroughputTarget,
        int NodeCount)
    {
        [YamlIgnore]
        public double NodeThroughputTarget => AggregateThroughputTarget / NodeCount;
    }
}