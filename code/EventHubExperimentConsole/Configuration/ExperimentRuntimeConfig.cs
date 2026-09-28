namespace EventHubExperimentConsole.Configuration
{
    internal record ExperimentRuntimeConfig(
        TimeSpan SubExperimentDuration,
        double MaxThroughputPerNode,
        double ThroughputPrecision,
        int? MaxSubExperimentCount);
}