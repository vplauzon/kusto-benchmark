namespace EventHubExperimentConsole.Configuration
{
    internal record SubExperimentConfig(
        string SubExperimentName,
        string IngestionTable,
        string EventHubConnectionString,
        int ThroughputTargetStart,
        int ThroughputPrecision);
}