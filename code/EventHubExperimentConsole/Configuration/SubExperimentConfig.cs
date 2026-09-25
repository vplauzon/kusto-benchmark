namespace EventHubExperimentConsole.Configuration
{
    internal record SubExperimentConfig(
        string SubExperimentName,
        Uri IngestionDbUri,
        string IngestionTable,
        string EventHubConnectionString,
        int ThroughputTargetStart);
}