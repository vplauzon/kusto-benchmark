namespace EventHubExperimentConsole.Configuration
{
    internal record SubExperimentConfig(
        string SubExperimentName,
        string IngestionDbUri,
        string IngestionTable,
        string TimestampColumn,
        string EventHubConnectionString,
        double ThroughputTargetStart);
}