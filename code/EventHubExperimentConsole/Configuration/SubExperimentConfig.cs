namespace EventHubExperimentConsole.Configuration
{
    internal record SubExperimentConfig(
        string SubExperimentName,
        string IngestionDbUri,
        string IngestionTable,
        string TimestampColumn,
        double ThroughputTargetStart,
        string? EventHubConnectionString = null,
        StreamingApiConfig? StreamingApi = null);
}
