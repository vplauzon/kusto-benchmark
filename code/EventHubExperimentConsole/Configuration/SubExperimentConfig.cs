namespace EventHubExperimentConsole.Configuration
{
    internal record SubExperimentConfig(
        string SubExperimentName,
        string IngestionTable,
        string EventHubFqdn,
        string EventHubName,
        double ThroughputTargetStart);
}
