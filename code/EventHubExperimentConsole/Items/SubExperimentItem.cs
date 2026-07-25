namespace EventHubExperimentConsole.Items
{
    internal record SubExperimentItem(
        string SubExperimentName,
        int NodeCount,
        double ThroughputTarget,
        DateTime StartTime,
        DateTime EndTime);
}