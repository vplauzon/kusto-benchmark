namespace EventHubExperimentConsole.Items
{
    internal record NodeItem(
        string SubExperimentName,
        int SubExperimentNodeIndex,
        DateTime StartTime,
        DateTime EndTime,
        int ThroughputTarget);
}