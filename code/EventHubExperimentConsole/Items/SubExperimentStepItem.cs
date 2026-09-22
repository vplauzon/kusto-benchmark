namespace EventHubExperimentConsole.Items
{
    internal record SubExperimentStepItem(
        string SubExperimentName,
        int NodeCount,
        double ThroughputTarget);
}