namespace EventHubExperimentConsole.Items
{
    internal record ExperimentStepItem(
        DateTime StartTime,
        DateTime EndTime,
        IReadOnlyList<SubExperimentStepItem> SubExperimentStepItems);
}