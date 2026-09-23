using System.Collections.ObjectModel;

namespace EventHubExperimentConsole.Items
{
    internal record ExperimentStepItem(
        DateTime StartTime,
        DateTime EndTime,
        IReadOnlyDictionary<string, SubExperimentStepItem> SubExperimentStepItemMap);
}