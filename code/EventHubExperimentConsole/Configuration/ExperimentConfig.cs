using System.Collections.Immutable;

namespace EventHubExperimentConsole.Configuration
{
    internal record ExperimentConfig(
        string ExperimentName,
        string TemplateDb,
        string TemplateName,
        IImmutableList<SubExperimentConfig> SubExperiments);
}
