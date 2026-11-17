namespace EventHubExperimentConsole.Configuration
{
    internal record StreamingApiConfig(
        string DataFormat,
        string DataMapping,
        bool IsOutputCompressed = false);
}