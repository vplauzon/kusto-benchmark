using EventHubConsole;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;
using Kusto.Data.Common;
using StreamingApiConsole;

namespace EventHubExperimentConsole.Orchestration
{
    internal static class SubExperimentRunnerFactory
    {
        #region Inner Types
        private sealed class EventHubSubExperimentRunner(
            EventHubOrchestration Orchestration) : ISubExperimentRunner
        {
            Task ISubExperimentRunner.ProcessAsync(CancellationToken ct) =>
                Orchestration.ProcessAsync(ct);

            ValueTask IAsyncDisposable.DisposeAsync() =>
                ((IAsyncDisposable)Orchestration).DisposeAsync();
        }

        private sealed class StreamingApiSubExperimentRunner(
            StreamingApiOrchestration Orchestration) : ISubExperimentRunner
        {
            Task ISubExperimentRunner.ProcessAsync(CancellationToken ct) =>
                Orchestration.ProcessAsync(ct);

            ValueTask IAsyncDisposable.DisposeAsync() =>
                ((IAsyncDisposable)Orchestration).DisposeAsync();
        }
        #endregion

        public static async Task<ISubExperimentRunner> CreateAsync(
            string experimentName,
            ExperimentConfig experimentConfig,
            SubExperimentConfig subExperimentConfig,
            NodeItem nodeItem,
            CancellationToken ct)
        {
            var dimensionNames =
                new[] { "ExperimentName", "SubExperimentName", "NodeIndex", "ThroughputTarget" };
            var dimensionValues = new[]
            {
                experimentName,
                nodeItem.SubExperimentName,
                nodeItem.SubExperimentNodeIndex.ToString(),
                nodeItem.AggregateThroughputTarget.ToString()
            };

            if (subExperimentConfig.IsEventHubSubExperiment)
            {
                var orchestration = await EventHubOrchestration.CreateAsync(
                    dimensionNames,
                    dimensionValues,
                    "System",
                    new Uri(experimentConfig.TemplateDbUri),
                    experimentConfig.TemplateName,
                    subExperimentConfig.EventHubConnectionString!,
                    string.Empty,
                    string.Empty,
                    nodeItem.NodeThroughputTarget,
                    false,
                    nodeItem.EndTime,
                    ct);

                return new EventHubSubExperimentRunner(orchestration);
            }

            var streamingConfig = subExperimentConfig.StreamingApi
                ?? throw new InvalidOperationException(
                    $"Sub-experiment '{subExperimentConfig.SubExperimentName}' must configure " +
                    "streamingApi when eventHubConnectionString is absent.");
            if (!Enum.TryParse<DataSourceFormat>(
                    streamingConfig.DataFormat,
                    ignoreCase: true,
                    out var dataFormat)
                || !Enum.IsDefined(dataFormat))
            {
                throw new InvalidOperationException(
                    $"Sub-experiment '{subExperimentConfig.SubExperimentName}' must specify " +
                    "a valid streamingApi.dataFormat.");
            }
            if (streamingConfig.DataMapping is null)
            {
                throw new InvalidOperationException(
                    $"Sub-experiment '{subExperimentConfig.SubExperimentName}' must specify " +
                    "streamingApi.dataMapping (use an empty string for no mapping).");
            }
            var tableUri = new Uri(
                $"{subExperimentConfig.IngestionDbUri.TrimEnd('/')}/" +
                Uri.EscapeDataString(subExperimentConfig.IngestionTable));
            var streamingOrchestration = await StreamingApiOrchestration.CreateAsync(
                dimensionNames,
                dimensionValues,
                "System",
                new Uri(experimentConfig.TemplateDbUri),
                experimentConfig.TemplateName,
                tableUri,
                nodeItem.NodeThroughputTarget,
                dataFormat,
                streamingConfig.DataMapping,
                streamingConfig.IsOutputCompressed,
                nodeItem.EndTime,
                ct);

            return new StreamingApiSubExperimentRunner(streamingOrchestration);
        }
    }
}
