using Azure.Core;
using EventHubExperimentConsole.Configuration;
using EventHubExperimentConsole.Items;
using System.Text.Json;

namespace EventHubExperimentConsole.Orchestration
{
    internal class LeaderOrchestration
    {
        private readonly static TimeSpan BEFORE_EXPERIMENT_DURATION = TimeSpan.FromSeconds(30);

        private readonly static TimeSpan AFTER_EXPERIMENT_DURATION = TimeSpan.FromSeconds(30);

        private readonly static TimeSpan DRAINING_INGESTION_DURATION = TimeSpan.FromSeconds(40);

        private readonly Guid _leaderNodeId;
        private readonly string _experimentName;
        private readonly ExperimentConfig _config;
        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly InstanceManager _instanceManager;
        private readonly IReadOnlyDictionary<string, KustoClient> _kustoClients;

        #region Constructors
        private LeaderOrchestration(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient,
            InstanceManager instanceManager,
            IReadOnlyDictionary<string, KustoClient> kustoClients,
            Guid leaderNodeId)
        {
            _leaderNodeId = leaderNodeId;
            _experimentName = experimentName;
            _config = config;
            _logBlobClient = logBlobClient;
            _instanceManager = instanceManager;
            _kustoClients = kustoClients;
        }

        public static async Task<LeaderOrchestration> CreateAsync(
            string experimentName,
            ExperimentConfig config,
            LogBlobClient<LogItem> logBlobClient,
            InstanceManager instanceManager,
            TokenCredential credential,
            Guid leaderNodeId)
        {
            var kustoClientPairs = await Task.WhenAll(
                config.SubExperiments.Select(async subExperiment =>
                    KeyValuePair.Create(
                        subExperiment.SubExperimentName,
                        await KustoClient.CreateAsync(
                            new Uri(subExperiment.IngestionDbUri),
                            subExperiment.IngestionTable,
                            subExperiment.TimestampColumn,
                            credential))));

            return new LeaderOrchestration(
                experimentName,
                config,
                logBlobClient,
                instanceManager,
                kustoClientPairs.ToDictionary(),
                leaderNodeId);
        }
        #endregion

        public async Task ProcessAsync(CancellationToken ct)
        {
            using var assignmentCancellation = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var assignmentTask = NodeAssignmentCoordinator.MonitorAsync(
                _leaderNodeId,
                _logBlobClient,
                assignmentCancellation.Token);

            try
            {
                while (await ProcessStepAsync(ct))
                {
                    ct.ThrowIfCancellationRequested();
                }

                assignmentCancellation.Cancel();
                try
                {
                    await assignmentTask;
                }
                catch (OperationCanceledException)
                    when (assignmentCancellation.IsCancellationRequested)
                {
                }
                Console.WriteLine(
                    "Experiment complete; scaling down to one instance and stopping " +
                    "the container app.");
                await _instanceManager.SetInstanceCountAsync(1, ct);
                await _instanceManager.StopAsync(ct);
            }
            finally
            {
                assignmentCancellation.Cancel();
                try
                {
                    await assignmentTask;
                }
                catch (OperationCanceledException)
                    when (assignmentCancellation.IsCancellationRequested)
                {
                }
            }
        }

        private async Task<bool> ProcessStepAsync(CancellationToken ct)
        {
            var allItems = await _logBlobClient.LoadAllAsync(ct);
            var now = DateTime.UtcNow;
            var experimentStepItems = allItems.Result
                .Where(i => i.ExperimentStepItem != null)
                .Select(i => i.ExperimentStepItem!)
                .OrderByDescending(i => i.StartTime)
                .ToArray();

            var latestStep = experimentStepItems.FirstOrDefault();
            if (latestStep != null)
            {
                if (latestStep.EndTime > now)
                {
                    var totalInstanceCount = GetInstanceCount(latestStep.SubExperimentStepItemMap);
                    Console.WriteLine(
                        $"Ensuring {totalInstanceCount} replicas for step " +
                        $"'{latestStep.StartTime:O}'");
                    await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);
                }

                var evaluationTime = latestStep.EndTime + AFTER_EXPERIMENT_DURATION;
                if (evaluationTime > DateTime.UtcNow)
                {
                    Console.WriteLine($"Awaiting step completion and grace period until " +
                        $"{evaluationTime:O}");
                    await TaskHelper.Until(evaluationTime, ct);

                    return true;
                }
            }

            return await ScheduleSubExperimentStepsAsync(experimentStepItems, ct);
        }

        private async Task<bool> ScheduleSubExperimentStepsAsync(
            ExperimentStepItem[] experimentStepItems,
            CancellationToken ct)
        {
            var subExperimentStepItemPairTasks = _config.SubExperiments
                .Select(c => CreateSubExperimentStepItemPairAsync(c, experimentStepItems, ct))
                .ToArray();

            await Task.WhenAll(subExperimentStepItemPairTasks);

            var subExperimentStepItemMap = subExperimentStepItemPairTasks
                .Select(t => t.Result)
                .Where(p => p != null)
                .Select(p => p!.Value)
                .ToDictionary();

            if (subExperimentStepItemMap.Count > 0)
            {
                var totalInstanceCount = GetInstanceCount(subExperimentStepItemMap);

                Console.WriteLine($"Starting experiment step with {totalInstanceCount} nodes");
                await _instanceManager.SetInstanceCountAsync(totalInstanceCount, ct);

                //  Scaling can take minutes:  the step window starts once it is done
                var startTime = DateTime.UtcNow.Add(BEFORE_EXPERIMENT_DURATION);
                var endTime = startTime.Add(_config.Runtime.SubExperimentDuration);
                var logItem = LogItem.Create(
                    new ExperimentStepItem(startTime, endTime, subExperimentStepItemMap));

                await _logBlobClient.AppendAsync(logItem, null, ct);
                Console.WriteLine($"Experiment step created");

                return true;
            }
            else
            {
                return false;
            }
        }

        private static int GetInstanceCount(
            IReadOnlyDictionary<string, SubExperimentStepItem> subExperimentSteps)
        {
            return 1 + subExperimentSteps.Values.Sum(step => step.NodeCount);
        }

        private async Task<KeyValuePair<string, SubExperimentStepItem>?>
            CreateSubExperimentStepItemPairAsync(
                SubExperimentConfig subExperimentConfig,
                ExperimentStepItem[] experimentStepItems,
                CancellationToken ct)
        {
            var subName = subExperimentConfig.SubExperimentName;

            if (experimentStepItems.Length == 0)
            {
                return KeyValuePair.Create(
                    subName,
                    CreateSubExperimentStepItem(subExperimentConfig.ThroughputTargetStart));
            }
            else if (experimentStepItems[0].SubExperimentStepItemMap.ContainsKey(
                subName))
            {
                await DrainIngestionAsync(_kustoClients[subName], ct);

                var historicalThroughputTargets = experimentStepItems
                    .Select(s => s.SubExperimentStepItemMap[subName])
                    .Select(i => i.AggregateThroughputTarget)
                    .Reverse();
                var lastStepItem = experimentStepItems[0];
                var lastStepSubItem = lastStepItem.SubExperimentStepItemMap[subName];
                var streamingFailureCount =
                    await _kustoClients[subName].FetchStreamingFailureCountAsync(
                        lastStepItem.StartTime,
                        lastStepItem.EndTime,
                        ct);
                var latencyFailureCount = subExperimentConfig.IsEventHubSubExperiment
                    ? await _kustoClients[subName].FetchLatencyFailureCountAsync(
                        lastStepItem.StartTime,
                        lastStepItem.EndTime,
                        ct)
                    : 0L;
                var clusterName = new Uri(subExperimentConfig.IngestionDbUri).Host.Split('.')[0];

                Console.WriteLine(
                    $"#success#  Experiment='{_experimentName}', SubExperiment='{subName}', " +
                    $"Cluster='{clusterName}', " +
                    $"AggregateThroughputTarget={lastStepSubItem.AggregateThroughputTarget}, " +
                    $"Start='{lastStepItem.StartTime:O}', End='{lastStepItem.EndTime:O}', " +
                    $"StreamingFailureCount={streamingFailureCount}, " +
                    $"LatencyFailureCount={latencyFailureCount}");

                var hasLastSubExperimentSucceeded =
                    (streamingFailureCount == 0) && (latencyFailureCount == 0);
                var nextThroughputTarget = new ThroughputPlanner().ComputeNextThroughput(
                    hasLastSubExperimentSucceeded,
                    historicalThroughputTargets,
                    _config.Runtime.ThroughputPrecision);

                if (nextThroughputTarget != null)
                {
                    return KeyValuePair.Create(
                        subName,
                        CreateSubExperimentStepItem(nextThroughputTarget.Value));
                }
                else
                {
                    return null;
                }
            }
            else
            {
                return null;
            }
        }

        private async Task DrainIngestionAsync(KustoClient kustoClient, CancellationToken ct)
        {
            var rowCount = await kustoClient.FetchRowCountAsync(ct);

            while (true)
            {
                await Task.Delay(DRAINING_INGESTION_DURATION, ct);

                var newRowCount = await kustoClient.FetchRowCountAsync(ct);

                if (newRowCount == rowCount)
                {
                    break;
                }
                rowCount = newRowCount;
            }
        }

        private SubExperimentStepItem CreateSubExperimentStepItem(double aggregateThroughputTarget)
        {
            if (!double.IsFinite(aggregateThroughputTarget) || aggregateThroughputTarget <= 0)
            {
                throw new InvalidOperationException(
                    "Aggregate throughput target must be finite and greater than zero.");
            }

            if (!double.IsFinite(_config.Runtime.MaxThroughputPerNode)
                || _config.Runtime.MaxThroughputPerNode <= 0)
            {
                throw new InvalidOperationException(
                    "Max throughput per node must be greater than zero.");
            }

            var nodeCount = checked((int)Math.Ceiling(
                aggregateThroughputTarget / _config.Runtime.MaxThroughputPerNode));

            return new SubExperimentStepItem(aggregateThroughputTarget, nodeCount);
        }
    }
}