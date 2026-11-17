using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole.Orchestration
{
    internal static class NodeAssignmentCoordinator
    {
        private static readonly TimeSpan CHECK_INTERVAL = TimeSpan.FromSeconds(5);

        public static async Task MonitorAsync(
            Guid leaderNodeId,
            LogBlobClient<LogItem> logBlobClient,
            CancellationToken ct)
        {
            while (true)
            {
                await AssignAvailableNodesAsync(leaderNodeId, logBlobClient, ct);
                await Task.Delay(CHECK_INTERVAL, ct);
            }
        }

        private static async Task AssignAvailableNodesAsync(
            Guid leaderNodeId,
            LogBlobClient<LogItem> logBlobClient,
            CancellationToken ct)
        {
            var items = await logBlobClient.LoadAllAsync(ct);
            var latestRegistrations = items.Result
                .Where(i => i.NodeRegistrationItem != null)
                .Select(i => i.NodeRegistrationItem!)
                .GroupBy(i => i.NodeId)
                .ToDictionary(g => g.Key, g => g.Last());
            if (!latestRegistrations.TryGetValue(leaderNodeId, out var leaderRegistration)
                || leaderRegistration.IsExpired)
            {
                throw new InvalidOperationException(
                    $"Leader node ({leaderNodeId}) has no active node registration.");
            }

            var latestAssignments = items.Result
                .Where(i => i.NodeAssignationItem != null)
                .Select(i => i.NodeAssignationItem!)
                .GroupBy(i => i.NodeId)
                .ToDictionary(g => g.Key, g => g.Last());
            var currentLeaderId = items.Result
                .Where(i => i.NodeAssignationItem != null
                    && i.NodeAssignationItem.NodeItem == null)
                .Select(i => i.NodeAssignationItem!.NodeId)
                .LastOrDefault();
            if (currentLeaderId != leaderNodeId)
            {
                throw new InvalidOperationException(
                    $"Node ({leaderNodeId}) no longer holds the leader assignment.");
            }

            var step = items.Result
                .Where(i => i.ExperimentStepItem != null)
                .Select(i => i.ExperimentStepItem!)
                .OrderByDescending(i => i.StartTime)
                .FirstOrDefault();
            if (step == null || step.EndTime <= DateTime.UtcNow)
            {
                return;
            }

            var slotAssignments = latestAssignments.Values
                .Where(a => a.NodeItem != null)
                .Where(a => latestRegistrations.TryGetValue(a.NodeId, out var registration)
                    && !registration.IsExpired)
                .Where(a => IsSameStep(a.NodeItem!, step))
                .Select(a => (a.NodeItem!.SubExperimentName, a.NodeItem.SubExperimentNodeIndex))
                .ToHashSet();
            var assignedNodeIds = latestAssignments.Values
                .Where(a => a.NodeItem != null && IsSameStep(a.NodeItem, step))
                .Select(a => a.NodeId)
                .ToHashSet();
            var availableNodeIds = new Queue<Guid>(latestRegistrations
                .Where(pair => pair.Key != leaderNodeId && !pair.Value.IsExpired)
                .Select(pair => pair.Key)
                .Where(nodeId => !assignedNodeIds.Contains(nodeId)));
            var newAssignments = new List<LogItem>();

            foreach (var subExperiment in step.SubExperimentStepItemMap)
            {
                for (var index = 0; index < subExperiment.Value.NodeCount; ++index)
                {
                    if (slotAssignments.Contains((subExperiment.Key, index)))
                    {
                        continue;
                    }
                    if (availableNodeIds.Count == 0)
                    {
                        break;
                    }
                    var nodeId = availableNodeIds.Dequeue();

                    var nodeItem = new NodeItem(
                        subExperiment.Key,
                        index,
                        step.StartTime,
                        step.EndTime,
                        subExperiment.Value.NodeThroughputTarget,
                        subExperiment.Value.AggregateThroughputTarget);
                    newAssignments.Add(LogItem.Create(new NodeAssignationItem(nodeItem, nodeId)));
                    slotAssignments.Add((subExperiment.Key, index));
                }
            }

            if (newAssignments.Count > 0)
            {
                var appended = await logBlobClient.AppendAsync(newAssignments, items.Tag, ct);
                if (appended)
                {
                    Console.WriteLine(
                        $"Assigned {newAssignments.Count} nodes to the active experiment step");
                }
            }
        }

        private static bool IsSameStep(NodeItem nodeItem, ExperimentStepItem step)
        {
            return nodeItem.StartTime == step.StartTime && nodeItem.EndTime == step.EndTime;
        }
    }
}
