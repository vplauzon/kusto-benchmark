using EventHubExperimentConsole.Items;

namespace EventHubExperimentConsole
{
    internal sealed class RegistrationManager : IAsyncDisposable
    {
        private static readonly TimeSpan REGISTRATION_TTL = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan HEARTBEAT_INTERVAL = TimeSpan.FromSeconds(15);
        private static readonly TimeSpan ASSIGNMENT_POLL_INTERVAL = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan CLEAN_REGISTRATION_DELAY = TimeSpan.FromMinutes(5);

        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly CancellationTokenSource _stopSource = new();
        private readonly Task _backgroundTask;
        private volatile bool _isLeader;
        private NodeItem? _lastAssignment;

        private RegistrationManager(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            bool isLeader)
        {
            _logBlobClient = logBlobClient;
            NodeId = nodeId;
            _isLeader = isLeader;
            _backgroundTask = RunBackgroundAsync(_stopSource.Token);
        }

        public Guid NodeId { get; }

        public bool IsLeader => _isLeader;

        public static async Task<RegistrationManager> RegisterAsync(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            CancellationToken ct)
        {
            await logBlobClient.AppendAsync(
                LogItem.Create(new NodeRegistrationItem(
                    nodeId,
                    DateTime.UtcNow.Add(REGISTRATION_TTL))),
                null,
                ct);

            while (true)
            {
                var items = await logBlobClient.LoadAllAsync(ct);
                var registrations = GetLatestRegistrations(items.Result);
                var hasLiveLeader = items.Result
                    .Where(i => i.NodeAssignationItem != null
                        && i.NodeAssignationItem.NodeItem == null)
                    .Select(i => i.NodeAssignationItem!)
                    .Where(i => registrations.TryGetValue(i.NodeId, out var registration)
                        && !registration.IsExpired)
                    .Any();

                if (!hasLiveLeader)
                {
                    var claimed = await logBlobClient.AppendAsync(
                        LogItem.Create(new NodeAssignationItem(null, nodeId)),
                        items.Tag,
                        ct);
                    if (claimed)
                    {
                        Console.WriteLine($"Node ({nodeId}) registered as leader");
                        return new RegistrationManager(logBlobClient, nodeId, true);
                    }
                }
                else
                {
                    Console.WriteLine($"Node ({nodeId}) registered as available");
                    return new RegistrationManager(logBlobClient, nodeId, false);
                }

                await Task.Delay(ASSIGNMENT_POLL_INTERVAL, ct);
            }
        }

        public async Task<NodeItem?> WaitForAssignmentAsync(CancellationToken ct)
        {
            while (true)
            {
                ct.ThrowIfCancellationRequested();
                var items = await _logBlobClient.LoadAllAsync(ct);
                var assignment = items.Result
                    .Where(i => i.NodeAssignationItem?.NodeId == NodeId)
                    .Select(i => i.NodeAssignationItem!)
                    .LastOrDefault();
                var nodeItem = assignment?.NodeItem;

                if (nodeItem != null
                    && nodeItem.EndTime > DateTime.UtcNow
                    && nodeItem != _lastAssignment)
                {
                    _lastAssignment = nodeItem;
                    Console.WriteLine(
                        $"Node ({NodeId}) assigned to " +
                        $"{nodeItem.SubExperimentName}:{nodeItem.SubExperimentNodeIndex}");
                    return nodeItem;
                }

                if (await TryClaimLeadershipAsync(ct))
                {
                    _isLeader = true;
                    Console.WriteLine($"Node ({NodeId}) took over as leader");
                    return null;
                }

                await Task.Delay(ASSIGNMENT_POLL_INTERVAL, ct);
            }
        }

        public async ValueTask DisposeAsync()
        {
            _stopSource.Cancel();
            try
            {
                await _backgroundTask;
            }
            catch (OperationCanceledException) when (_stopSource.IsCancellationRequested)
            {
            }
            finally
            {
                _stopSource.Dispose();
            }
        }

        private async Task<bool> TryClaimLeadershipAsync(CancellationToken ct)
        {
            var items = await _logBlobClient.LoadAllAsync(ct);
            var registrations = GetLatestRegistrations(items.Result);
            var hasLiveLeader = items.Result
                .Where(i => i.NodeAssignationItem != null
                    && i.NodeAssignationItem.NodeItem == null)
                .Select(i => i.NodeAssignationItem!)
                .Where(i => registrations.TryGetValue(i.NodeId, out var registration)
                    && !registration.IsExpired)
                .Any();
            if (hasLiveLeader)
            {
                return false;
            }

            return await _logBlobClient.AppendAsync(
                LogItem.Create(new NodeAssignationItem(null, NodeId)),
                items.Tag,
                ct);
        }

        private async Task RunBackgroundAsync(CancellationToken ct)
        {
            try
            {
                await RunHeartbeatLoopAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                var message = $"Node ({NodeId}) lost its registration heartbeat: {ex.Message}";
                Console.Error.WriteLine(message);
                Environment.FailFast(message, ex);
            }
        }

        private async Task RunHeartbeatLoopAsync(CancellationToken ct)
        {
            var lastClean = DateTime.UtcNow;
            while (true)
            {
                await Task.Delay(HEARTBEAT_INTERVAL, ct);
                await _logBlobClient.AppendAsync(
                    LogItem.Create(new NodeRegistrationItem(
                        NodeId,
                        DateTime.UtcNow.Add(REGISTRATION_TTL))),
                    null,
                    ct);

                if (IsLeader)
                {
                    var items = await _logBlobClient.LoadAllAsync(ct);
                    var currentLeader = items.Result
                        .Where(i => i.NodeAssignationItem != null
                        && i.NodeAssignationItem.NodeItem == null)
                        .Select(i => i.NodeAssignationItem!)
                        .LastOrDefault();
                    if (currentLeader?.NodeId != NodeId)
                    {
                        Environment.FailFast(
                            $"Leader node ({NodeId}) lost its assignment to " +
                            $"({currentLeader?.NodeId.ToString() ?? "none"}).");
                    }

                    if (lastClean.Add(CLEAN_REGISTRATION_DELAY) < DateTime.UtcNow)
                    {
                        await _logBlobClient.CompactAsync(ct);
                        lastClean = DateTime.UtcNow;
                    }
                }
            }
        }

        private static Dictionary<Guid, NodeRegistrationItem>
            GetLatestRegistrations(IEnumerable<LogItem> items)
        {
            return items
                .Where(i => i.NodeRegistrationItem != null)
                .Select(i => i.NodeRegistrationItem!)
                .GroupBy(i => i.NodeId)
                .ToDictionary(g => g.Key, g => g.Last());
        }
    }
}
