using EventHubExperimentConsole.Items;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

namespace EventHubExperimentConsole
{
    internal class RegistrationManager : IAsyncDisposable
    {
        private static readonly TimeSpan REGISTRATION_TTL = TimeSpan.FromSeconds(30);
        private static readonly TimeSpan AWAIT_REGISTRATION_DELAY = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan CLEAN_REGISTRATION_DELAY = TimeSpan.FromMinutes(5);
        private static readonly TimeSpan RENEWAL_RETRY_DELAY = TimeSpan.FromMilliseconds(200);
        private const int MAX_RENEWAL_ATTEMPTS = 10;

        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly Guid _nodeId;
        private readonly TaskCompletionSource _registrationSource = new();
        private readonly Task _backgroundTask;
        private string _currentLogTag = string.Empty;

        #region Constructor
        private RegistrationManager(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            NodeItem? nodeItem,
            string initialLogTag,
            CancellationToken ct)
        {
            _logBlobClient = logBlobClient;
            _nodeId = nodeId;
            NodeItem = nodeItem;
            _currentLogTag = initialLogTag;
            _backgroundTask = RunBackgroundAsync(ct);
        }

        #region Register
        public static async Task<RegistrationManager> RegisterAsync(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            CancellationToken ct)
        {
            while (true)
            {
                var result = await TryRegisterAsync(logBlobClient, nodeId, ct);

                if (result.Success)
                {
                    var nodeItem = result.NodeItem;

                    if (nodeItem != null)
                    {
                        Console.WriteLine(
                            $"Node ({nodeId}) registration with " +
                            $"{nodeItem.SubExperimentName}:{nodeItem.SubExperimentNodeIndex}" +
                            $"({nodeItem.ThroughputTarget})");
                    }
                    else
                    {
                        Console.WriteLine($"Node ({nodeId}) registration with leader");
                    }

                    return new RegistrationManager(
                        logBlobClient,
                        nodeId,
                        result.NodeItem,
                        result.LogTag,
                        ct);
                }
                else
                {
                    Console.WriteLine($"Node ({nodeId}) registration is delayed");

                    await Task.Delay(AWAIT_REGISTRATION_DELAY, ct);
                }
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem, string LogTag)> TryRegisterAsync(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            CancellationToken ct)
        {
            var allItemsResult = await logBlobClient.LoadAllAsync(ct);
            var leaderResult = await TryRegisterLeaderAsync(
                logBlobClient,
                allItemsResult.Tag,
                allItemsResult.Result,
                nodeId,
                ct);

            if (leaderResult.Success)
            {
                return leaderResult;
            }
            else
            {
                var nonLeaderResult = await TryRegisterNonLeaderAsync(
                    logBlobClient,
                    allItemsResult.Tag,
                    allItemsResult.Result,
                    nodeId,
                    ct);

                if (nonLeaderResult.Success)
                {
                    return nonLeaderResult;
                }
                else
                {
                    Console.WriteLine($"No registration available for node ({nodeId})");

                    return (false, null, allItemsResult.Tag);
                }
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem, string LogTag)> TryRegisterLeaderAsync(
            LogBlobClient<LogItem> logBlobClient,
            string logTag,
            IImmutableList<LogItem> allItems,
            Guid nodeId,
            CancellationToken ct)
        {
            var ttlRegistrationItems = allItems
                .Where(r => r.TtlRegistrationItem != null)
                .Select(r => r.TtlRegistrationItem!);
            var leaderRegistrationItem = ttlRegistrationItems
                .FirstOrDefault(i => i.NodeItem == null);

            if (leaderRegistrationItem != null && !leaderRegistrationItem.IsExpired)
            {
                return (false, null, logTag);
            }
            else
            {
                var result = await TryRegisterNodeAsync(logBlobClient, nodeId, null, logTag, ct);
                return (result.Success, result.NodeItem, logTag);
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem, string LogTag)> TryRegisterNodeAsync(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            NodeItem? nodeItem,
            string logTag,
            CancellationToken ct)
        {
            var success = await logBlobClient.AppendAsync(
                LogItem.Create(new TtlRegistrationItem(
                    nodeItem,
                    nodeId,
                    DateTime.UtcNow.Add(REGISTRATION_TTL))),
                logTag,
                ct);

            return (success, nodeItem, logTag);
        }

        private static async Task<(bool Success, NodeItem? NodeItem, string LogTag)> TryRegisterNonLeaderAsync(
            LogBlobClient<LogItem> logBlobClient,
            string logTag,
            IImmutableList<LogItem> allItems,
            Guid nodeId,
            CancellationToken ct)
        {
            var ttlRegistrationItemGroups = allItems
                .Where(r => r.TtlRegistrationItem?.NodeItem != null)
                .Select(r => r.TtlRegistrationItem!)
                .GroupBy(t => t.NodeItem!.SubExperimentName)
                .ToDictionary(g => g.Key, g => g.Where(t => !t.IsExpired).ToArray());
            var now = DateTime.UtcNow;
            var experimentStepItems = allItems
                .Where(r => r.ExperimentStepItem != null)
                .Select(r => r.ExperimentStepItem!)
                //  Only active or upcoming steps can be registered against
                .Where(s => s.EndTime > now)
                .OrderByDescending(s => s.StartTime);

            foreach (var experimentStepItem in experimentStepItems)
            {
                foreach (var pair in experimentStepItem.SubExperimentStepItemMap)
                {
                    var subExperimentName = pair.Key;
                    var subExperimentStepItem = pair.Value;

                    if (ttlRegistrationItemGroups.TryGetValue(
                        subExperimentName,
                        out var registrationItems))
                    {   //  Some registration present
                        //  Let's find the first one available
                        var takenIndexes = registrationItems
                            .Select(i => i.NodeItem!.SubExperimentNodeIndex);
                        //  We take-1 because first-or-default would return 0 if none is available
                        var indexAvailable = Enumerable.Range(0, subExperimentStepItem.NodeCount)
                            .Except(takenIndexes)
                            .Take(1)
                            .ToArray();

                        if (indexAvailable.Length == 1)
                        {   //  One index is available
                            var index = indexAvailable[0];

                            var result = await TryRegisterNodeAsync(
                                logBlobClient,
                                nodeId,
                                new NodeItem(
                                    subExperimentName,
                                    index,
                                    experimentStepItem.StartTime,
                                    experimentStepItem.EndTime,
                                    subExperimentStepItem.ThroughputTarget),
                                logTag,
                                ct);
                            return (result.Success, result.NodeItem, logTag);
                        }
                    }
                    else
                    {   //  No registration available for that sub experiment:
                        //  let's register the first one
                        var result = await TryRegisterNodeAsync(
                            logBlobClient,
                            nodeId,
                            new NodeItem(
                                subExperimentName,
                                0,
                                experimentStepItem.StartTime,
                                experimentStepItem.EndTime,
                                subExperimentStepItem.ThroughputTarget),
                            logTag,
                            ct);
                        return (result.Success, result.NodeItem, logTag);
                    }
                }
            }

            return (false, null, logTag);
        }
        #endregion
        #endregion

        public NodeItem? NodeItem { get; }

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _registrationSource.TrySetResult();
            await _backgroundTask;
        }

        private async Task RunBackgroundAsync(CancellationToken ct)
        {
            var lastClean = DateTime.MinValue;

            while (!_registrationSource.Task.IsCompleted)
            {
                if (NodeItem == null && lastClean.Add(CLEAN_REGISTRATION_DELAY) < DateTime.UtcNow)
                {   //  Compaction rewrites the blob:  the cached tag is stale afterwards
                    await _logBlobClient.CompactAsync(ct);
                    lastClean = DateTime.UtcNow;
                }
                ct.ThrowIfCancellationRequested();
                //  Pause
                await Task.WhenAny(Task.Delay(REGISTRATION_TTL / 2, ct), _registrationSource.Task);
                ct.ThrowIfCancellationRequested();
                if (!_registrationSource.Task.IsCompleted)
                {   //  Update registration
                    await RenewRegistrationAsync(ct);
                }
            }
        }

        /// <summary>
        /// Renews the registration of this node.
        /// An e-tag mismatch simply means another node appended to the log (another node
        /// renewing its own registration, a new experiment step, a compaction, etc.):  it is
        /// not a sign of split brain, so the renewal is retried against the current state.
        /// Split brain is detected by inspecting the log itself:  another node holding a
        /// non-expired registration on the same slot.
        /// </summary>
        private async Task RenewRegistrationAsync(CancellationToken ct)
        {
            for (var attempt = 0; attempt != MAX_RENEWAL_ATTEMPTS; ++attempt)
            {
                var allItems = await _logBlobClient.LoadAllAsync(ct);

                _currentLogTag = allItems.Tag;
                DetectSlotTakeOver(allItems.Result);

                var renewalSucceeded = await _logBlobClient.AppendAsync(
                    LogItem.Create(new TtlRegistrationItem(
                        NodeItem,
                        _nodeId,
                        DateTime.UtcNow.Add(REGISTRATION_TTL))),
                    _currentLogTag,
                    ct);

                if (renewalSucceeded)
                {
                    if (NodeItem != null)
                    {
                        Console.WriteLine(
                            $"Node ({_nodeId}) renewed registration with " +
                            $"{NodeItem.SubExperimentName}:{NodeItem.SubExperimentNodeIndex}");
                    }
                    else
                    {
                        Console.WriteLine($"Node ({_nodeId}) renewed registration with leader");
                    }

                    return;
                }
                //  Concurrent append:  reload and retry against the new state
                await Task.Delay(RENEWAL_RETRY_DELAY, ct);
            }

            throw new InvalidOperationException(
                $"Node ({_nodeId}) couldn't renew its registration after " +
                $"{MAX_RENEWAL_ATTEMPTS} attempts");
        }

        private void DetectSlotTakeOver(IImmutableList<LogItem> allItems)
        {
            var conflictingItem = allItems
                .Where(i => i.TtlRegistrationItem != null)
                .Select(i => i.TtlRegistrationItem!)
                .Where(i => !i.IsExpired)
                .Where(i => i.NodeId != _nodeId)
                .FirstOrDefault(i => IsSameSlot(i.NodeItem, NodeItem));

            if (conflictingItem != null)
            {
                if (NodeItem == null)
                {
                    var message =
                        $"FATAL: Node ({_nodeId}) lost leader lease due to split brain " +
                        $"condition.  Node ({conflictingItem.NodeId}) has taken over the " +
                        $"leader role.  Crashing the process.";

                    Console.Error.WriteLine(message);
                    Environment.FailFast(message);
                }
                else
                {
                    Console.Error.WriteLine(
                        $"WARNING:  Node ({_nodeId}) registration on " +
                        $"{NodeItem.SubExperimentName}:{NodeItem.SubExperimentNodeIndex} " +
                        $"is also held by node ({conflictingItem.NodeId})");
                }
            }
        }

        private static bool IsSameSlot(NodeItem? left, NodeItem? right)
        {
            if (left == null || right == null)
            {
                return left == null && right == null;
            }
            else
            {
                return left.SubExperimentName == right.SubExperimentName
                    && left.SubExperimentNodeIndex == right.SubExperimentNodeIndex;
            }
        }
    }
}