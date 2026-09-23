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

        private readonly LogBlobClient<LogItem> _logBlobClient;
        private readonly Guid _nodeId;
        private readonly TaskCompletionSource _registrationSource = new();
        private readonly Task _backgroundTask;

        #region Constructor
        private RegistrationManager(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            NodeItem? nodeItem,
            CancellationToken ct)
        {
            _logBlobClient = logBlobClient;
            _nodeId = nodeId;
            NodeItem = nodeItem;
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
                            $"{nodeItem.SubExperimentName}:{nodeItem.SubExperimentNodeIndex}");
                    }
                    else
                    {
                        Console.WriteLine($"Node ({nodeId}) registration with leader");
                    }

                    return new RegistrationManager(
                        logBlobClient,
                        nodeId,
                        result.NodeItem,
                        ct);
                }
                else
                {
                    Console.WriteLine($"Node ({nodeId}) registration is delayed");

                    await Task.Delay(AWAIT_REGISTRATION_DELAY, ct);
                }
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem)> TryRegisterAsync(
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

                    return (false, null);
                }
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem)> TryRegisterLeaderAsync(
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
                return (false, null);
            }
            else
            {
                return await TryRegisterNodeAsync(logBlobClient, nodeId, null, logTag, ct);
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem)> TryRegisterNodeAsync(
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
                    DateTime.Now.Add(REGISTRATION_TTL))),
                logTag,
                ct);

            return (success, nodeItem);
        }

        private static async Task<(bool Success, NodeItem? NodeItem)> TryRegisterNonLeaderAsync(
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
            var now = DateTime.Now;
            var experimentStepItems = allItems
                .Where(r => r.ExperimentStepItem != null)
                .Select(r => r.ExperimentStepItem!)
                .Where(s => s.EndTime < now);

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

                            return await TryRegisterNodeAsync(
                                logBlobClient,
                                nodeId,
                                new NodeItem(
                                    subExperimentName,
                                    index,
                                    experimentStepItem.StartTime,
                                    experimentStepItem.EndTime),
                                logTag,
                                ct);
                        }
                    }
                    else
                    {   //  No registration available for that sub experiment:
                        //  let's register the first one
                        return await TryRegisterNodeAsync(
                            logBlobClient,
                            nodeId,
                            new NodeItem(
                                subExperimentName,
                                0,
                                experimentStepItem.StartTime,
                                experimentStepItem.EndTime),
                            logTag,
                            ct);
                    }
                }
            }

            return (false, null);
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
                if (NodeItem == null && lastClean.Add(CLEAN_REGISTRATION_DELAY) < DateTime.Now)
                {
                    await _logBlobClient.CompactAsync(ct);
                    lastClean = DateTime.Now;
                }
                ct.ThrowIfCancellationRequested();
                //  Pause
                await Task.WhenAny(Task.Delay(REGISTRATION_TTL / 2, ct), _registrationSource.Task);
                ct.ThrowIfCancellationRequested();
                if (!_registrationSource.Task.IsCompleted)
                {   //  Update registration
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
                    await _logBlobClient.AppendAsync(
                        LogItem.Create(new TtlRegistrationItem(
                            NodeItem,
                            _nodeId,
                            DateTime.Now.Add(REGISTRATION_TTL))),
                        null,
                        ct);
                }
            }
        }
    }
}