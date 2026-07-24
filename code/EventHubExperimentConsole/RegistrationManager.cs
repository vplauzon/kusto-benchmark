using EventHubExperimentConsole.Items;
using System;
using System.Collections.Generic;
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
                    await Task.Delay(AWAIT_REGISTRATION_DELAY, ct);
                }
            }
        }

        private static async Task<(bool Success, NodeItem? NodeItem)> TryRegisterAsync(
            LogBlobClient<LogItem> logBlobClient,
            Guid nodeId,
            CancellationToken ct)
        {
            var result = await logBlobClient.LoadAllAsync(ct);
            var ttlRegistrationItems = result.Result
                .Where(r => r.TtlRegistrationItem != null)
                .Select(r => r.TtlRegistrationItem!);
            var leaderRegistrationItem = ttlRegistrationItems
                .FirstOrDefault(i => i.NodeItem == null);

            if (leaderRegistrationItem != null && !leaderRegistrationItem.IsExpired)
            {
                //  Look at more than leader
                //  TODO
            }
            else
            {
                var success = await logBlobClient.AppendAsync(
                    LogItem.Create(new TtlRegistrationItem(
                        null,
                        nodeId,
                        DateTime.Now.Add(REGISTRATION_TTL))),
                    result.Tag,
                    ct);

                return (success, null);
            }

            Console.WriteLine($"No registration available for node ({nodeId})");

            return (false, null);
        }
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

            while (!ct.IsCancellationRequested && !_registrationSource.Task.IsCompleted)
            {
                if (lastClean.Add(CLEAN_REGISTRATION_DELAY) < DateTime.Now)
                {
                    await _logBlobClient.CompactAsync(ct);
                    lastClean = DateTime.Now;
                }
                ct.ThrowIfCancellationRequested();
                //  Update registration
                await _logBlobClient.AppendAsync(
                    LogItem.Create(new TtlRegistrationItem(
                        NodeItem,
                        _nodeId,
                        DateTime.Now.Add(REGISTRATION_TTL))),
                    null,
                    ct);
                ct.ThrowIfCancellationRequested();
                //  Pause
                await Task.Delay(REGISTRATION_TTL / 2, ct);
            }
        }
    }
}