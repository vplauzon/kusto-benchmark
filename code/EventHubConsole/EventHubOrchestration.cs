using Azure.Core;
using Azure.Identity;
using BenchmarkLib;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace EventHubConsole
{
    internal class EventHubOrchestration : IAsyncDisposable
    {
        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(10);

        private readonly ExpressionGenerator _generator;
        private readonly ConcurrentQueue<Task> _queryTaskQueue = new();

        #region Constructors
        private EventHubOrchestration(
            ExpressionGenerator generator)
        {
            _generator = generator;
        }

        public static async Task<EventHubOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken ct)
        {
            var credentials = CredentialFactory.CreateCredentials(options.Authentication);
            var template = options.TemplateText;
            var generator = await ExpressionGenerator.CreateAsync(template, null, ct);

            return new EventHubOrchestration(generator);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken ct)
        {
            await Task.CompletedTask;
        }
    }
}