using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole
{
    internal static class TaskHelper
    {
        public static async Task Until(DateTime targetUtcDate, CancellationToken ct = default)
        {
            var now = DateTime.UtcNow;

            if (targetUtcDate > now)
            {
                await Task.Delay(targetUtcDate - now, ct);
            }
        }
    }
}