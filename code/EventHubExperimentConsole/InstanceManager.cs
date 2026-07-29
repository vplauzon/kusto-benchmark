using Azure.Core;
using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole
{
    /// <summary>
    /// Component controlling the number of instances of a specified Azure Container Application.
    /// </summary>
    internal class InstanceManager
    {
        public InstanceManager(
            string containerAppId,
            TokenCredential credential)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Sets the instance count for the application.  When the method returns, the count is set.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task SetInstanceCountAsync(CancellationToken ct)
        {
            throw new NotImplementedException();
        }
    }
}