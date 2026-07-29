using Azure;
using Azure.Core;
using Azure.ResourceManager;
using Azure.ResourceManager.AppContainers;
using Azure.ResourceManager.AppContainers.Models;

namespace EventHubExperimentConsole
{
    /// <summary>
    /// Component controlling the number of instances of a specified Azure Container Application.
    /// </summary>
    internal class InstanceManager
    {
        private readonly ContainerAppResource _containerApp;

        public InstanceManager(
            string containerAppId,
            TokenCredential credential)
        {
            var armClient = new ArmClient(credential);

            _containerApp = armClient.GetContainerAppResource(
                new ResourceIdentifier(containerAppId));
        }

        /// <summary>
        /// Sets the instance count for the application.  When the method returns, the count is set.
        /// </summary>
        /// <param name="instanceCount">Exact number of instances to run.</param>
        /// <param name="ct">Cancellation token.</param>
        public async Task SetInstanceCountAsync(int instanceCount, CancellationToken ct)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(instanceCount);

            var response = await _containerApp.GetAsync(ct);
            var data = new ContainerAppData(response.Value.Data.Location)
            {
                Template = new ContainerAppTemplate
                {
                    Scale = new ContainerAppScale
                    {
                        MinReplicas = instanceCount,
                        MaxReplicas = instanceCount
                    }
                }
            };

            await _containerApp.UpdateAsync(WaitUntil.Completed, data, ct);
        }
    }
}