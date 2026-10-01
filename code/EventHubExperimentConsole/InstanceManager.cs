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
        private const int MAX_INSTANCE_COUNT = 40;

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
        /// <remarks>
        /// Scale settings are revision-scoped:  changing them creates a new revision, which
        /// replaces every replica (including the caller).  The update is therefore skipped when
        /// the count is already set.
        /// </remarks>
        /// <param name="instanceCount">Exact number of instances to run.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns><c>true</c> if the container app was updated.</returns>
        public async Task<bool> SetInstanceCountAsync(int instanceCount, CancellationToken ct)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(instanceCount);
            
            if (instanceCount > MAX_INSTANCE_COUNT)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(instanceCount),
                    $"Instance count cannot exceed {MAX_INSTANCE_COUNT}.");
            }

            var response = await _containerApp.GetAsync(ct);
            var currentScale = response.Value.Data.Template?.Scale;

            if (currentScale?.MinReplicas == instanceCount
                && currentScale?.MaxReplicas == instanceCount)
            {
                Console.WriteLine($"Instance count already set to {instanceCount}");

                return false;
            }

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

            return true;
        }

        /// <summary>
        /// Stops the container app.
        /// </summary>
        /// <param name="ct">Cancellation token.</param>
        public async Task StopAsync(CancellationToken ct)
        {
            await _containerApp.StopAsync(WaitUntil.Completed, ct);
        }
    }
}