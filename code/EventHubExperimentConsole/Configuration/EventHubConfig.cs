using Azure.Core;
using Azure.Storage.Blobs;
using SharpYaml;
using System.Text.Json;

namespace EventHubExperimentConsole.Configuration
{
    internal record EventHubConfig(
        long MaxThroughputPerNode);
}