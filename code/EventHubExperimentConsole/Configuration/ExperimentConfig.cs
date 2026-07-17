using Azure.Core;
using Azure.Storage.Blobs;
using SharpYaml;
using System.Text.Json;

namespace EventHubExperimentConsole.Configuration
{
    internal record ExperimentConfig(
        string TemplateDb,
        string TemplateName,
        IReadOnlyList<SubExperimentConfig> SubExperiments)
    {
        private static readonly YamlSerializerOptions Options = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public static async Task<ExperimentConfig> LoadAsync(
            string blobUri,
            TokenCredential credential,
            CancellationToken token)
        {
            var blobClient = new BlobClient(new Uri(blobUri), credential);
            var response = await blobClient.DownloadStreamingAsync(cancellationToken: token);

            await using (var content = response.Value.Content)
            using (var reader = new StreamReader(content))
            {
                var yaml = await reader.ReadToEndAsync(token);

                return YamlSerializer.Deserialize<ExperimentConfig>(yaml, Options)!;
            }
        }
    }
}