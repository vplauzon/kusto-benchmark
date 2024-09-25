
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;

namespace IngestorConsole
{
    internal class MainOrchestration : IAsyncDisposable
    {
        private readonly string _sampleText;

        #region Constructors
        private MainOrchestration(string sampleText)
        {
            _sampleText = sampleText;
        }

        public static async Task<MainOrchestration> CreateAsync(
            CommandLineOptions options,
            CancellationToken token)
        {
            var credentials = CreateCredentials(options.Authentication);
            var blobClient = CreateBlobClient(options.Source, credentials);
            var sampleText = await LoadBlobAsync(blobClient);

            return new MainOrchestration(sampleText);
        }

        private static TokenCredential CreateCredentials(string authentication)
        {
            if (string.IsNullOrWhiteSpace(authentication))
            {
                return new DefaultAzureCredential();
            }
            else
            {
                throw new NotImplementedException();
            }
        }

        private static BlobClient CreateBlobClient(string source, TokenCredential credentials)
        {
            var sourceUri = new Uri(source);
            var segments = sourceUri.Segments;
            var containerUri = new Uri($"{sourceUri.Scheme}://{sourceUri.Host}/{segments[1]}");
            var blobName = string.Join("/", segments.Skip(2));
            var containerClient = new BlobContainerClient(containerUri, credentials);
            var blobClient = containerClient.GetBlobClient(blobName);

            return blobClient;
        }

        private static async Task<string> LoadBlobAsync(BlobClient blobClient)
        {
            var download = await blobClient.DownloadAsync();

            using (var reader = new StreamReader(download.Value.Content))
            {
                return await reader.ReadToEndAsync();
            }
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            await Task.CompletedTask;
        }

        public async Task ProcessAsync(CancellationToken token)
        {
            await Task.CompletedTask;
        }
    }
}