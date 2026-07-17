using Azure;
using Azure.Core;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Azure.Storage.Files.DataLake.Specialized;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;
using System.Text.Json;

namespace EventHubExperimentConsole
{
    /// <summary>
    /// Wrapper for an append-blob.
    /// The blob content is JSON where each line is a JSON document which can be
    /// serialized to <see cref="T"/>.  Typically those JSON documents can be compacted as
    /// new versions are added.
    /// The interaction with the blob are done with the blob's tag to ensure a level of atomicity.
    /// The blob will be access by different processes running in different containers:
    /// we can't use in-memory locks, this is why we rely on tags.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal class LogBlobClient<T> : IDisposable
    {
        private static readonly JsonSerializerOptions JsonSerializerOptions = new();

        private readonly AppendBlobClient _appendBlobClient;
        private readonly DataLakeFileClient _dataLakeFileClient;
        private readonly Func<IEnumerable<T>, IEnumerable<T>> _compactFunc;

        /// <summary>Construct an instance pointing to a blob path.</summary>
        /// <param name="blobUri">Complete blob URI pointing to the log blob.</param>
        /// <param name="compactFunc">Function which compacts a list of deserialized lines</param>
        private LogBlobClient(
            AppendBlobClient appendBlobClient,
            DataLakeFileClient dataLakeFileClient,
            Func<IEnumerable<T>, IEnumerable<T>> compactFunc)
        {
            _appendBlobClient = appendBlobClient;
            _dataLakeFileClient = dataLakeFileClient;
            _compactFunc = compactFunc;
        }

        /// <summary>
        /// Create an instance pointing to a blob path and ensure the append blob exists.
        /// </summary>
        /// <param name="blobUri">Complete blob URI pointing to the log blob.</param>
        /// <param name="compactFunc">Function which compacts a list of deserialized lines.</param>
        /// <param name="credential">Credential used to access storage.</param>
        /// <param name="ct"></param>
        public static async Task<LogBlobClient<T>> CreateAsync(
            Uri blobUri,
            Func<IEnumerable<T>, IEnumerable<T>> compactFunc,
            TokenCredential credential,
            CancellationToken ct)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(blobUri.ToString());
            ArgumentNullException.ThrowIfNull(compactFunc);
            ArgumentNullException.ThrowIfNull(credential);

            var blobClientUri = CreateBlobEndpointUri(blobUri);
            var dataLakeClientUri = CreateDataLakeEndpointUri(blobUri);

            var appendBlobClient = new AppendBlobClient(blobClientUri, credential);
            var dataLakeFileClient = new DataLakeFileClient(dataLakeClientUri, credential);

            await appendBlobClient.CreateIfNotExistsAsync(cancellationToken: ct);

            return new LogBlobClient<T>(
                appendBlobClient,
                dataLakeFileClient,
                compactFunc);
        }

        void IDisposable.Dispose()
        {
            //  Azure storage clients do not own disposable resources.
        }

        /// <summary>
        /// Load the entire blob, deserialize each line, compact them and return the
        /// compacted list.  It also returns the blob version's tag.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task<TaggedResult<IImmutableList<T>>> LoadAllAsync(CancellationToken ct)
        {
            var response = await _appendBlobClient.DownloadStreamingAsync(
                cancellationToken: ct);

            var items = new List<T>();

            await using (var content = response.Value.Content)
            using (var reader = new StreamReader(
                content,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                leaveOpen: false))
            {
                while (true)
                {
                    var line = await reader.ReadLineAsync(ct);

                    if (line == null)
                    {
                        break;
                    }

                    if (string.IsNullOrWhiteSpace(line))
                    {
                        continue;
                    }

                    items.Add(JsonSerializer.Deserialize<T>(line, JsonSerializerOptions)!);
                }
            }

            var compacted = _compactFunc(items).ToImmutableList();

            return new TaggedResult<IImmutableList<T>>(
                compacted,
                response.Value.Details.ETag.ToString());
        }

        /// <summary>
        /// Compact the content of the blob.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public async Task CompactAsync(CancellationToken ct)
        {
            while (!await TryCompactOnceAsync(ct))
            {
                ct.ThrowIfCancellationRequested();
                await Task.Delay(TimeSpan.FromMilliseconds(100), ct);
            }
        }

        /// <summary>Append a single document.</summary>
        /// <param name="document"></param>
        /// <param name="tag">
        /// Optional tag:  if provided, the append only happends if tag hasn't changed.
        /// </param>
        /// <param name="ct"></param>
        /// <returns>
        /// True if append succeeds.  False if the optional tag does not match the
        /// current blob version.
        /// </returns>
        public Task<bool> AppendAsync(
            T document,
            string? tag,
            CancellationToken ct)
        {
            return AppendAsync([document], tag, ct);
        }

        /// <summary>Append a multiple documents.</summary>
        /// <param name="documents"></param>
        /// <param name="tag"></param>
        /// <param name="ct"></param>
        /// <returns>
        /// True if append succeeds.  False if the optional tag does not match the
        /// current blob version.
        /// </returns>
        public Task<bool> AppendAsync(
            IEnumerable<T> documents,
            string? tag,
            CancellationToken ct)
        {
            ArgumentNullException.ThrowIfNull(documents);

            return AppendDocumentsInternalAsync(
                _appendBlobClient,
                documents,
                tag,
                ct);
        }

        private static Uri CreateBlobEndpointUri(Uri blobUri)
        {
            var uriBuilder = new UriBuilder(blobUri);

            uriBuilder.Host = uriBuilder.Host.Replace(
                ".dfs.",
                ".blob.",
                StringComparison.OrdinalIgnoreCase);

            return uriBuilder.Uri;
        }

        private static Uri CreateDataLakeEndpointUri(Uri blobUri)
        {
            var uriBuilder = new UriBuilder(blobUri);
            uriBuilder.Host = uriBuilder.Host.Replace(
                ".blob.",
                ".dfs.",
                StringComparison.OrdinalIgnoreCase);

            return uriBuilder.Uri;
        }

        private static DataLakeRequestConditions? CreateDataLakeConditions(string? tag)
        {
            if (string.IsNullOrWhiteSpace(tag))
            {
                return null;
            }

            return new DataLakeRequestConditions
            {
                IfMatch = new ETag(tag),
            };
        }

        private async Task<bool> AppendDocumentsInternalAsync(
            AppendBlobClient appendBlobClient,
            IEnumerable<T> documents,
            string? tag,
            CancellationToken ct)
        {
            var payload = SerializeDocuments(documents);
            var maxBlockBytes = appendBlobClient.AppendBlobMaxAppendBlockBytes;

            if (payload.Length == 0)
            {
                return true;
            }

            if (tag != null && payload.Length > maxBlockBytes)
            {
                throw new InvalidOperationException(
                    $"Conditional append payload is {payload.Length} bytes, " +
                    $"which exceeds the append-blob single block limit of {maxBlockBytes} bytes.");
            }

            var conditions = CreateAppendConditions(tag);
            var offset = 0;
            while (offset < payload.Length)
            {
                var blockLength = Math.Min(
                    maxBlockBytes,
                    payload.Length - offset);

                await using var stream = new MemoryStream(payload, offset, blockLength, writable: false);
                if (conditions == null)
                {
                    await appendBlobClient.AppendBlockAsync(
                        stream,
                        transactionalContentHash: null,
                        conditions: conditions,
                        progressHandler: null,
                        cancellationToken: ct);
                }
                else
                {
                    var appended = await TryRunOptimisticAsync(() => appendBlobClient.AppendBlockAsync(
                        stream,
                        transactionalContentHash: null,
                        conditions: conditions,
                        progressHandler: null,
                        cancellationToken: ct));

                    if (!appended)
                    {
                        return false;
                    }
                }

                conditions = null;
                offset += blockLength;
            }

            return true;
        }

        private static bool IsOptimisticConcurrencyFailure(RequestFailedException ex)
            => ex.Status == 412;

        private static async Task<bool> TryRunOptimisticAsync(Func<Task> action)
        {
            try
            {
                await action();
                return true;
            }
            catch (RequestFailedException ex) when (IsOptimisticConcurrencyFailure(ex))
            {
                return false;
            }
        }

        private static AppendBlobRequestConditions? CreateAppendConditions(string? tag)
        {
            if (string.IsNullOrWhiteSpace(tag))
            {
                return null;
            }

            return new AppendBlobRequestConditions
            {
                IfMatch = new ETag(tag),
            };
        }

        private static byte[] SerializeDocuments(IEnumerable<T> documents)
        {
            using var stream = new MemoryStream();

            foreach (var document in documents)
            {
                JsonSerializer.Serialize(stream, document, JsonSerializerOptions);
                stream.WriteByte((byte)'\n');
            }

            return stream.ToArray();
        }

        private string CreateTemporaryPath()
        {
            var pathName = _dataLakeFileClient.Path;
            var lastSeparator = pathName.LastIndexOf('/');
            var folder = lastSeparator >= 0
                ? pathName[..(lastSeparator + 1)]
                : string.Empty;
            var fileName = lastSeparator >= 0
                ? pathName[(lastSeparator + 1)..]
                : pathName;

            return $"{folder}{fileName}.{Guid.NewGuid():N}.tmp";
        }

        private async Task<bool> TryCompactOnceAsync(CancellationToken ct)
        {
            var loaded = await LoadAllAsync(ct);
            var tempPath = CreateTemporaryPath();
            var tempBlobClient = _appendBlobClient
                .GetParentBlobContainerClient()
                .GetAppendBlobClient(tempPath);
            var tempDataLakeFileClient = _dataLakeFileClient
                .GetParentFileSystemClient()
                .GetFileClient(tempPath);

            try
            {
                await tempBlobClient.CreateAsync(cancellationToken: ct);

                var appended = await AppendDocumentsInternalAsync(
                    tempBlobClient,
                    loaded.Result,
                    null,
                    ct);

                if (!appended)
                {
                    throw new InvalidOperationException(
                        "Unable to append compacted payload to temporary blob.");
                }

                var renamed = await TryRunOptimisticAsync(() => tempDataLakeFileClient.RenameAsync(
                    destinationPath: _dataLakeFileClient.Path,
                    destinationFileSystem: _dataLakeFileClient.FileSystemName,
                    sourceConditions: null,
                    destinationConditions: CreateDataLakeConditions(loaded.Tag),
                    cancellationToken: ct));

                if (!renamed)
                {
                    await TryDeleteIfExistsAsync(tempBlobClient, ct);
                    return false;
                }

                return true;
            }
            catch
            {
                await TryDeleteIfExistsAsync(tempBlobClient, ct);
                throw;
            }
        }

        private static async Task TryDeleteIfExistsAsync(
            AppendBlobClient appendBlobClient,
            CancellationToken ct)
        {
            try
            {
                await appendBlobClient.DeleteIfExistsAsync(cancellationToken: ct);
            }
            catch (RequestFailedException)
            {
            }
        }
    }
}