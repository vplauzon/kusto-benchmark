using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Text;

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
        /// <summary>Construct an instance pointing to a blob path.</summary>
        /// <param name="blobPath">ADLS gen-2 path to the log blob.</param>
        /// <param name="compactFunc">Function which compacts a list of deserialized lines</param>
        public LogBlobClient(
            string blobPath,
            Func<IEnumerable<T>, IEnumerable<T>> compactFunc)
        {
            //  Create the underlying append blob client
            throw new NotImplementedException();
        }

        void IDisposable.Dispose()
        {
            //  Dispose whatever needs to be disposed
            //  If async-dispose is required, we should make the class Iasyncdisposable
            throw new NotImplementedException();
        }

        /// <summary>
        /// Load the entire blob, deserialize each line, compact them and return the
        /// compacted list.  It also returns the blob version's tag.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        public Task<TaggedResult<IImmutableList<T>>> LoadAllAsync(CancellationToken? ct = null)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Compact the content of the blob.
        /// </summary>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task CompactAsync(CancellationToken? ct = null)
        {
            //  Call LoadAllAsync (which compacts documents in memory)
            //  Do that by creating a temporary blob in the same folder
            //  And rename / move the temporary blob in place of the main blob
            //  using a tag to ensure nobody else changed the blob in-between
            throw new NotImplementedException();
        }

        /// <summary>Append a single document.</summary>
        /// <param name="document"></param>
        /// <param name="tag">Optional tag:  if provided, the append only happends if tag hasn't changed.</param>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task AppendAsync(
            T document,
            string? tag = null,
            CancellationToken? ct = null)
        {
            //  What exception is thrown if tag changed?
            throw new NotImplementedException();
        }

        /// <summary>Append a multiple documents.</summary>
        /// <param name="documents"></param>
        /// <param name="tag"></param>
        /// <param name="ct"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public Task AppendAsync(
            IEnumerable<T> documents,
            string? tag = null,
            CancellationToken? ct = null)
        {
            throw new NotImplementedException();
        }
    }
}