using System.Collections.Concurrent;

namespace BenchmarkLib
{
    public class IngestionMetricWriter
    {
        #region Inner types
        private record Metric(
            DateTime Timestamp,
            TimeSpan Duration,
            long UncompressedSize,
            long CompressedSize,
            long RowCount);
        #endregion

        private static readonly TimeSpan PERIOD = TimeSpan.FromSeconds(10);
        private readonly ConcurrentQueue<Metric> _metrics = new();
        private readonly object _lock = new object();
        private DateTime _blockStart = DateTime.Now;

        public void Write(
            TimeSpan duration,
            long uncompressedSize,
            long compressedSize,
            long rowCount)
        {
            bool blockComplete = false;

            lock (_lock)
            {
                if (!_metrics.Any())
                {
                    _blockStart = DateTime.Now;
                }
                if (DateTime.Now.Subtract(_blockStart) > PERIOD)
                {
                    blockComplete = true;
                }
            }
            _metrics.Enqueue(new Metric(
                DateTime.Now,
                duration,
                uncompressedSize,
                compressedSize,
                rowCount));
            if (blockComplete)
            {
                ExportMetrics();
            }
        }

        private void ExportMetrics()
        {
            var list = new List<Metric>();

            while (_metrics.TryDequeue(out var metric))
            {
                list.Add(metric);
            }

            var startDate = list.Min(m => m.Timestamp);
            var startDateText = startDate.ToString("yyyy-MM-dd HH:mm:ss.ffff");
            var maxLatency = list.Max(m => m.Duration);
            var uncompressedSize = list.Sum(m => m.UncompressedSize);
            var compressedSize = list.Sum(m => m.CompressedSize);
            var rowCount = list.Sum(m => m.RowCount);

            Console.WriteLine(
                $"#metric# Timestamp={startDateText}, Uncompressed={uncompressedSize}, "
                + $"Compressed={compressedSize}, MaxLatency={maxLatency}, "
                + $"RowCount={rowCount}, BlobCount={list.Count}");
        }
    }
}