using System.Collections.Concurrent;

namespace BenchmarkLib
{
    public class IngestionMetricWriter : IAsyncDisposable
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
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _backgroundTask;

        #region Construction
        public IngestionMetricWriter()
        {
            _backgroundTask = MonitorAsync(_cts.Token);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _cts.Cancel();
            await _backgroundTask;
        }

        public void Write(
            TimeSpan duration,
            long uncompressedSize,
            long compressedSize,
            long rowCount)
        {
            _metrics.Enqueue(new Metric(
                DateTime.Now,
                duration,
                uncompressedSize,
                compressedSize,
                rowCount));
        }

        private async Task MonitorAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                await Task.Delay(PERIOD);
                PublishMetrics();
            }
        }

        private void PublishMetrics()
        {
            var list = GetAllMetrics();
            var metricsByMinute = list
                .GroupBy(m => m.Timestamp.Minute);

            foreach (var metrics in metricsByMinute)
            {
                var startDate = metrics.Min(m => m.Timestamp);
                var startDateText = startDate.ToString("yyyy-MM-dd HH:mm:ss.ffff");
                var maxLatency = metrics.Max(m => m.Duration);
                var uncompressedSize = metrics.Sum(m => m.UncompressedSize);
                var compressedSize = metrics.Sum(m => m.CompressedSize);
                var rowCount = metrics.Sum(m => m.RowCount);

                Console.WriteLine(
                    $"#metric# Timestamp={startDateText}, Uncompressed={uncompressedSize}, "
                    + $"Compressed={compressedSize}, MaxLatency={maxLatency}, "
                    + $"RowCount={rowCount}, BlobCount={list.Count}");
            }
        }

        private List<Metric> GetAllMetrics()
        {
            var list = new List<Metric>();

            while (_metrics.TryDequeue(out var metric))
            {
                list.Add(metric);
            }

            return list;
        }
    }
}