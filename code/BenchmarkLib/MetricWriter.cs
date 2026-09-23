using System.Collections.Concurrent;
using System.Collections.Immutable;

namespace BenchmarkLib
{
    public class MetricWriter : IAsyncDisposable
    {
        #region Inner types
        private record Metric(
            DateTime Timestamp,
            IImmutableList<string> DimensionValues,
            string MetricName,
            double MetricValue);
        #endregion

        private readonly IImmutableList<string> _dimensionNames;
        private readonly IImmutableList<string> _metricNames;
        private readonly TimeSpan _publishPeriod;
        private readonly ConcurrentQueue<Metric> _metricQueue = new();
        private readonly CancellationTokenSource _cts = new CancellationTokenSource();
        private readonly Task _backgroundTask;

        #region Construction
        public MetricWriter(
            IEnumerable<string> dimensionNames,
            IEnumerable<string> metricNames,
            TimeSpan publishPeriod)
        {
            _dimensionNames = dimensionNames.ToImmutableArray();
            _metricNames = metricNames.ToImmutableArray();
            _publishPeriod = publishPeriod;
            _backgroundTask = PublishAsync(_cts.Token);
        }
        #endregion

        async ValueTask IAsyncDisposable.DisposeAsync()
        {
            _cts.Cancel();
            await _backgroundTask;
        }

        public void WriteMetric(IEnumerable<string> dimensionValues, string metricName, double metricValue)
        {
            _metricQueue.Enqueue(new(
                DateTime.UtcNow,
                dimensionValues.ToImmutableArray(),
                metricName,
                metricValue));
        }

        private async Task PublishAsync(CancellationToken ct)
        {
            IImmutableList<Metric> remainMetrics = ImmutableArray<Metric>.Empty;

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    await Task.Delay(_publishPeriod, ct);
                    remainMetrics = PublishMetrics(remainMetrics, false);
                }
            }
            catch
            {
                PublishMetrics(remainMetrics, true);
                throw;
            }
        }

        private IImmutableList<Metric> PublishMetrics(
            IImmutableList<Metric> remainMetrics,
            bool publishIncompleteBucket)
        {
            DateTime Bin(DateTime value, TimeSpan bucket)
            {
                if (bucket <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(nameof(bucket));
                }

                var ticks = value.Ticks / bucket.Ticks * bucket.Ticks;

                return new DateTime(ticks, value.Kind);
            }

            var now = DateTime.UtcNow;
            var currentBucket = Bin(now, _publishPeriod);
            var list = GetQueuedMetrics();
            var metricsToKeep = remainMetrics.Concat(list)
                .Where(m => !publishIncompleteBucket && m.Timestamp >= currentBucket);
            var metricsByBucket = remainMetrics.Concat(list)
                .Where(m => publishIncompleteBucket || m.Timestamp < currentBucket)
                .GroupBy(m => Bin(m.Timestamp, _publishPeriod))
                .OrderBy(g => g.Key);

            foreach (var metricBucket in metricsByBucket)
            {
                var metricByDimensionValues = metricBucket
                    .GroupBy(m => string.Join('-', m.DimensionValues));

                foreach (var metricByDimensionValue in metricByDimensionValues)
                {
                    var dimensionValues = metricByDimensionValue.First().DimensionValues;
                    var metricValueMap = metricByDimensionValue
                        .GroupBy(m => m.MetricName)
                        .ToDictionary(g => g.Key, g => g.Sum(m => m.MetricValue));

                    Console.Write($"#metric# Timestamp={metricBucket.Key}, ");
                    //  Dimensions
                    Console.Write(string.Join(
                        ", ",
                        _dimensionNames
                        .Zip(dimensionValues, (n, v) => $"{n}={v}")));
                    if (_dimensionNames.Count > 0)
                    {
                        Console.Write(", ");
                    }
                    //  Metrics
                    Console.Write(string.Join(
                        ", ",
                        _metricNames
                        .Select(n => metricValueMap.ContainsKey(n)
                        ? $"{n}={metricValueMap[n]}"
                        : $"{n}=0")));
                    Console.WriteLine();
                }
            }

            return metricsToKeep.ToImmutableArray();
        }

        private List<Metric> GetQueuedMetrics()
        {
            var list = new List<Metric>();

            while (_metricQueue.TryDequeue(out var metric))
            {
                list.Add(metric);
            }

            return list;
        }
    }
}