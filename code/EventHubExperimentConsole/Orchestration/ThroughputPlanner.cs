using System;
using System.Collections.Generic;
using System.Linq;

namespace EventHubExperimentConsole.Orchestration
{
    internal class ThroughputPlanner
    {
        /// <summary>
        /// Computes what should be the next throughput to experiment with, given the historical
        /// throughputs tried and whether the last throughput succeeded or not.
        /// </summary>
        /// <remarks>
        /// Input provides only the success of the last throughput tried.
        /// Historical outcomes are inferred from the monotonic success/failure boundary.
        /// </remarks>
        /// <param name="hasLastSucceeded">Has the last throughput succeeded or not.</param>
        /// <param name="historicalThroughputs">
        /// History of all throughputs tried (at least one).
        /// </param>
        /// <param name="throughputPrecision">Maximum acceptable gap between success and
        /// failure.</param>
        /// <returns>Next throughput to try, or <c>null</c> if the process should stop
        /// here.</returns>
        public double? ComputeNextThroughput(
            bool hasLastSucceeded,
            IEnumerable<double> historicalThroughputs,
            double throughputPrecision)
        {
            ArgumentNullException.ThrowIfNull(historicalThroughputs);

            if (!double.IsFinite(throughputPrecision) || throughputPrecision <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(throughputPrecision),
                    "Throughput precision must be finite and greater than zero.");
            }

            var throughputs = historicalThroughputs.ToArray();
            if (throughputs.Length == 0)
            {
                throw new ArgumentException(
                    "At least one historical throughput is required.",
                    nameof(historicalThroughputs));
            }

            if (throughputs.Any(throughput => !double.IsFinite(throughput) || throughput <= 0))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(historicalThroughputs),
                    "Historical throughputs must be finite and greater than zero.");
            }

            var lastThroughput = throughputs[^1];

            if (hasLastSucceeded)
            {
                var lowestFailedThroughput = throughputs
                    .Where(throughput => throughput > lastThroughput)
                    .DefaultIfEmpty()
                    .Min();

                if (lowestFailedThroughput == 0)
                {
                    var doubledThroughput = lastThroughput * 2;
                    return double.IsFinite(doubledThroughput)
                        ? doubledThroughput
                        : null;
                }

                return GetNextMidpoint(
                    lastThroughput,
                    lowestFailedThroughput,
                    throughputPrecision);
            }

            var highestSuccessfulThroughput = throughputs
                .Where(throughput => throughput < lastThroughput)
                .DefaultIfEmpty()
                .Max();

            return highestSuccessfulThroughput == 0
                ? null
                : GetNextMidpoint(
                    highestSuccessfulThroughput,
                    lastThroughput,
                    throughputPrecision);
        }

        private static double? GetNextMidpoint(
            double successfulThroughput,
            double failedThroughput,
            double throughputPrecision)
        {
            var gap = failedThroughput - successfulThroughput;

            if (gap <= throughputPrecision)
            {
                return null;
            }

            return (successfulThroughput + failedThroughput) / 2;
        }
    }
}