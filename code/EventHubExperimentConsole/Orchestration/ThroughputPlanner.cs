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
        public int? ComputeNextThroughput(
            bool hasLastSucceeded,
            IEnumerable<int> historicalThroughputs,
            int throughputPrecision)
        {
            ArgumentNullException.ThrowIfNull(historicalThroughputs);

            if (throughputPrecision <= 0)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(throughputPrecision),
                    "Throughput precision must be greater than zero.");
            }

            var throughputs = historicalThroughputs.ToArray();
            if (throughputs.Length == 0)
            {
                throw new ArgumentException(
                    "At least one historical throughput is required.",
                    nameof(historicalThroughputs));
            }

            if (throughputs.Any(throughput => throughput <= 0))
            {
                throw new ArgumentOutOfRangeException(
                    nameof(historicalThroughputs),
                    "Historical throughputs must be greater than zero.");
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
                    var doubledThroughput = (long)lastThroughput * 2;
                    return doubledThroughput <= int.MaxValue
                        ? (int)doubledThroughput
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

        private static int? GetNextMidpoint(
            int successfulThroughput,
            int failedThroughput,
            int throughputPrecision)
        {
            var gap = (long)failedThroughput - successfulThroughput;
            if (gap <= throughputPrecision)
            {
                return null;
            }

            return (int)(((long)successfulThroughput + failedThroughput) / 2);
        }
    }
}