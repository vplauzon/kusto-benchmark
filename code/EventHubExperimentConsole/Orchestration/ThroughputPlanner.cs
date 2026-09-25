using System;
using System.Collections.Generic;
using System.Text;

namespace EventHubExperimentConsole.Orchestration
{
    internal class ThroughputPlanner
    {
        /// <summary>
        /// Computes what should be the next throughput to experiment with.
        /// , given the historical
        /// throughputs tried and whether the last throughput succeeded or not.
        /// </summary>
        /// <remarks>
        /// Input provides only the success of the last throughput tried.
        /// Historical ones can be deduced from the list of historical throughputs, depending if
        /// they increase or decrease.
        /// </remarks>
        /// <param name="hasLastSucceeded">Has the last throughput succeeded or not.</param>
        /// <param name="historicalThroughputs">
        /// History of all throughputs tried (at least one).
        /// </param>
        /// <param name="throughputPrecision">Precision for throughput</param>
        /// <returns>Next throughput to try or <c>null</c> if the process should stop here.</returns>
        public int? ComputeNextThroughput(
            bool hasLastSucceeded,
            IEnumerable<int> historicalThroughputs,
            int throughputPrecision)
        {
            throw new NotImplementedException();
        }
    }
}