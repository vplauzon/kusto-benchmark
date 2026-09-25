using System.Collections.Generic;
using EventHubExperimentConsole.Orchestration;

namespace BenchmarkTest;

public class ThroughputPlannerTests
{
    private readonly ThroughputPlanner _planner = new();

    [Fact]
    public void DoublesAfterSuccessWithoutFailure()
    {
        AssertNextThroughput(true, [10, 20], 5, 40);
    }

    [Fact]
    public void StopsWhenFirstAttemptFails()
    {
        AssertNextThroughput(false, [10], 5, null);
    }

    [Fact]
    public void UsesMidpointAfterFailure()
    {
        AssertNextThroughput(false, [10, 20, 40], 5, 30);
    }

    [Fact]
    public void UsesMidpointAfterSuccessInBinarySearch()
    {
        AssertNextThroughput(true, [10, 20, 40, 80, 60], 5, 70);
    }

    [Fact]
    public void StopsWhenBracketIsWithinPrecision()
    {
        AssertNextThroughput(false, [10, 20], 10, null);
    }

    [Fact]
    public void RoundsMidpointDownForIntegerThroughput()
    {
        AssertNextThroughput(false, [2, 7], 2, 4);
    }

    private void AssertNextThroughput(
        bool hasLastSucceeded,
        IEnumerable<int> historicalThroughputs,
        int throughputPrecision,
        int? expectedThroughput)
    {
        var actualThroughput = _planner.ComputeNextThroughput(
            hasLastSucceeded,
            historicalThroughputs,
            throughputPrecision);

        Assert.Equal(expectedThroughput, actualThroughput);
    }
}
