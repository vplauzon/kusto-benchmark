namespace EventHubExperimentConsole.Items
{
    internal record SubExperimentStepItem(
        double AggregateThroughputTarget,
        int NodeCount)
    {
        public double NodeThroughputTarget => AggregateThroughputTarget / NodeCount;
    }
}