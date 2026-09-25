#	Scheduling

This spec describes how the leader schedules experiment steps to find the throughput
breaking point for each sub-experiment. Read the [Overview spec](Overview.md) first.

## Model

An experiment is a collection of independent sub-experiments, each targeting a different
Kusto cluster. The leader runs experiments by scheduling steps. A step is a time-bounded
work item; all active sub-experiments in the step run concurrently for the same duration.

Each sub-experiment is configured by `SubExperimentConfig`, which specifies:

*	`ThroughputTargetStart` — the aggregate starting throughput target
*	`ThroughputPrecision` — the maximum acceptable gap between the highest successful
	and lowest failed aggregate throughput

Each step has a duration specified by `ExperimentConfig.SubExperimentDuration`,
applied uniformly to all sub-experiments in that step (to run them concurrently and save time).

## Step execution and success

The leader schedules an `ExperimentStepItem` containing one or more sub-experiments.
All sub-experiments in a step run concurrently for the same duration.

When a step completes, the leader queries the related Kusto cluster to verify that
ingestion was streaming (100% ingestion, no fallback to batching) for the full
`SubExperimentDuration`.

*	If yes, the step is a **success** for that sub-experiment.
*	If no (batching occurred), the step is a **failure** for that sub-experiment.

## Search algorithm

The leader searches for each sub-experiment's breaking point using the following
algorithm:

1.	Start with `AggregateThroughputTarget = ThroughputTargetStart`.
2.	Schedule a step.
3.	After the step completes:
	*	If it **failed and no success has been recorded**: the starting target was too
		high. Mark the sub-experiment complete (no breaking point found).
	*	If it **succeeded**: double the target (`AggregateThroughputTarget *= 2`) and
		schedule the next step.
	*	If it **failed and a prior success exists**: enter binary search mode (see
		below).

### Binary search (backtracking)

Once a failure occurs after a success, the leader has brackets: the highest successful
target `T_success` and the lowest failed target `T_fail`.

For each subsequent step:

1.	Set `AggregateThroughputTarget = (T_success + T_fail) / 2`.
2.	Schedule a step.
3.	After the step completes:
	*	If it **succeeded**: set `T_success = AggregateThroughputTarget`; continue.
	*	If it **failed**: set `T_fail = AggregateThroughputTarget`; continue.
4.	Repeat until `T_fail - T_success ≤ ThroughputPrecision`.

Throughputs are integers, so midpoint division rounds down. `ThroughputPrecision` must be
positive. At that point, the sub-experiment is complete, with the breaking point narrowed to
the interval `[T_success, T_fail]`.

## Node distribution

The leader converts each `AggregateThroughputTarget` into concrete execution parameters:

*	Divide `AggregateThroughputTarget` by a constant maximum throughput per node to
	determine `NodeCount`.
*	Calculate `NodeThroughputTarget = AggregateThroughputTarget / NodeCount`.
*	Write these into `SubExperimentStepItem` and append the `ExperimentStepItem` to
   the log.

Each sub-experiment node receives its assigned `NodeThroughputTarget` and produces
that throughput to the Event Hub.
