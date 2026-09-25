#	Overview

This project compiles into a Console application meant to run in a container hosted in
*Azure Container Apps*.

There are two types of nodes / instances of this application:

*	Leader (see `LeaderOrchestration` class)
*	`Sub-experiment` (see `SubExperimentOrchestration` class)

`Program.Main` instantiates `MainOrchestration` which delegates the node-type decision to
`RegistrationManager` and then instantiates one of the other two orchestrators.  If no leader is
running, a leader will start, otherwise, a sub-experiment will start.

`LeaderOrchestration` plans work for sub-experiment instances.  It can change the instance count in
*Azure Container Apps* using the `InstanceManager` class.

The way nodes communicate is through an append blob controlled by `LogBlobClient<LogItem>` (see
next section).

Typically a leader will start, register a few sub-experiments (as LogItems) and increase the number of
container app instances.  New app instances will start, read the log, register as a sub-experiment
node (binding the node with a sub-experiment) and start running the experiment.

## Log Item (`LogItem`)

Log has two item types, i.e. one and only one is non-`null`:

*	`TtlRegistrationItem` - a node registration item
*	`ExperimentStepItem` - a normalized experiment-step item

###	TtlRegistrationItem

This represents a node registration.  It has an `ExpirationTime` property which is used to determine
if the node is still alive.  If the node
does not update its registration before the expiration time, it is considered dead and will be
removed from the log.

A node is identified with a `NodeId` (a GUID) which remains the same as long as the app runs.

A node type is determined by the value of `NodeItem`:

*	`null` signals this is the leader node
*	non-`null` signals this is a sub-experiment node
	* It has a sub-experiment name
	* It has a sub-experiment index (more than one instance might be necessary to deliver the
	required throughput)
	* It has a `StartTime` and an `EndTime`, copied from the experiment step the node registered
	against ; a sub-experiment node runs until that `EndTime`

### ExperimentStepItem

An `ExperimentStepItem` doesn't represent a node.  It represents a time-bounded work item for nodes
to register against.

It contains a `StartTime`, an `EndTime`, and `SubExperimentStepItemMap`, a dictionary of
`SubExperimentStepItem` entries keyed by sub-experiment name.  Each entry describes one
sub-experiment's desired node count and throughput target.

### SubExperimentStepItem

A `SubExperimentStepItem` is the normalized representation of a single work item in the step.  It
specifies the number of nodes that should participate (`NodeCount`) and the throughput each node
should deliver (`ThroughputTarget`).  The sub-experiment name isn't part of the item:  it is the key
in `SubExperimentStepItemMap`.

###	Contention

`LogBlobClient<LogItem>` takes care of contention optimistically by using e-tags (native to Azure Blob).

When an update is requested with an e-tag, if the e-tag doesn't represent the current state of the
blob, the operation fails:  `AppendAsync` returns `false` instead of throwing.  This forces the
caller to read the blob again so that each update is done knowing the current state.

Compaction (`CompactAsync`) follows the same optimistic pattern:  the compacted content is written
to a temporary blob which is then renamed (through the ADLS endpoint) over the log blob, conditioned
on the e-tag read at the start.  If the rename fails, the temporary blob is deleted and the whole
operation is retried.

##	Orchestration run

A node never knows where it is at.  For a leader, it is very possible another node ran as leader and
started sub-experiments and then failed.  Same for a sub-experiment node, it is possible another node ran
as sub-experiment and started a sub-experiment but failed before completing it.

For this reason, each node reads the log and determines what it should do.