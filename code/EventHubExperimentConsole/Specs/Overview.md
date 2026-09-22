#	Overview

This project compiles into a Console application meant to run in a container hosted in *Azure Container App*.

There are two types of nodes / instances of this application:

*	Leader (see `LeaderOrchestration` class)
*	`Benchmark` (see `BenchmarkOrchestration` class)

`Program.Main` instantiates `MainOrchestration` which then decides which node type it should be and instantiate one of the
other two orchestrators.  If no leader is running, leader will start, otherwise, a benchmark will start.

`LeaderOrchestration` plans work for benchmark instances.  It can change instance count in *Azure Container App* using
the `InstanceManager` class.

The way nodes communicate is through an append blob controlled by `LogBlobClient<LogItem>` (see next section).

Typically a leader will start, register a few experiments (as LogItems) and increase the number of
container app instances.  New app instances will start, read the log, register as sub-experiment node (binding the node with a sub
experiment) and start running the experiment.

## Log Item (`LogItem`)

Logs have two types:

*	`TtlRegistrationItem` - a node registration item
*	`SubExperimentItem` - a sub-experiment item

###	TtlRegistrationItem

This represents a node registration.  It has a `Ttl` (time to live) property which is used to determine if the node
is still alive.  If the node does not update its registration before the TTL expires, it is considered dead
and will be removed from the log.

A node is identified with a `NodeId` (a GUID) which remains the same as long as the app runs.

A node type is determined by the value of `NodeItem`:

*	`null` signals this is the leader node
*	non-`null` signals this is a benchmark node
	* It has a sub-experiment name
	* It has a sub-experiment index (more than one instance might be necessary to deliver the required throughput)

### SubExperimentItem

A `SubExperimentItem` doesn't represent a node.  It represents a work item for nodes to register against.

It specifies the name of the sub-experiment, the time window it should occur, the number of nodes that
should participate and the throughput each node should deliver.

###	Contention

LogBlobClient<> takes care of contention optimistically by using e-tags (native to Azure Blob).

When an update is requested with an e-tag, if the e-tag doesn't represent the current state of the blob,
the operation fails.  This forces the reader to read the blob again so that each update is done knowing the
current state.