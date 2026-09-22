#	Overview

This project compiles into a Console application meant to run in a container hosted in *Azure Container App*.

There are two types of nodes / instances of this application:

*	Leader (see `LeaderOrchestration` class)
*	`Benchmark` (see `BenchmarkOrchestration` class)

`Program.Main` instantiates `MainOrchestration` which then decides which node type it should be and instantiate one of the
other two orchestrator.  If no node is running, leader will start, otherwise, a benchmark will start.

`LeaderOrchestration` plans work for benchmark instances.  It can change instance number of in *Azure Container App* using
the `InstanceManager` class.

The way nodes communicate is through an append blob controlled by `LogBlobClient<LogItem>`.
Looking at `LogItem`, it can be either a node registration (`TtlRegistrationItem`) or a sub-experiment (SubExperimentItem)
a group of instances should run.  This is the communication mechanic.

Typically a leader will start, register a few experiments (as LogItems) and increase the number of
container app instances.  New app will start, read the log, register as sub-experiment node (binding the node with a sub
experiment) and start running the experiment.

