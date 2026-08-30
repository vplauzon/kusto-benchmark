#	Overview

This project compiles into a Console application meant to run in a container hosted in Azure Container App.

There are two types of nodes / instances of this application:  the orchestrator and the experiment nodes.
The orchestrator (see `ExperimentOrchestration`) is the one that is started first and it will start other instances of itself
to run experiments.

The orchestrator can change the number of instances in Azure Container App using the `InstanceManager` class.

The way the orchestrator and the other instances communicate is through an append blob controlled by `LogBlobClient<LogItem>`.
Looking at `LogItem`, it can be either a node (instance) registration or an experiment a group of instances should run.  This
is the communication mechanic.

Typically an orchestrator will start by itself, will register a few experiments (as LogItems) and will increase the number of
container app instances.  New app will start, read the log, register as experiment node (binding the node with an experiment) and
start running the experiment.