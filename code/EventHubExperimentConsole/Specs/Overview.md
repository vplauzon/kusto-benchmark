#	Overview

This project compiles into a Console application meant to run in a container hosted in
*Azure Container Apps*.

See the [Coding standard](CodingStandard.md) for repository formatting requirements.

There are two types of nodes / instances of this application:

*	Leader (see `LeaderOrchestration` class)
*	`Sub-experiment` (see `SubExperimentOrchestration` class)

The [registration specs](Registration.md) details how nodes register and are assigned tasks.

##	Orchestration run

A node never knows where it is at.  For a leader, it is very possible another node ran as leader and
started sub-experiments and then failed.  Same for a sub-experiment node, it is possible another node ran
as sub-experiment and started a sub-experiment but failed before completing it.

For this reason, each node reads the log and determines what it should do.