#	Registration

This spec describes how nodes register as an active node and are assigned to run sub-experiment
steps.

`Program.Main` instantiates `MainOrchestration` which determines the node type and then
instantiates one of the other two orchestrators.

`LeaderOrchestration` plans work for sub-experiment instances.  It can change the instance count in
*Azure Container Apps* using the `InstanceManager` class.

When the leader is done scheduling work, it scales the Container App down to a single instance and
then stops the app.  This is a final cleanup step to avoid leaving the leader active after the
experiment completes.

The way nodes communicate is through an append blob controlled by `LogBlobClient<LogItem>` (see
next section).

Typically a leader will start, register a few sub-experiments (as LogItems) and increase the number of
container app instances.  New app instances will start, read the log, register as a sub-experiment
node (binding the node with a sub-experiment) and start running the experiment.

## Log Item (`LogItem`)

Log has a few item types, i.e. one and only one is non-`null`:

*	`TtlRegistrationItem` - this is legacy and should be removed
*	`NodeRegistrationItem` - a node registration item:  indicates a node exist and is alive
*	`NodeAssignationItem` - a node assignation item:  assigns an experiment step to a node
*	`ExperimentStepItem` - a normalized experiment-step item

###	NodeRegistrationItem

This represents a node registration.  It has an `ExpirationTime` property which is used to determine
if the node is still alive.  If the node
does not update its registration before the expiration time, it is considered dead.
Its assignation will be given to another node.

A node is identified with a `NodeId` (a GUID) which remains the same as long as the app runs.

When a node starts, the first thing it does is generate itself a `NodeId` and register itself in
the log with a `NodeRegistrationItem`.  This can be done using unconditional append, i.e. without
contention.

###	NodeAssignationItem

Only the leader node assign a node to a sub-experiment step.
This allows this system to scale to hundreds of nodes without contention.

### ExperimentStepItem

An `ExperimentStepItem` doesn't represent a node.  It represents a time-bounded work item for nodes
to be assigned to.

###	Contention

`LogBlobClient<LogItem>` takes care of contention optimistically by using e-tags (native to Azure Blob).

When an update is requested with an e-tag, if the e-tag doesn't represent the current state of the
blob, the operation fails:  `AppendAsync` returns `false` instead of throwing.  This forces the
caller to read the blob again so that each update is done knowing the current state.

Compaction (`CompactAsync`) follows the same optimistic pattern:  the compacted content is written
to a temporary blob which is then renamed (through the ADLS endpoint) over the log blob, conditioned
on the e-tag read at the start.  If the rename fails, the temporary blob is deleted and the whole
operation is retried.

An e-tag mismatch only means the blob changed since it was read.

###	Registration renewal and split brain

`RegistrationManager` renews its registration every half TTL.  A renewal re-reads the log (getting a
fresh e-tag), verifies ownership, then appends conditionally ; on e-tag mismatch it reloads and
retries.

Ownership is verified against the log content, not against the e-tag:  a node has lost its slot when
a non-expired `TtlRegistrationItem` from another `NodeId` covers the same slot (the leader slot for
`NodeItem == null`, or the same sub-experiment name and node index otherwise).  A leader which lost
its slot is a genuine split brain and crashes the process ; a sub-experiment node logs a warning.

When a node's registration is disposed (orchestration completed or shutdown requested, e.g. on
`SIGTERM` when *Azure Container Apps* replaces a revision), the node releases its slot by
appending an already-expired `TtlRegistrationItem`, so a replacement node doesn't have to wait
for the TTL.  The release is best effort (bounded by a short timeout) and is skipped if the node
no longer owns the slot.  Compaction keeps the last appended item per slot and then drops it if
expired, which is what makes a release effective.

All times written to the log (`ExpirationTime`, step `StartTime` / `EndTime`) are UTC.

##	Cleanup

Log file is cleaned up by the leader node.  This allows the log file not to grow indefinitely
and avoids reaching the append-blob append-count limit.