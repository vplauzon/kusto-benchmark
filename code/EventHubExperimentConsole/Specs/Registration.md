# Registration

This spec describes node registration and how the leader assigns active nodes to
sub-experiment steps.

Nodes communicate through the append blob controlled by `LogBlobClient<LogItem>`. Each
`MainOrchestration` process creates a unique `NodeId` and registers it before selecting a role.

## Log items

Exactly one item type is populated in each `LogItem`:

* `NodeRegistrationItem` records that a node is alive. Its `ExpirationTime` is renewed every half
  TTL using unconditional appends. A registration is considered expired when its last recorded
  expiration time has passed.
* `NodeAssignationItem` records a role for a node. `NodeItem == null` identifies the leader;
  otherwise it assigns a sub-experiment slot and time-bounded step to the node.
* `ExperimentStepItem` describes the time-bounded work available for assignment.

## Leader election

After appending its initial `NodeRegistrationItem`, a node reads the log. If no unexpired node owns
the latest leader assignation, it conditionally appends a leader assignation using the log ETag.
Only one contender can win that append. Other nodes remain available and poll for assignments.

A leader renews its node registration and verifies that its latest assignation is still the leader
role. If the leader registration expires, an available node can claim leadership with the same
conditional append. A leader that discovers it has lost the role terminates rather than continuing
to coordinate work.

## Worker assignment

The leader polls the log every five seconds. For the latest unexpired experiment step, it considers
active node registrations and assigns available nodes to unfilled sub-experiment node indexes. It
writes all assignments from one log snapshot in a single ETag-conditional append. On an ETag
conflict, it reloads and retries on the next poll.

Workers poll every five seconds for their latest assignment. When assigned a non-expired step, a
worker runs it and then returns to polling while continuing to renew its node registration. If a
worker's registration expires, its slot is considered unfilled and can be assigned to another node.

## Contention and cleanup

Node registrations and renewals use unconditional appends, avoiding contention between workers
claiming slots. Leader election and assignment use conditional appends because they must be
coordinated. An ETag mismatch only means the log changed since it was read; it is not itself a
registration failure.

The leader compacts the log every five minutes. Compaction retains the latest registration for each
unexpired node, the latest assignation for each retained node, and all experiment steps. All times
written to the log are UTC.
