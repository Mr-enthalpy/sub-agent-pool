# Local Agent Scheduler — Architecture Direction and Core Invariants

Status: Architecture Direction
Phase: Pre-V0.1 Implementation
Audience: Implementation / Architecture Agents
Execution Status: DO NOT IMPLEMENT DIRECTLY FROM THIS DOCUMENT
Initial Document Policy: Preserve as one document for now. Split only after V0.1 architecture and interfaces have stabilized.

---

## 1. Document Purpose

This document records the long-term architectural direction, semantic boundaries, core entities, lifecycle model, failure model, and non-negotiable invariants of the Local Agent Scheduler project.

It is intentionally not yet a detailed implementation specification.

An implementation agent receiving this document should:

1. Treat it as the authoritative architecture direction.
2. Preserve the semantic boundaries and invariants defined here.
3. Not begin large-scale implementation solely from this document.
4. Record this document intact in the repository during the initial phase.
5. Avoid prematurely splitting it into many design documents.
6. Once V0.1 becomes concrete, split derived specifications as needed, for example:

   * scheduler state machines
   * SQLite schema
   * task/result protocols
   * pool lifecycle
   * adapter contract
   * recovery semantics
   * configuration schema
   * CLI/process architecture
7. If later implementation pressure conflicts with an invariant in this document, treat that as an architecture issue requiring explicit review rather than silently weakening the invariant.

The purpose of this document is continuity of architectural intent across agents, executions, frontend changes, and future implementation phases.

---

# 2. Project Definition

The project is a:

**General-Purpose Local Agent Scheduler / Agent Pool Runtime**

It is not:

* a Codex subagent configuration helper
* a DeepSeek worker manager
* a CCR scheduler
* a TokenRhythm routing layer
* a replacement model provider
* an AI root/orchestrator
* a generic workflow engine

Codex is the first execution/frontend integration target for V0.1.

DeepSeek, CCR, TokenRhythm, and current provider routing are deployment circumstances of the first environment. They must not become scheduler semantics.

The core long-term separation is:

```text
Scheduler owns semantics.
Adapter owns execution mechanics.
Provider layer owns model/provider resolution.
```

---

# 3. Historical Context

The architecture emerged from repeated failures caused by assigning too much semantic responsibility to a rapidly evolving frontend multi-agent runtime.

The original execution topology was approximately:

```text
User
  ↓
Official Codex Root
  ↓
Multiple specialized DeepSeek workers
  ↓
CCR / TokenRhythm / external DeepSeek-compatible deployment
```

The desired worker pool included roles such as:

* scout
* analyst
* auditor
* coder
* test coder
* configuration coder
* generic worker

The root was intended to retain:

* decomposition
* scheduling
* integration authority
* auditing
* phase gates

External-model workers were intended to perform token-heavy execution.

The existing `flash-subagent-helper` project successfully addresses the model/frontend enablement layer:

```text
Codex
→ local CCR
→ external OpenAI-Responses-compatible deployment
```

Historically one current selector has been:

```text
基元律动/deepseek-v4-flash-0731
```

This is a TokenRhythm selector/alias.

It must not be described as the official DeepSeek model ID.

`flash-subagent-helper` remains an external-model enablement and Codex integration package.

It is not the Scheduler Core.

---

# 4. Lessons from the Existing Runtime

Several failures led directly to the current architecture.

## 4.1 Physical child-thread continuity is not a reliable lifecycle primitive

Same-thread continuation and project synchronization could not be treated as reliable.

Therefore:

```text
Logical Agent != Physical Child Thread
```

Logical continuity must survive fresh physical executions.

Same-thread reuse may remain an optimization.

It must never be required for correctness.

---

## 4.2 Physical spawn is not authoritative task delivery

A physical worker may exist without actually receiving the authoritative task.

Therefore:

```text
Execution existence != Task ownership
```

The Scheduler must own durable task state.

Frontend message mechanisms may bridge a task into an execution, but they are never the authoritative queue.

---

## 4.3 Root-side waiting is a scheduling problem

If the root must manually monitor workers, retry failures, inspect health, or repeatedly decide whether workers are finished, the root and workers compete for execution attention.

Batch coordination and worker supervision therefore belong in the Scheduler.

Root waiting is only a frontend behavior used to sleep until Scheduler events arrive.

---

## 4.4 Physical execution failure is not logical-agent death

429, 5xx, timeout, frontend failure, process crash, host shutdown, or runtime loss may kill one physical execution without terminating the semantic worker.

Therefore logical identity and physical embodiment must be separate.

---

## 4.5 Provider failure handling and task scheduling are orthogonal

Credential selection, provider routing, CCR cooldown, API keys, model selectors, and upstream routing must remain outside Scheduler Core.

The Scheduler sees opaque execution profiles and standardized execution outcomes.

---

# 5. Architectural Layers

The system has three primary layers plus a root notification boundary.

## 5.1 Scheduler Core

The Scheduler owns deterministic control-plane semantics:

* durable task queue
* task dependencies
* task matching
* claim
* lease
* heartbeat
* attempt lifecycle
* ACK/NACK
* retry timing
* batch coordination
* result queue
* logical agents
* pool topology
* agent affinity
* incarnation lifecycle
* workstreams
* checkpoints
* bounded continuity state
* suspend
* escalation
* crash recovery
* topology reconciliation
* durable root notifications

The Scheduler Core must not know:

* Codex APIs
* OpenCode APIs
* CCR internals
* TokenRhythm
* DeepSeek-specific routing
* API keys
* credential rotation
* frontend command syntax
* runtime harness internals

---

## 5.2 Execution Adapter

An Execution Adapter translates Scheduler execution requests into frontend/runtime mechanics.

V0.1 provides a Codex Adapter.

Possible future adapters include:

* OpenCode
* Claude Code
* CLI process
* direct API runtime
* other local agent execution terminals

Adapters should be deliberately thin and cheap to rewrite.

Do not build a large compatibility hierarchy in anticipation of unknown frontend evolution.

If a future Codex release changes execution mechanics, updating or rewriting the Codex Adapter is preferable to contaminating Scheduler Core.

---

## 5.3 Provider / Model Layer

The Scheduler refers only to an opaque:

```text
ExecutionProfile
```

Examples:

```text
deepseek_flash_worker
cheap_scout
large_reasoner
independent_auditor
```

The concrete mapping from an ExecutionProfile to:

* model
* provider
* CCR route
* TokenRhythm selector
* credential
* API endpoint

belongs outside Scheduler Core.

---

## 5.4 RootBridge

Worker execution and root notification are separate boundaries.

An Execution Adapter must not automatically own root notification.

This is required for mixed-terminal configurations such as:

```text
Root: Codex

Workers:
  Codex
  OpenCode
```

A future OpenCode worker adapter must not need to understand how to wake a Codex root.

Therefore:

```text
ExecutionAdapter
```

and:

```text
RootBridge
```

are separate logical interfaces.

V0.1 may implement both inside the same Codex integration package, but Scheduler Core must treat them separately.

---

# 6. Core Data Flow

The core flow is bidirectional and durable:

```text
Root
 │
 │ submit semantic plan / tasks
 ▼
Task Queue
 │
 │ claim + lease
 ▼
Logical Agent
 │
 ▼
Incarnation
 │
 ▼
Execution
 │
 ├──────── success ───────► Result Queue
 │
 └──────── failure ───────► Failure / Recovery / Escalation
                              │
                              ▼
                        Root Notification
                              │
                              ▼
                             Root
```

The Scheduler is the source of truth throughout this flow.

Physical runtime threads, frontend sessions, and worker messages are not authoritative.

---

# 7. Task Semantics

A Task is the authoritative semantic work unit.

Candidate V0.1 states:

```text
BLOCKED
QUEUED
LEASED
RUNNING
RETRY_WAIT
SUSPENDED
COMPLETED
CANCELLED
```

`COMPLETED` and `CANCELLED` are terminal.

A physical execution failure does not imply Task failure.

A Task is only consumed when a valid current attempt successfully ACKs.

The queue therefore follows:

```text
QUEUED
  ↓ claim
LEASED
  ↓ physical execution confirmed
RUNNING
  ↓ valid success ACK
COMPLETED
```

Transient or mechanically recoverable failure:

```text
RUNNING
  ↓
RETRY_WAIT
  ↓ eligibility reached
QUEUED
```

Unrecoverable or semantically ambiguous failure:

```text
RUNNING / RETRY_WAIT
  ↓
SUSPENDED
  ↓
ESCALATION TO ROOT
```

Do not promise exactly-once execution.

The execution model is:

```text
AT-LEAST-ONCE EXECUTION
+
IDEMPOTENCY-AWARE RECOVERY
```

---

# 8. Attempt and Lease

`Attempt` is a first-class scheduling entity.

It must not be reduced to an integer field on Execution.

A claim creates an Attempt even when runtime creation later fails.

Example:

```text
claim Task
→ Attempt A1 created
→ Lease created
→ adapter start fails before Execution exists
```

This is still an Attempt.

Candidate Attempt states:

```text
ACTIVE
SUCCEEDED
FAILED
EXPIRED
CANCELLED
```

A Lease represents the current authority of an Attempt to mutate authoritative Task state.

Candidate Lease states:

```text
ACTIVE
RELEASED
EXPIRED
REVOKED
```

Every lease must carry a fencing identity, for example a monotonically increasing per-task epoch.

A stale attempt may finish physically.

It must not be allowed to:

* complete the Task
* NACK the current Task
* promote a checkpoint
* replace logical-agent current state

Example:

```text
Attempt A1: lease epoch 7 expires

Attempt A2: lease epoch 8 starts

A1 later returns success
```

The A1 Execution may be recorded as physically successful, but its success is stale and cannot mutate the Task.

---

# 9. Result Queue

Successful execution does not directly send a result to Root.

A valid Task completion atomically creates a durable Result.

Conceptually:

```text
Task Queue
→ Agent
→ successful execution
→ Result Queue
→ notify Root
→ Root consumes Result
```

Implementation must not literally delete or move the Task row.

Instead:

```text
Task → COMPLETED
+
Result → AVAILABLE
```

occur in one transaction.

The Result is a first-class entity.

Candidate fields include:

```text
id
task_id
batch_id
attempt_id
logical_agent_id
execution_id

payload_ref
summary_ref
checkpoint_ref
workspace_state_ref

state
created_at
consumed_at
consumer_ref
disposition
```

Candidate Result states:

```text
AVAILABLE
ACKED
```

Worker ACK and Root Result ACK are different operations.

Worker ACK means:

```text
Task execution successfully committed.
```

Root Result ACK means:

```text
Root consumed the already-committed Result.
```

Root consumption must not control whether the Task is considered complete.

---

# 10. Batch Semantics

Batch is a first-class Scheduler entity.

Candidate states:

```text
OPEN
ACTIVE
SUSPENDED
COMPLETED
CANCELLED
```

A normal batch completes when all required Tasks have durably completed and their authoritative Results have entered the Result Queue. V0.1 exposes no optional Task path, so every submitted Task is required at this barrier.

Root consumption of those Results is not required for Scheduler completion.

If one or more Tasks require semantic intervention:

```text
Batch → SUSPENDED
```

Already completed Results remain available.

By default, suspension stops new claims from the suspended Batch while already-running Attempts may drain.

Root receives a durable decision-required notification.

Root then consumes:

* already completed results
* failure summaries
* suspended task state
* escalation data

and chooses recovery operations.

---

# 11. Partial Success and Recovery Authority

Partial success is not a condition the Scheduler may interpret semantically.

Example:

```text
T1 → Result R1
T2 → failure
T3 → Result R3
```

The Scheduler may perform only recovery already authorized by deterministic policy.

For example:

```text
TRANSIENT_EXTERNAL
+
attempt < configured maximum
→ retry
```

When declared mechanical recovery is exhausted or no deterministic policy applies:

```text
Task → SUSPENDED
Batch → SUSPENDED
Escalation created
Root notified
```

The Scheduler must not invent a strategy such as:

* retry because most tasks succeeded
* rollback the batch
* replace the model
* change role
* ignore the failure
* accept a partial result

These are Root decisions.

The Scheduler exposes operations.

Root composes them.

Examples of useful recovery primitives:

```text
retry_task
retry_with_execution_policy
replace_agent_and_retry
cancel_task
cancel_batch
resubmit_task
resume_batch
ack_result
```

A `COMPLETED` Task should not be reopened.

If Root requires new work based on an old Task, create a new Task with provenance such as:

```text
supersedes_task_id
```

Historical Results remain intact.

---

# 12. Logical Agent

A LogicalAgent is Scheduler-owned semantic identity.

It is not:

* a Codex child thread
* a process
* a model session
* a container
* an API connection

It may survive multiple physical runtime instances.

It contains semantic continuity such as:

* partition/role membership
* workstream
* long-term continuity identity
* checkpoint reference
* bounded continuity memory
* current assignment
* health/lifecycle state

V0.1 should keep one important simplifying invariant:

```text
One LogicalAgent has at most one active Task assignment.
```

Parallel capacity should be created using multiple LogicalAgents rather than concurrent tasks inside one LogicalAgent.

---

# 13. Incarnation

`Incarnation` represents one physical embodiment of a LogicalAgent.

This creates an explicit distinction among:

```text
LogicalAgent
Incarnation
Execution
Attempt
```

Definitions:

```text
LogicalAgent
= semantic identity

Incarnation
= one physical embodiment of that identity

Execution
= one concrete runtime execution

Attempt
= one Scheduler authority epoch over a Task
```

Example:

```text
LogicalAgent A17

Incarnation 1
  ├─ Execution E1
  ├─ Execution E2
  └─ physical runtime dies

Incarnation 2
  ├─ restores durable continuity
  └─ continues as LogicalAgent A17
```

This is revival.

---

# 14. Revival vs New Birth

Revival and new birth are semantically different.

## Revival

Physical runtime dies, but semantic identity remains:

```text
LogicalAgent A17
Incarnation 1 dies
↓
Incarnation 2 created
↓
LogicalAgent remains A17
```

The new Incarnation inherits the latest committed durable continuity state.

To external scheduling semantics, A17 remains logically alive.

---

## New Birth

The previous LogicalAgent is semantically dead or a new pool member is required:

```text
LogicalAgent A17 → RETIRED

LogicalAgent A23 → BORN
```

A23 may receive the latest workstream/project state for synchronization.

It is still a different agent.

It does not inherit A17's identity.

A semantically retired LogicalAgent must not later be silently revived.

Thus:

```text
physical death
→ may cause revival

semantic retirement
→ requires new birth if capacity is later needed
```

Semantic retirement also fences every reusable STARTING/WARM/COLD Incarnation.
An identity that is terminal at the LogicalAgent layer cannot retain an
authoritative warm physical presence outside Scheduler lifecycle ownership.

---

# 15. Logical Agent Memory

Long-lived agents should benefit from physically long-lived runtime state where available.

A warm physical Incarnation may preserve high-value runtime-local continuity such as:

* model context
* cache locality
* harness state
* frontend session state
* runtime-local indexes
* temporary tool state

Scheduler must not reproduce or own these runtime details.

Warm physical continuity is preferred when available.

It is an optimization, not a correctness requirement.

Durable continuity therefore has three conceptual layers:

```text
Layer 1
Runtime-local hot memory
- opaque to Scheduler
- highest continuity/cache value
- may disappear on physical death

Layer 2
Scheduler-owned continuity capsule
- bounded
- structured
- durable
- portable between Incarnations

Layer 3
Authoritative project/workstream state
- workspace
- repository
- project artifacts
- checkpoint/project state references
```

The continuity capsule may include:

```text
INVARIANTS
DECISIONS
CURRENT DESIGN
REJECTED ALTERNATIVES
OPEN QUESTIONS
KNOWN FAILURES
CURRENT CHECKPOINT
NEXT LIKELY STEPS
```

It must not evolve into:

* an unlimited transcript database
* a vector memory platform
* a replacement harness
* an autonomous semantic retrieval subsystem
* a full reconstruction of internal model state

Revival means restoring enough durable semantic continuity to preserve LogicalAgent identity and workstream lineage.

It does not mean recreating an identical hidden model state.

---

# 16. Pool Topology

LogicalAgents exist within Scheduler-owned pool topology.

The Scheduler should maintain:

```text
Desired Pool Topology
+
Actual LogicalAgent Population
```

and reconcile the two deterministically.

A useful core entity is:

```text
PoolPartition
```

A PoolPartition represents a task-consumption responsibility.

It is richer than a simple read/write role.

Examples might include:

```text
backend_architecture
repo_exploration
independent_audit
test_implementation
configuration_changes
```

The Scheduler should maintain sufficient logical capacity according to partition policy.

Capacity refers to logical consumer capacity, not necessarily active OS processes.

A cold resident LogicalAgent may still count as recoverable logical capacity if it can be instantiated when needed.

---

# 17. Pool Reconciliation

A deterministic Pool Reconciler observes:

```text
desired topology
actual logical population
current assignments
agent failures
semantic retirement
topology revisions
```

and mechanically performs allowed lifecycle operations such as:

* birth
* revival
* drain
* retirement
* move
* merge

It must not make semantic planning decisions.

If a partition has insufficient eligible members, the reconciler can create new LogicalAgents according to configured policy.

It does not decide what those agents should think or how their harness operates.

---

# 18. Task Affinity

Root should not choose:

* long-lived vs short-lived physical child
* new vs revived physical runtime
* Codex vs OpenCode process
* particular frontend thread

Root describes which class of consumer should execute the Task.

Conceptually:

```text
consumption_affinity {
    partition
    workstream
    continuity
    affinity_tags
}
```

Possible continuity semantics may include:

```text
required
preferred
none
```

`required` means a compatible LogicalAgent continuity identity is required.

It does not mean the same physical chat thread is required.

`preferred` permits Scheduler degradation according to policy.

`none` allows any compatible consumer.

Lifecycle mechanics remain internal Scheduler decisions.

---

# 19. Ephemeral and Resident Agents

Short-lived and long-lived agents should use the same Task / Attempt / Lease protocol.

Their difference is primarily retention policy.

Example ephemeral lifecycle:

```text
BORN
→ READY
→ ASSIGNED
→ task completes
→ RETIRED
```

Example resident lifecycle:

```text
BORN
→ READY
→ ASSIGNED
→ READY
→ ASSIGNED
→ ...
```

Root should not need a separate API to request an “ephemeral agent” versus a “resident agent”.

It describes task-consumption affinity.

Scheduler allocation and partition policy determine retention behavior.

---

# 20. Pool Reorganization

Root may change the division of labor during a project.

Root must not directly manipulate individual worker processes.

Instead Scheduler exposes a limited topology control surface.

V0.1 direction includes operations conceptually equivalent to:

```text
DEFINE / UPSERT PARTITION
RESIZE PARTITION
MOVE CAPACITY / MEMBERSHIP
MERGE PARTITIONS
RETIRE PARTITION
```

Split may be deferred beyond V0.1.

Topology modifications should be versioned and preferably submitted as an atomic topology revision.

The Scheduler reconciles actual membership to the desired revision.

---

# 21. MOVE Semantics

MOVE changes a LogicalAgent's scheduling classification.

It does not kill and recreate that agent.

A successful MOVE preserves:

* logical_agent_id
* continuity lineage
* checkpoint history
* current durable memory
* physical Incarnation where possible

For BUSY agents, V0.1 should normally apply the move at an assignment/drain boundary.

Current and desired scheduling membership are distinct during that boundary:
`partition_name` records current membership, while `pending_partition_name`
records desired membership. Pool reconciliation and later topology revisions
must compose against desired membership so consecutive MOVE/MERGE operations do
not strand an identity in an inactive partition.

An idle READY agent has no future assignment boundary.  If a MOVE crosses
ExecutionTarget and its previous Incarnation has no active Execution, the
topology transaction fences that old reusable binding as LOST and immediately
moves the same LogicalAgent identity.  It must not create an unassigned,
permanently DRAINING identity.

A topology change must not silently mutate the role contract governing an already-active Attempt.

---

# 22. MERGE Semantics

MERGE combines scheduling partitions.

LogicalAgents retain their identities.

Their private continuity states are not automatically combined.

Scheduler must not produce a synthetic “merged cognition”.

Shared state should be communicated through Workstream/project/checkpoint state.

A merge therefore affects scheduling classification, not agent minds.

For V0.1, MERGE also moves every nonterminal source Task's future scheduling classification to the target. An already-active Attempt keeps its frozen LogicalAgent, ExecutionTarget/Profile, and lease epoch; only later retry/dispatch observes the target partition. Desired target capacity is the sum of the two declared capacities at the topology revision boundary, never a function of instantaneous runtime population.

If an assignment ends in writer-safety suspension, desired membership remains
pending until quiescence or frozen attempt isolation is proven. Resolving that
obligation must commit the canonical destination and its retention policy before
the LogicalAgent becomes schedulable again; every authority-ending path uses
the same cutover invariant.

Lease expiry may intentionally leave the old physical Execution in
STARTING/RUNNING/UNKNOWN for stale-history reconciliation. Once authority has
ended, that row does not prevent logical topology detachment when the work was
read-only, the Execution's frozen snapshot proves attempt isolation, or
quiescence is confirmed. It continues to block a non-isolated writer whose
quiescence is unknown.

This safety predicate belongs to the cutover primitive, not to its caller.
Consequently topology composition is order independent: expiry followed by
MOVE/MERGE and MOVE/MERGE followed by expiry produce the same safe detachment or
the same suspended desired membership.

The same idle cross-target cutover rule applies during MERGE: inactive reusable
presence is fenced, while only an actually ASSIGNED identity uses a pending
drain transition. MERGE rebases pending inbound references to the canonical
target and preserves an already-different desired destination. At an immediate
or assignment-boundary cutover, the LogicalAgent adopts the destination
partition's retention policy. If the destination changes ExecutionTarget, any
terminal/quiescent reusable presence on the old target is fenced before the
membership commit.

---

# 23. Partition Removal and Semantic Death

If a partition is removed without migration:

```text
READY members
→ RETIRED

BUSY members
→ DRAINING
→ RETIRED after the configured boundary
```

This is semantic death.

RETIRE must reject a partition that still has nonterminal Tasks or inbound
desired LogicalAgents. It cannot leave a pending transition pointing at a
retired partition.

A later deficit for a similar role should create newborn agents rather than silently reviving semantically retired ones.

---

# 24. Logical-Agent State vs Physical Presence

Logical lifecycle and physical presence should remain separate dimensions.

Candidate LogicalAgent states:

```text
INITIALIZING
READY
ASSIGNED
REVIVING
DRAINING
SUSPENDED
RETIRED
```

Physical presence may independently be something like:

```text
WARM
COLD
UNKNOWN
```

Examples:

```text
READY + WARM
READY + COLD
ASSIGNED + WARM
ASSIGNED + UNKNOWN
```

A resident agent does not need an idle physical process running forever.

Cold logical continuity is valid.

---

# 25. ExecutionTarget

To support frontend evolution and mixed execution terminals, Scheduler must not bind LogicalAgents directly to Codex or any other frontend.

Introduce a thin execution-boundary concept:

```text
ExecutionTarget
```

An ExecutionTarget means:

> a registered place through which an Execution can be instantiated.

Examples may eventually include:

```text
local_codex
local_opencode
local_cli_worker
```

Scheduler may know only standardized facts such as:

* identifier
* availability
* adapter reference
* minimal correctness-relevant capabilities
* configuration reference

Scheduler does not understand frontend-specific configuration.

---

# 26. LogicalAgent Must Not Bind to ExecutionTarget

Incorrect:

```text
LogicalAgent
  frontend = Codex
```

Correct:

```text
LogicalAgent
  semantic identity
  partition
  workstream
  continuity
```

A physical Incarnation may bind to an ExecutionTarget:

```text
LogicalAgent A

Incarnation 1
→ Codex target

Incarnation 2
→ OpenCode target
```

Such migration is permitted in principle if the target satisfies the required execution contract.

Changing execution terminals must not alter LogicalAgent identity.

---

# 27. ExecutionTarget vs ExecutionProfile

These are orthogonal.

```text
ExecutionTarget
= where/how an execution is hosted

ExecutionProfile
= which opaque model/provider execution profile is requested
```

Example:

```text
ExecutionTarget = local_codex
ExecutionProfile = deepseek_flash_worker
```

The concrete binding may eventually become:

```text
Codex
→ CCR
→ TokenRhythm
→ DeepSeek Flash
```

but Scheduler Core must never resolve that path.

Future valid combinations might include:

```text
local_opencode + deepseek_flash_worker
local_codex + large_reasoner
```

without modifying Task, Lease, Result, LogicalAgent, or Batch semantics.

---

# 28. Minimal Execution Adapter Boundary

The adapter abstraction must remain intentionally lightweight.

A conceptual V0.1 contract is approximately:

```text
start_execution(...)
observe_execution(...)
interrupt_execution(...)
terminate_execution(...)
collect_outcome(...)
reconcile_start(...)
```

These are synchronous Scheduler-facing operations. Every Adapter implementation
must bound each call with its own configured operation deadline, including all
underlying process, transport, and stream I/O, and translate timeout into the
standard outcome vocabulary. The Scheduler does not provide a generic watchdog
for a non-conforming Adapter.

The exact API is not yet frozen beyond this bounded-call correctness contract.

The Adapter receives enough input to instantiate one physical execution.

It does not own:

* task queue
* retry decisions
* leases
* batch semantics
* LogicalAgent lifecycle
* pool reconciliation
* Result Queue
* escalation policy

The Adapter translates Scheduler decisions into frontend mechanics and translates frontend outcomes into standardized Scheduler events.

---

# 29. Adapter Capability Discipline

Do not build a generic frontend capability framework.

Only expose capability differences that affect Scheduler correctness.

Examples that may be relevant:

```text
workspace read/write ability
start reconciliation
termination confirmation
resident incarnation support
```

For example, termination confirmation materially affects safe writer recovery.

Frontend-specific convenience features that do not alter scheduling correctness should remain invisible to Scheduler Core.

---

# 30. Acquisition Model for V0.1

V0.1 should prefer:

```text
Scheduler-owned matching
+
Scheduler-owned claim
+
Scheduler-triggered dispatch
```

Conceptually:

```text
Scheduler selects LogicalAgent
→ creates Attempt + Lease transactionally
→ ensures an Incarnation
→ selects compatible ExecutionTarget
→ invokes ExecutionAdapter
```

There is no need to implement an autonomous worker polling protocol in V0.1.

Future adapters may internally use pull mechanics.

The Scheduler semantic contract remains authoritative regardless of push/pull mechanics.

---

# 31. Writer Recovery

At-least-once execution creates a special problem for shared-workspace writers.

Lease expiration proves:

```text
Scheduler authority has expired.
```

It does not prove:

```text
the old physical writer has stopped mutating files.
```

Therefore fencing protects Scheduler state but cannot alone protect a shared filesystem.

A replacement writer must not be started against the same mutable workspace until the previous writer is positively quiescent, unless stronger attempt isolation exists. Any isolation fact used for this proof belongs to the concrete Execution created under it and must be durably frozen before physical start; current ExecutionTarget configuration cannot retroactively grant or revoke that fact after restart.

Read-only duplicate attempts may be tolerated where appropriate.

Writer retries must inspect current workspace state before continuing because a previous physical execution may have partially completed the assignment.

V0.1 recovery principle:

```text
CURRENT WORKSPACE IS AUTHORITATIVE
```

Future stronger options may include:

* attempt-scoped worktrees
* staged patches
* transactional write workflows

These should not be overimplemented in V0.1 unless required.

---

# 32. Ambiguous Execution Start

An Adapter call may time out after the runtime actually created an execution.

Therefore blindly calling `start_execution()` again may create duplicates.

Where available, adapters should support an execution request identity and start reconciliation.

If start state is ambiguous:

* attempt reconciliation
* attach if execution exists
* confirm absence before safely replacing where possible
* apply stricter rules to writer tasks
* suspend if correctness cannot be mechanically established

An Execution row and a configured Adapter do not themselves prove that the
current daemon supervises that physical work. Lease renewal requires
daemon-owned admission after a bounded start/reconciliation positively confirms
the exact Execution as RUNNING. UNKNOWN/ambiguous reconciliation never rolls
that admission deadline forward; when the existing Lease expires, ordinary
fencing and writer-quiescence rules apply.

A positive RUNNING observation establishes state and renews its Lease in one
fenced transaction. Daemon-owned supervision admission occurs only after that
commit; a separate first heartbeat is not an authority bridge.
Core heartbeat APIs accept only a persisted RUNNING Execution. A late bounded
start that lost authority records its physical state and complete runtime handle
on the exact Execution/Incarnation without admitting supervision or modifying
Task authority; ambiguous and terminal late starts use the same physical-only
path.

The Core should understand standardized ambiguity, not frontend-specific exception text.

---

# 33. Failure Classification

Adapters translate runtime/provider errors into standardized execution outcomes.

Candidate classes may include:

```text
TRANSIENT_EXTERNAL
TIMEOUT
EXECUTION_LOST
START_FAILURE
RESOURCE_UNAVAILABLE
PERMISSION_FAILURE
INVALID_RESULT
ADAPTER_PROTOCOL_FAILURE
UNKNOWN
```

Adapters may provide:

```text
failure_class
failure_code
normalized_failure_signature
retry_hint
```

Scheduler policy operates on these standardized facts.

Supervisor or adapter unavailability is not evidence that a physical Execution
failed or became quiescent. When Task authority must be revoked while physical
termination remains unconfirmed, the Execution stays UNKNOWN and reconcilable.
In particular, a non-isolated writer with unknown quiescence continues to block
cross-target physical detachment until the safety obligation is resolved.

Scheduler must not parse CCR-, Codex-, TokenRhythm-, OpenCode-, or provider-specific error strings.

---

# 34. Escalation

Escalation is a first-class state transition.

When deterministic recovery cannot resolve a Task:

```text
Task → SUSPENDED
```

and an Escalation record is created atomically.

An escalation packet should contain at least:

```text
task_id
logical_agent_id
workstream_id
failure_class
normalized_failure_signature
attempt history summary
last successful checkpoint
current workspace state reference
automatic recovery performed
reason for suspension
decision_required
```

It must not include unnecessary credentials or full sensitive provider logs.

Root then decides whether to:

* retry
* re-plan
* change execution profile/policy
* change task role/affinity
* abort
* perform manual intervention

Scheduler executes the selected primitive.

It does not make the semantic decision.

---

# 35. Root Notification

Root is not expected to poll task queues, heartbeats, leases, or physical worker state.

Scheduler supervises these continuously.

Root should generally be awakened only for low-frequency control events such as:

```text
BATCH_RESULTS_READY
BATCH_SUSPENDED
DECISION_REQUIRED
FATAL_SCHEDULER_CONDITION
```

Root notification is not data transport.

It is a wakeup/control mechanism.

The actual data remains in:

* Result Queue
* Batch state
* Failure records
* Escalation records

Notifications should use a durable outbox so Scheduler crashes do not silently lose wakeups.

At-least-once notification plus event-ID deduplication is sufficient.

Notification delivery should run independently from execution supervision and lease renewal. A notifier owns only outbox delivery state and bounded RootBridge calls; it does not consume Results or assume scheduling authority.

---

# 36. Checkpoints and Continuity Fencing

Checkpoint promotion is authoritative state mutation.

Therefore stale Attempts must not be allowed to promote LogicalAgent memory.

A late physical execution may produce an artifact that is retained for diagnostics.

It must not overwrite the current checkpoint if its lease/attempt/generation is no longer authoritative.

Checkpoint continuity and physical workspace truth must remain distinct:

```text
Checkpoint
= committed semantic summary

Workspace
= authoritative current physical project state
```

Writer recovery must inspect the workspace even when a valid checkpoint exists.

---

# 37. Storage

V0.1 should use:

```text
SQLite + WAL
```

because the initial system is:

* local-first
* single-machine
* transactional
* restart-sensitive
* easier to deploy without external infrastructure

Do not begin V0.1 with:

* Redis
* Kafka
* RabbitMQ
* distributed consensus
* PostgreSQL cluster
* distributed lock service

Candidate core tables include:

```text
tasks
batches

attempts
leases

results
failures
escalations

logical_agents
incarnations

pool_partitions
pool_topology_revisions

executions

workstreams
checkpoints

events / notification_outbox
```

Transactional claim is mandatory.

---

# 38. Configuration Boundaries

Three categories must remain separate.

## 38.1 Persistent Source of Truth

Human-readable, versioned, validated configuration such as:

* pool topology
* partition policies
* retry policies
* target definitions
* execution profile references
* adapter references
* RootBridge configuration

---

## 38.2 Derived Runtime Artifacts

Frontend/provider artifacts generated from persistent configuration, for example:

* Codex configuration
* wrappers
* environment files
* external-model integration artifacts

These should be reproducible and replaceable.

They are not Scheduler runtime state.

---

## 38.3 Runtime State

Dynamic execution state belongs in SQLite:

* task state
* lease state
* Attempts
* Results
* LogicalAgent state
* Incarnations
* runtime handles
* failures
* checkpoints
* batch state
* topology reconciliation state

Runtime state must not be written back into human configuration files.

---

# 39. Process Architecture Direction

V0.1 should remain operationally simple.

A likely shape is:

```text
Scheduler daemon
  ├─ SQLite WAL
  ├─ Scheduler Core
  ├─ Pool Reconciler
  ├─ Dispatcher
  ├─ Result / Escalation handling
  ├─ Notification outbox
  ├─ Adapter registry
  │    └─ Codex Adapter
  └─ RootBridge registry
       └─ Codex RootBridge

CLI
  └─ diagnostics / controlled operations
```

Do not create a generic adapter RPC framework unless actual process isolation requirements justify it.

An adapter may initially be an in-process implementation.

Future isolation into another process should preserve the logical contract.

---

# 40. Scheduler Restart

Scheduler restart must be recovery-aware.

Migration must not invent historical topology intent. If an older topology
operation omitted the declared-capacity facts needed to reconstruct MERGE, or
left nonterminal work on an inactive partition, startup rejects the database
atomically and requires explicit operator repair instead of guessing from the
current agent population.

Normal dispatch should not begin immediately after process startup.

Conceptually:

```text
START
→ RECOVER
→ reconcile Attempts / Leases / Executions
→ READY
→ normal dispatch
```

A lease that expired while Scheduler was offline does not automatically prove an old writer is dead.

Adapter reconciliation and writer safety rules still apply.

---

# 41. Relationship to flash-subagent-helper

`flash-subagent-helper` remains separate.

Its responsibility is external-model enablement for Codex, such as:

```text
Codex
→ CCR
→ external DeepSeek-compatible provider
```

It may supply configuration or runtime environment used by an ExecutionProfile.

It must not own:

* Scheduler queues
* LogicalAgent lifecycle
* lease semantics
* retries
* batches
* Result Queue
* pool topology
* escalations

The Scheduler repository must therefore be independent.

Its README should describe a general-purpose local Agent Scheduler, not “a tool for making Codex call DeepSeek subagents”.

---

# 42. V0.1 Scope

V0.1 should remain small but semantically complete.

Expected scope:

* SQLite WAL
* durable Task Queue
* transactional claim
* Attempt
* Lease
* heartbeat
* ACK/NACK
* Task dependencies
* Result Queue
* Batch
* LogicalAgent
* Incarnation
* ephemeral/resident retention
* PoolPartition
* pool reconciliation
* basic topology revision
* move
* merge
* workstream
* checkpoint
* bounded continuity capsule
* execution generation
* standardized failure classification
* deterministic retry/backoff
* suspend/escalate
* Codex Execution Adapter
* Codex RootBridge
* ExecutionTarget abstraction
* ExecutionProfile reference
* crash-safe notification outbox
* restart reconciliation
* CLI diagnostics
* deterministic configuration
* clean shutdown/recovery

Explicitly deferred:

* distributed scheduler
* multi-node HA
* leader election
* vector database
* scheduler LLM
* AI task decomposition
* web dashboard
* marketplace/plugin framework
* generic workflow language
* autoscaling prediction
* distributed lock service
* complex event bus
* speculative generic frontend abstraction
* partition split unless needed

---

# 43. Core Invariants

The following invariants are the strongest part of this document.

They should survive implementation changes.

### Authority

1. Task state is authoritative. Physical execution state is not authoritative.

2. A physical execution thread/session/process is never the source of truth for Task ownership.

3. A claim does not consume a Task. Only a valid ACK completes it.

4. Worker crash must never silently lose an unacknowledged Task.

5. Scheduler owns deterministic worker supervision. Root does not continuously monitor worker health.

---

### Logical lifecycle

6. LogicalAgent lifetime is Scheduler-owned.

7. Physical execution lifetime is Adapter-owned.

8. Same-thread continuation is never required for correctness.

9. One LogicalAgent has at most one active Task assignment in V0.1.

10. LogicalAgent creation and physical Incarnation creation are distinct events.

11. Physical Incarnation death does not by itself imply LogicalAgent death.

12. Revival preserves LogicalAgent identity and durable continuity.

13. New birth creates a new LogicalAgent identity.

14. A semantically RETIRED LogicalAgent cannot be revived.

---

### Attempt / Lease fencing

15. Every Task execution authority is represented by an Attempt and active Lease/fencing identity.

16. A stale Attempt may update its own physical Execution history but may never mutate authoritative Task state.

17. A stale Attempt may never promote authoritative LogicalAgent checkpoint/memory.

18. Lease expiration revokes scheduling authority but does not prove physical execution termination.

---

### Execution semantics

19. At-least-once execution is assumed.

20. Scheduler must not claim exactly-once execution.

21. Writer recovery must be idempotency-aware.

22. A replacement writer must not run against the same mutable workspace until the previous writer is known to be quiescent, unless stronger attempt isolation exists.

23. Ambiguous runtime start must not be resolved through blind duplicate writer launch.

24. Current workspace state is authoritative for writer recovery.

---

### Results

25. Every authoritative successful Task completion atomically creates exactly one authoritative Result.

26. Task completion and Result consumption are separate lifecycles.

27. Result delivery is durable and independent of root notification.

28. Root notification is a wakeup mechanism, not Result transport.

29. Partial Batch success preserves all committed Results.

30. A completed Task is not reopened. Renewed work creates a new Task with explicit provenance.

---

### Recovery and escalation

31. Scheduler may execute a predeclared deterministic recovery policy.

32. Scheduler must not invent semantic recovery strategy.

33. Persistent, unclassifiable, or policy-exhausted failure becomes SUSPENDED plus Escalation.

34. Root owns semantic replanning and architecture decisions.

35. Scheduler exposes recovery primitives; Root chooses their composition when deterministic policy is insufficient.

36. A suspended Task must have a corresponding open Escalation.

---

### Pool semantics

37. Task submission expresses consumption affinity, not physical lifecycle choice.

38. Pool availability is defined over scheduler-owned logical capacity, not physical process count.

39. Warm runtime continuity is an optimization, not a correctness primitive.

40. Scheduler durable memory must remain bounded and must not become an agent harness or unlimited transcript store.

41. Pool topology operations target partitions/capacity rather than physical worker processes.

42. MOVE and MERGE preserve LogicalAgent identity unless explicit retirement policy says otherwise.

43. Role/partition change of a BUSY LogicalAgent applies at an assignment boundary unless its Attempt is explicitly cancelled.

44. Removing a partition without migration semantically retires its members.

45. Pool reconciliation is deterministic.

---

### Frontend / adapter independence

46. Scheduler Core must not know API keys.

47. Scheduler Core must not know CCR-, TokenRhythm-, DeepSeek-, Codex-, or OpenCode-specific routing semantics.

48. Codex-specific behavior belongs in the Codex Adapter or Codex RootBridge.

49. LogicalAgent identity is independent of ExecutionTarget.

50. Incarnation, not LogicalAgent, binds to a concrete ExecutionTarget.

51. ExecutionAdapter handles physical execution mechanics only.

52. Root wakeup is independent of the worker ExecutionAdapter.

53. ExecutionProfile and ExecutionTarget are orthogonal references.

54. Task affinity describes eligible logical consumers, not frontend choice.

55. Mixed execution terminals must not change Task, Attempt, Lease, Result, LogicalAgent, Batch, or Escalation semantics.

56. Adapter capability exposure is limited to facts required for scheduling correctness.

57. Adding or replacing an execution frontend should require adapter/configuration work, not Scheduler Core state-machine changes.

---

### Batch and notification

58. Batch is a first-class Scheduler object.

59. Batch completion means required Tasks completed and Results durably enqueued; it does not require Root to ACK those Results.

60. Batch suspension stops new claims according to batch policy while preserving already-committed work.

61. Root notifications must be durable Scheduler state/outbox records rather than best-effort side effects.

---

### Storage and restart

62. Dynamic Task/Lease/Execution state belongs in durable runtime storage, not configuration files.

63. Claim must be transactional.

64. Scheduler restart must reconcile existing Attempts, Leases, and physical Executions before normal dispatch.

65. SQLite is an implementation choice for V0.1, not part of the permanent semantic contract.

---

# 44. Architecture Acceptance Test

A useful future test for architectural integrity is:

> Can a second execution frontend such as OpenCode be added alongside Codex without changing the core Task, Attempt, Lease, Result, LogicalAgent, Batch, Pool, and Escalation state machines?

If the answer is no, the adapter boundary has probably leaked frontend mechanics into Scheduler Core.

Similarly:

> Can the current provider/model routing change without changing Scheduler Core semantics?

If the answer is no, provider concerns have leaked into scheduling.

These are stronger architecture tests than whether the first Codex integration is convenient.

---

# 45. Current Architecture Status

At the time of this document, V0.1 is considered architecturally close to closed.

The remaining work should primarily be specification rather than architectural expansion.

Before large-scale coding, derive and freeze:

1. exact state transition matrices
2. SQLite schema
3. transaction boundaries
4. claim/fencing protocol
5. Result Queue protocol
6. pool matching priority
7. pool reconciliation rules
8. MOVE/MERGE drain semantics
9. continuity capsule schema and limits
10. writer recovery/quiescence rules
11. standardized Adapter outcome schema
12. minimal ExecutionAdapter contract
13. minimal RootBridge contract
14. startup reconciliation algorithm
15. configuration schema/versioning
16. minimum CLI diagnostics

Do not use this remaining specification work as an opportunity to introduce unrelated abstractions.

---

# 46. Instruction to Future Agents

When taking over this project:

Do not reinterpret it as a Codex-specific subagent helper.

Do not move provider credentials or model routing into Scheduler Core.

Do not use physical frontend threads as logical worker identities.

Do not make same-thread reuse necessary for continuity.

Do not remove Task lease/fencing semantics for implementation convenience.

Do not collapse Result delivery into frontend messaging.

Do not let Root become the worker health monitor.

Do not let Scheduler become an AI planner.

Do not allow Scheduler to decide semantic recovery when policy does not already determine the answer.

Do not replace bounded logical continuity with an unlimited transcript store.

Do not bind LogicalAgents to one execution frontend.

Do not design a speculative universal adapter framework.

Keep adapters small enough to replace.

Invest architectural complexity in durable Scheduler semantics, not frontend preservation.

---

# 47. Document Maintenance Policy

For the initial repository phase, save this architecture direction as one complete document.

Do not immediately fragment it.

The single-file version provides a common context for implementation agents and reduces the risk that individual specifications lose the original architectural motivation and invariants.

Once V0.1 state machines and interfaces stabilize, this document may remain as the architecture overview while detailed specifications are split into documents such as:

```text
architecture/
  overview.md
  core-invariants.md
  state-machines.md
  pool-lifecycle.md
  task-result-protocol.md
  adapter-contract.md
  recovery-semantics.md
  storage-schema.md
  configuration.md
```

The split documents should derive from this architecture rather than silently redefine it.

If a later design intentionally changes one of the core invariants, update this document explicitly and record the architectural reason.

Until such a revision occurs, this document should be treated as the durable statement of project direction.
