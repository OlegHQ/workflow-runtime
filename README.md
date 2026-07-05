# workflow-runtime

`workflow-runtime` is an OCaml library for backend-backed workflow execution and
observation. It tracks lifecycle state by durable workflow id, tenant, kind,
subject, and metadata, exposes structured snapshots, and provides an atomic
claim/lease API for multi-worker execution.

The API is backend-functorized. The core runtime is storage-agnostic; the
`workflow-runtime.mongo` library supplies a MongoDB backend using `findAndModify`
for atomic multi-instance claims. The `workflow-runtime.eio` library supplies a
reusable Eio worker runner over the same functorized runtime API.

## Guarantees

- Atomic worker claims with expiring leases.
- Worker heartbeats to extend active leases.
- Lease expiry recovery after worker/process crashes.
- Owner-gated workflow mutations require an active, unexpired lease; stale
  workers cannot complete, reschedule, retry, schedule timers, start children,
  complete updates, or heartbeat after lease expiry.
- Terminal completion as `succeeded`, `blocked`, or `failed`.
- Durable cancellation as `cancelled`, including active lease release and a
  replayable `workflow_cancelled` audit event.
- Terminal workflow history repairs missing completion/cancellation events from
  the durable workflow document during history reads.
- Durable reschedule back to `queued`.
- Append-only ordered workflow event history for audit and replay inputs.
- Activity result recording and lookup so retried workflow code can preserve
  completed side effects.
- Activity result recording is idempotent by workflow/activity id and repairs a
  missing activity history event from the durable result record on duplicate
  delivery.
- Kind-filtered claims for task-queue style workers.
- Mongo `ensure` provisions claim indexes for due queued work and expired
  running leases, both globally and by workflow kind.
- Mongo claims use a deterministic due-time plus workflow-id order, matching the
  in-memory backend and avoiding collection natural-order drift.
- Retry policy support with bounded exponential backoff and terminal exhaustion.
- Durable timers that release the worker lease, persist wake-up metadata, and
  append `timer_fired` before the resumed claim.
- Fired timers repair missing `timer_fired` history events from durable timer
  records on later claims.
- Pure deterministic replay over event history for workflow-visible completion,
  timer, activity-result, and signal state.
- Idempotent external workflow signals, keyed by workflow id and signal id.
- Durable update-style request/response messages with pending, completed,
  rejected, and failed states.
- Idempotent signal requests, update requests, and update completions repair
  missing workflow-history events from the durable side-table record on
  duplicate delivery.
- Query APIs that replay workflow-visible state from durable history.
- History compaction into a replayable snapshot event for long-running
  workflows.
- Durable child workflows: any claimed workflow can start normal child
  workflows, children can start their own children, and parent histories replay
  `child_workflow_started` events.
- Duplicate child starts repair missing child-link and parent history records
  when the child workflow record already exists.
- Reusable Eio worker runner for polling, kind-filtered claims, handler
  execution, heartbeat-based lease extension, durable completion, reschedule,
  retry, and handler-failure recording.
- Worker-runner configuration validates lease and heartbeat timings, and
  heartbeat loss stops the handler fiber before stale work can complete.
- Core and Mongo backends reject invalid workflows, worker ids, lease durations,
  and retry policies before mutating durable state.
- Tenant-filtered and tenant-grouped snapshots.
- Core runtime has no dependency on Eio, Dream, Mongo, or application domain
  types.
- Mongo backend uses BSON DTOs generated with `bson.ppx`.

## Temporal Comparison

Temporal is more than a distributed status table. Its reliability comes from a
durable Event History, task queues, workflow replay, activity result
preservation, durable timers, retries, timeouts, and worker polling. This
library implements a smaller reusable foundation: durable state, atomic claims,
task-queue filtering, leases, heartbeats, recovery, ordered event history,
deterministic replay, activity-result preservation, durable timers, retry
backoff, idempotent signals, durable updates, query-state replay, cancellation,
nested child workflows, history compaction, a reusable Eio worker runner, and
visibility.

Temporal still has a broader production platform surface: dedicated frontend,
history, matching, and worker services; mature SDK workflow runners; advanced
visibility; archival; multi-cluster operation; and managed cloud options. Use
this library when Poster needs an embedded OCaml workflow foundation over its
existing Mongo deployment. Use Temporal when the system needs the full external
orchestration platform.

## Minimal Example

```ocaml
let workflow =
  Workflow_runtime.
    {
      id = "publish_123";
      tenant_id = "user_123";
      kind = "publish_attempt";
      subject_id = Some "post_123";
      name = Some "personal_blog";
      metadata = [ ("destination", "personal_blog") ];
    }

module Runtime =
  Workflow_runtime.Make
    (struct
      let now_ms () = Int64.of_float (Unix.gettimeofday () *. 1000.)
    end)
    (Workflow_runtime.Memory_backend)

module Runner = Workflow_runtime_eio.Make (Runtime)

let backend = Workflow_runtime.Memory_backend.create ()

let () =
  Runtime.enqueue backend workflow (Workflow_runtime.enqueue_options ()) |> ignore

let run_worker env =
  let config =
    Workflow_runtime_eio.config ~kind:"publish_attempt" ~worker_id:"worker-1" ()
  in
  Runner.run_forever ~clock:(Eio.Stdenv.clock env) backend config
    (fun claim ->
      (* Do idempotent side effects, using activity results for replay safety. *)
      Workflow_runtime_eio.Complete
        {
          status = Workflow_runtime.Succeeded;
          message = "published " ^ claim.item.workflow.id;
        })
```

## Development

```bash
opam install . --deps-only --with-test
opam exec -- dune build @all
opam exec -- dune runtest
```
