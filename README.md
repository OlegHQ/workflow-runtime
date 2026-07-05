# workflow-runtime

`workflow-runtime` is an OCaml library for backend-backed workflow execution and
observation. It tracks lifecycle state by durable workflow id, tenant, kind,
subject, and metadata, exposes structured snapshots, and provides an atomic
claim/lease API for multi-worker execution.

The API is backend-functorized. The core runtime is storage-agnostic; the
`workflow-runtime.mongo` library supplies a MongoDB backend using `findAndModify`
for atomic multi-instance claims.

## Guarantees

- Atomic worker claims with expiring leases.
- Worker heartbeats to extend active leases.
- Lease expiry recovery after worker/process crashes.
- Terminal completion as `succeeded`, `blocked`, or `failed`.
- Durable reschedule back to `queued`.
- Append-only ordered workflow event history for audit and replay inputs.
- Activity result recording and lookup so retried workflow code can preserve
  completed side effects.
- Kind-filtered claims for task-queue style workers.
- Retry policy support with bounded exponential backoff and terminal exhaustion.
- Durable timers that release the worker lease, persist wake-up metadata, and
  append `timer_fired` before the resumed claim.
- Pure deterministic replay over event history for workflow-visible completion,
  timer, activity-result, and signal state.
- Idempotent external workflow signals, keyed by workflow id and signal id.
- Query APIs that replay workflow-visible state from durable history.
- History compaction into a replayable snapshot event for long-running
  workflows.
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
backoff, idempotent signals, query-state replay, history compaction, and
visibility.

Temporal still has a broader production platform surface: dedicated frontend,
history, matching, and worker services; mature SDK workflow runners; updates;
cancellation; child workflows; advanced visibility; archival; multi-cluster
operation; and managed cloud options. Use this library when Poster needs an
embedded OCaml workflow foundation over its existing Mongo deployment. Use
Temporal when the system needs the full external orchestration platform.

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

let backend = Workflow_runtime.Memory_backend.create ()

let () =
  Runtime.enqueue backend workflow (Workflow_runtime.enqueue_options ()) |> ignore;
  match Runtime.claim_next backend ~worker_id:"worker-1" ~lease_ms:30_000L with
  | Ok (Some claim) ->
      Runtime.complete backend ~workflow_id:claim.item.workflow.id
        ~worker_id:"worker-1" ~status:Succeeded ~message:"published"
      |> ignore
  | Ok None | Error _ -> ()
```

## Development

```bash
opam install . --deps-only --with-test
opam exec -- dune build @all
opam exec -- dune runtest
```
