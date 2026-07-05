# workflow-runtime

`workflow-runtime` is a small OCaml library for observing in-process workflow
execution. It tracks lifecycle state by durable workflow id, tenant, kind,
subject, and metadata, then exposes structured, flat JSON, and tenant-grouped
JSON snapshots.

It is deliberately not a durable Temporal replacement. Keep the source of truth
in a database, queue, or log, and use this runtime as the single-process
execution/observability layer around those durable records.

## Guarantees

- Thread-safe lifecycle updates.
- Bounded retention through `max_items`.
- Oldest terminal workflows are evicted first.
- Active `queued` and `running` workflows are kept visible.
- Tenant-filtered and tenant-grouped snapshots.
- No dependency on Eio, Dream, Mongo, or any application domain types.

## Minimal Example

```ocaml
let runtime =
  Workflow_runtime.create
    ~clock:(fun () -> Int64.of_float (Unix.gettimeofday () *. 1000.))
    ()

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

let () =
  Workflow_runtime.record_queued runtime workflow;
  Workflow_runtime.record_running runtime workflow;
  Workflow_runtime.record_succeeded runtime workflow "published"
```

## Development

```bash
opam install . --deps-only --with-test
opam exec -- dune build @all
opam exec -- dune runtest
```
