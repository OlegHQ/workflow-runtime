let workflow ?(kind = "publish_attempt") id =
  Workflow_runtime.
    {
      id;
      tenant_id = "tenant_a";
      kind;
      subject_id = Some ("post_" ^ id);
      name = Some "personal_blog";
      metadata = [ ("destination", "personal_blog") ];
    }

let expect_ok label = function
  | Ok value -> value
  | Error error ->
      Alcotest.fail
        (label ^ ": " ^ Workflow_runtime.Memory_backend.error_to_string error)

let runtime now =
  let module Runtime =
    Workflow_runtime.Make
      (struct
        let now_ms () = !now
      end)
      (Workflow_runtime.Memory_backend)
  in
  (module Runtime :
    Workflow_runtime.S
      with type backend = Workflow_runtime.Memory_backend.t
       and type error = Workflow_runtime.Memory_backend.error)

let test_runner_completes_workflow () =
  let now = ref 1_000L in
  let module Runtime = (val runtime now) in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_complete")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue";
  Eio_main.run @@ fun env ->
  let config =
    Workflow_runtime_eio.config ~worker_id:"runner_a"
      ~heartbeat_interval_ms:0L ()
  in
  let result =
    Runner.run_once ~clock:(Eio.Stdenv.clock env) backend config
      (fun (_claim : Workflow_runtime.claim) ->
        Workflow_runtime_eio.Complete
          { status = Workflow_runtime.Succeeded; message = "published" })
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "completed result" true
    (match result with
    | Workflow_runtime_eio.Completed { workflow_id; status } ->
        String.equal workflow_id "wf_complete"
        && status = Workflow_runtime.Succeeded
    | _ -> false);
  let item =
    Runtime.snapshot backend |> expect_ok "snapshot" |> List.hd
  in
  Alcotest.(check string) "succeeded" "succeeded"
    (Workflow_runtime.status_to_string item.status)

let test_runner_records_handler_failure () =
  let now = ref 2_000L in
  let module Runtime = (val runtime now) in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_fail")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue";
  Eio_main.run @@ fun env ->
  let config =
    Workflow_runtime_eio.config ~worker_id:"runner_a"
      ~heartbeat_interval_ms:0L ()
  in
  let result =
    Runner.run_once ~clock:(Eio.Stdenv.clock env) backend config
      (fun (_claim : Workflow_runtime.claim) -> failwith "publisher crashed")
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "handler failed result" true
    (match result with
    | Workflow_runtime_eio.Handler_failed { workflow_id; error } ->
        String.equal workflow_id "wf_fail"
        && String.contains error 'p'
    | _ -> false);
  let item =
    Runtime.snapshot backend |> expect_ok "snapshot" |> List.hd
  in
  Alcotest.(check string) "failed" "failed"
    (Workflow_runtime.status_to_string item.status)

let test_runner_reschedules_and_filters_by_kind () =
  let now = ref 3_000L in
  let module Runtime = (val runtime now) in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow ~kind:"email" "wf_email")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue email";
  Runtime.enqueue backend (workflow ~kind:"publish" "wf_publish")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue publish";
  Eio_main.run @@ fun env ->
  let config =
    Workflow_runtime_eio.config ~kind:"publish" ~worker_id:"runner_a"
      ~heartbeat_interval_ms:0L ()
  in
  let result =
    Runner.run_once ~clock:(Eio.Stdenv.clock env) backend config
      (fun claim ->
        Alcotest.(check string) "filtered kind" "publish"
          claim.Workflow_runtime.item.workflow.kind;
        Workflow_runtime_eio.Reschedule
          { run_at_ms = 10_000L; message = "waiting for publish window" })
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "rescheduled result" true
    (match result with
    | Workflow_runtime_eio.Rescheduled { workflow_id; run_at_ms } ->
        String.equal workflow_id "wf_publish" && run_at_ms = 10_000L
    | _ -> false);
  let snapshot = Runtime.snapshot backend |> expect_ok "snapshot" in
  let publish =
    snapshot
    |> List.find (fun item ->
           String.equal item.Workflow_runtime.workflow.id "wf_publish")
  in
  Alcotest.(check string) "publish queued" "queued"
    (Workflow_runtime.status_to_string publish.status);
  Alcotest.(check int64) "publish run_at" 10_000L publish.run_at_ms

let test_runner_retry_exhaustion () =
  let now = ref 4_000L in
  let module Runtime = (val runtime now) in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_retry")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue";
  Eio_main.run @@ fun env ->
  let config =
    Workflow_runtime_eio.config ~worker_id:"runner_a"
      ~heartbeat_interval_ms:0L ()
  in
  let policy =
    Workflow_runtime.retry_policy ~max_attempts:1
      ~initial_backoff_ms:100L ()
  in
  let result =
    Runner.run_once ~clock:(Eio.Stdenv.clock env) backend config
      (fun (_claim : Workflow_runtime.claim) ->
        Workflow_runtime_eio.Retry { policy; message = "external 503" })
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "retry exhausted" true
    (match result with
    | Workflow_runtime_eio.Retry_exhausted { workflow_id; attempt } ->
        String.equal workflow_id "wf_retry" && attempt = 1
    | _ -> false);
  let item =
    Runtime.snapshot backend |> expect_ok "snapshot" |> List.hd
  in
  Alcotest.(check string) "failed" "failed"
    (Workflow_runtime.status_to_string item.status)

let test_runner_heartbeats_during_handler () =
  let now = ref 5_000L in
  let module Runtime =
    Workflow_runtime.Make
      (struct
        let now_ms () =
          let value = !now in
          now := Int64.add value 5L;
          value
      end)
      (Workflow_runtime.Memory_backend)
  in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_heartbeat")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue";
  Eio_main.run @@ fun env ->
  let clock = Eio.Stdenv.clock env in
  let config =
    Workflow_runtime_eio.config ~worker_id:"runner_a" ~lease_ms:100L
      ~heartbeat_interval_ms:1L ()
  in
  let result =
    Runner.run_once ~clock backend config
      (fun (_claim : Workflow_runtime.claim) ->
        Eio.Time.sleep clock 0.01;
        Workflow_runtime_eio.Complete
          { status = Workflow_runtime.Succeeded; message = "published" })
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "completed" true
    (match result with
    | Workflow_runtime_eio.Completed { workflow_id; status } ->
        String.equal workflow_id "wf_heartbeat"
        && status = Workflow_runtime.Succeeded
    | _ -> false);
  let history =
    Runtime.history backend ~workflow_id:"wf_heartbeat"
    |> expect_ok "history"
  in
  let heartbeat_count =
    history
    |> List.filter (fun event ->
           event.Workflow_runtime.kind = Workflow_runtime.Workflow_heartbeat)
    |> List.length
  in
  Alcotest.(check bool) "heartbeat recorded" true (heartbeat_count > 0)

let test_config_rejects_invalid_values () =
  Alcotest.check_raises "empty worker id"
    (Invalid_argument "worker_id must not be empty")
    (fun () ->
      ignore (Workflow_runtime_eio.config ~worker_id:"" ()));
  Alcotest.check_raises "bad lease"
    (Invalid_argument "lease_ms must be positive")
    (fun () ->
      ignore (Workflow_runtime_eio.config ~worker_id:"runner" ~lease_ms:0L ()));
  Alcotest.check_raises "bad poll interval"
    (Invalid_argument "poll_interval_ms must be positive")
    (fun () ->
      ignore
        (Workflow_runtime_eio.config ~worker_id:"runner" ~poll_interval_ms:0L ()));
  Alcotest.check_raises "bad heartbeat interval"
    (Invalid_argument "heartbeat_interval_ms must be zero or positive")
    (fun () ->
      ignore
        (Workflow_runtime_eio.config ~worker_id:"runner"
           ~heartbeat_interval_ms:(-1L) ()));
  Alcotest.check_raises "heartbeat must fit in lease"
    (Invalid_argument "heartbeat_interval_ms must be shorter than lease_ms")
    (fun () ->
      ignore
        (Workflow_runtime_eio.config ~worker_id:"runner" ~lease_ms:100L
           ~heartbeat_interval_ms:100L ()))

let test_runner_stops_when_heartbeat_loses_lease () =
  let now = ref 6_000L in
  let module Runtime =
    Workflow_runtime.Make
      (struct
        let now_ms () =
          let value = !now in
          now := Int64.add value 200L;
          value
      end)
      (Workflow_runtime.Memory_backend)
  in
  let module Runner = Workflow_runtime_eio.Make (Runtime) in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_lost_lease")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue";
  Eio_main.run @@ fun env ->
  let clock = Eio.Stdenv.clock env in
  let config =
    Workflow_runtime_eio.config ~worker_id:"runner_a" ~lease_ms:100L
      ~heartbeat_interval_ms:1L ()
  in
  let handler_finished = ref false in
  let result =
    Runner.run_once ~clock backend config
      (fun (_claim : Workflow_runtime.claim) ->
        Eio.Time.sleep clock 0.05;
        handler_finished := true;
        Workflow_runtime_eio.Complete
          { status = Workflow_runtime.Succeeded; message = "too late" })
    |> expect_ok "run once"
  in
  Alcotest.(check bool) "lease lost result" true
    (match result with
    | Workflow_runtime_eio.Lease_lost { workflow_id } ->
        String.equal workflow_id "wf_lost_lease"
    | _ -> false);
  Alcotest.(check bool) "handler cancelled before side effect" false
    !handler_finished;
  Runtime.claim_workflow backend ~workflow_id:"wf_lost_lease"
    ~worker_id:"runner_b" ~lease_ms:100L
  |> expect_ok "reclaim"
  |> Option.is_some
  |> Alcotest.(check bool) "reclaimed after lost lease" true

let () =
  Alcotest.run "workflow_runtime_eio"
    [
      ( "runner",
        [
          Alcotest.test_case "completes workflow" `Quick
            test_runner_completes_workflow;
          Alcotest.test_case "records handler failure" `Quick
            test_runner_records_handler_failure;
          Alcotest.test_case "reschedules and filters by kind" `Quick
            test_runner_reschedules_and_filters_by_kind;
          Alcotest.test_case "retry exhaustion" `Quick
            test_runner_retry_exhaustion;
          Alcotest.test_case "heartbeats during handler" `Quick
            test_runner_heartbeats_during_handler;
          Alcotest.test_case "rejects invalid config" `Quick
            test_config_rejects_invalid_values;
          Alcotest.test_case "stops when heartbeat loses lease" `Quick
            test_runner_stops_when_heartbeat_loses_lease;
        ] );
    ]
