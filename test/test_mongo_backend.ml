let env name default =
  match Sys.getenv_opt name with Some "" | None -> default | Some value -> value

let host = env "POSTER_MONGO_HOST" "oracle-vm"
let port = env "POSTER_MONGO_PORT" "27017" |> int_of_string

let db =
  Printf.sprintf "workflow_runtime_e2e_%d_%d" (Unix.getpid ()) (Random.bits ())

let collection = "workflows"

let workflow id =
  Workflow_runtime.
    {
      id;
      tenant_id = "tenant_a";
      kind = "integration";
      subject_id = Some ("subject_" ^ id);
      name = Some "mongo";
      metadata = [ ("test", "mongo_backend") ];
    }

let workflow_kind id kind =
  Workflow_runtime.{ (workflow id) with kind }

let expect_ok label = function
  | Ok value -> value
  | Error error ->
      Alcotest.fail (label ^ ": " ^ Workflow_runtime_mongo.error_to_string error)

let test_mongo_claims_and_lease_recovery () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let config =
    Mongo_config.
      {
        (default ~host ~port ~database:db ()) with
        direct_connection = true;
        server_selection_timeout_ms = 2_000;
        connect_timeout_ms = 2_000;
        socket_timeout_ms = Some 5_000;
        app_name = Some "workflow-runtime-e2e";
      }
  in
  let client =
    match
      Mongo_eio.connect ~sw ~net:(Eio.Stdenv.net env)
        ~clock:(Eio.Stdenv.clock env) ~config
    with
    | Ok client -> client
    | Error error ->
        Alcotest.fail ("connect: " ^ Mongo_error.to_string error)
  in
  Fun.protect
    ~finally:(fun () ->
      let _ =
        Mongo_eio.direct_run_command client db
          [ ("dropDatabase", Bson.create_int32 1l) ]
      in
      Mongo_eio.close_direct client)
    (fun () ->
      let backend_a =
        Workflow_runtime_mongo.create ~client ~db ~collection ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:1_000L (workflow "wf_1")
        (Workflow_runtime.enqueue_options ~run_at_ms:1_000L ())
      |> expect_ok "enqueue wf_1";
      let first =
        Workflow_runtime_mongo.claim_next backend_a ~worker_id:"worker_a"
          ~now_ms:1_000L ~lease_ms:100L
        |> expect_ok "claim worker_a"
        |> Option.get
      in
      Alcotest.(check string) "first worker" "worker_a" first.worker_id;
      let second =
        Workflow_runtime_mongo.claim_next backend_b ~worker_id:"worker_b"
          ~now_ms:1_000L ~lease_ms:100L
        |> expect_ok "claim worker_b"
      in
      Alcotest.(check bool) "not double claimed" true (Option.is_none second);
      let recovered =
        Workflow_runtime_mongo.claim_workflow backend_b ~workflow_id:"wf_1"
          ~worker_id:"worker_b"
          ~now_ms:1_101L ~lease_ms:100L
        |> expect_ok "claim expired"
        |> Option.get
      in
      Alcotest.(check string) "recovery worker" "worker_b" recovered.worker_id;
      Alcotest.(check int) "attempt incremented" 2 recovered.item.attempt;
      Workflow_runtime_mongo.complete backend_b ~workflow_id:"wf_1"
        ~worker_id:"worker_b" ~now_ms:1_102L ~status:Succeeded ~message:"done"
      |> expect_ok "complete"
      |> Alcotest.(check bool) "completed" true;
      let snapshot =
        Workflow_runtime_mongo.snapshot backend_a |> expect_ok "snapshot"
      in
      Alcotest.(check int) "one workflow" 1 (List.length snapshot);
      Alcotest.(check string)
        "succeeded" "succeeded"
        (Workflow_runtime.status_to_string (List.hd snapshot).status);
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_1" backend_a
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "history"
        [
          "workflow_enqueued";
          "workflow_claimed";
          "workflow_claimed";
          "workflow_completed";
        ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history);
      Alcotest.(check (list int))
        "sequence"
        [ 1; 2; 3; 4 ]
        (List.map (fun event -> event.Workflow_runtime.sequence) history))

let test_mongo_activity_results_survive_backend_instances () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let config =
    Mongo_config.
      {
        (default ~host ~port ~database:db ()) with
        direct_connection = true;
        server_selection_timeout_ms = 2_000;
        connect_timeout_ms = 2_000;
        socket_timeout_ms = Some 5_000;
        app_name = Some "workflow-runtime-e2e";
      }
  in
  let client =
    match
      Mongo_eio.connect ~sw ~net:(Eio.Stdenv.net env)
        ~clock:(Eio.Stdenv.clock env) ~config
    with
    | Ok client -> client
    | Error error ->
        Alcotest.fail ("connect: " ^ Mongo_error.to_string error)
  in
  Fun.protect
    ~finally:(fun () ->
      let _ =
        Mongo_eio.direct_run_command client db
          [ ("dropDatabase", Bson.create_int32 1l) ]
      in
      Mongo_eio.close_direct client)
    (fun () ->
      let backend_a =
        Workflow_runtime_mongo.create ~client ~db ~collection:"activity_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"activity_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:2_000L (workflow "wf_activity")
        (Workflow_runtime.enqueue_options ~run_at_ms:2_000L ())
      |> expect_ok "enqueue";
      let claim =
        Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_activity"
          ~worker_id:"worker_a" ~now_ms:2_001L ~lease_ms:1_000L
        |> expect_ok "claim"
        |> Option.get
      in
      Alcotest.(check string) "claimed" "wf_activity" claim.item.workflow.id;
      Workflow_runtime_mongo.record_activity_result backend_a ~now_ms:2_002L
        Workflow_runtime.
          {
            activity_id = "publish_call";
            workflow_id = "wf_activity";
            name = "Publish call";
            attempt = 1;
            status = Activity_succeeded;
            result_json = Some {|{"external_id":"123"}|};
            error = None;
            updated_at_ms = 0L;
          }
      |> expect_ok "record activity";
      let found =
        Workflow_runtime_mongo.find_activity_result backend_b
          ~workflow_id:"wf_activity" ~activity_id:"publish_call"
        |> expect_ok "find activity"
        |> Option.get
      in
      Alcotest.(check (option string))
        "result survives backend instance" (Some {|{"external_id":"123"}|})
        found.result_json;
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_activity" backend_b
        |> expect_ok "history"
      in
      Alcotest.(check string)
        "activity event" "activity_completed"
        (history |> List.rev |> List.hd |> fun event ->
         Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind))

let test_mongo_kind_claim_filter_and_retry_policy () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let config =
    Mongo_config.
      {
        (default ~host ~port ~database:db ()) with
        direct_connection = true;
        server_selection_timeout_ms = 2_000;
        connect_timeout_ms = 2_000;
        socket_timeout_ms = Some 5_000;
        app_name = Some "workflow-runtime-e2e";
      }
  in
  let client =
    match
      Mongo_eio.connect ~sw ~net:(Eio.Stdenv.net env)
        ~clock:(Eio.Stdenv.clock env) ~config
    with
    | Ok client -> client
    | Error error ->
        Alcotest.fail ("connect: " ^ Mongo_error.to_string error)
  in
  Fun.protect
    ~finally:(fun () ->
      let _ =
        Mongo_eio.direct_run_command client db
          [ ("dropDatabase", Bson.create_int32 1l) ]
      in
      Mongo_eio.close_direct client)
    (fun () ->
      let backend_a =
        Workflow_runtime_mongo.create ~client ~db ~collection:"retry_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"retry_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "durable" true capabilities.durable;
      Alcotest.(check bool) "retry backoff" true capabilities.retry_backoff;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:3_000L
        (workflow_kind "wf_publish" "publish_attempt")
        (Workflow_runtime.enqueue_options ~run_at_ms:3_000L ())
      |> expect_ok "enqueue publish";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:3_000L
        (workflow_kind "wf_email" "send_email")
        (Workflow_runtime.enqueue_options ~run_at_ms:3_000L ())
      |> expect_ok "enqueue email";
      let claim =
        Workflow_runtime_mongo.claim_next backend_b ~kind:"send_email"
          ~worker_id:"email-worker" ~now_ms:3_000L ~lease_ms:10_000L
        |> expect_ok "claim kind"
        |> Option.get
      in
      Alcotest.(check string) "claimed kind" "send_email"
        claim.item.workflow.kind;
      let policy =
        Workflow_runtime.retry_policy ~max_attempts:2 ~initial_backoff_ms:500L
          ~max_backoff_ms:1_000L ()
      in
      let retry =
        Workflow_runtime_mongo.retry backend_b ~workflow_id:"wf_email"
          ~worker_id:"email-worker" ~now_ms:3_001L ~policy
          ~message:"smtp timeout"
        |> expect_ok "retry"
        |> Option.get
      in
      let run_at_ms =
        match retry with
        | Workflow_runtime.Retried { attempt; run_at_ms } ->
            Alcotest.(check int) "retry attempt" 1 attempt;
            run_at_ms
        | Retries_exhausted _ -> Alcotest.fail "expected retry"
      in
      Alcotest.(check int64) "retry run_at" 3_501L run_at_ms;
      let retry_claim =
        Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_email"
          ~worker_id:"email-worker" ~now_ms:run_at_ms ~lease_ms:10_000L
        |> expect_ok "claim retry"
        |> Option.get
      in
      Alcotest.(check int) "second attempt" 2 retry_claim.item.attempt;
      let exhausted =
        Workflow_runtime_mongo.retry backend_a ~workflow_id:"wf_email"
          ~worker_id:"email-worker" ~now_ms:(Int64.add run_at_ms 1L) ~policy
          ~message:"smtp still down"
        |> expect_ok "retry exhausted"
        |> Option.get
      in
      (match exhausted with
      | Workflow_runtime.Retries_exhausted { attempt } ->
          Alcotest.(check int) "exhausted attempt" 2 attempt
      | Retried _ -> Alcotest.fail "expected exhausted retries");
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_email" backend_b
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "retry history"
        [
          "workflow_enqueued";
          "workflow_claimed";
          "workflow_rescheduled";
          "workflow_claimed";
          "workflow_completed";
        ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history))

let () =
  Mirage_crypto_rng_unix.use_default ();
  Alcotest.run "workflow-runtime-mongo"
    [
      ( "mongo",
        [
          Alcotest.test_case "claims and lease recovery" `Quick
            test_mongo_claims_and_lease_recovery;
          Alcotest.test_case "activity results survive backend instances" `Quick
            test_mongo_activity_results_survive_backend_instances;
          Alcotest.test_case "kind claim filter and retry policy" `Quick
            test_mongo_kind_claim_filter_and_retry_policy;
        ] );
    ]
