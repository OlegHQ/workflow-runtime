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

let expect_invalid_workflow label expected = function
  | Error (`Invalid_workflow message) -> Alcotest.(check string) label expected message
  | Ok _ -> Alcotest.fail (label ^ ": expected invalid workflow")
  | Error error ->
      Alcotest.fail (label ^ ": " ^ Workflow_runtime_mongo.error_to_string error)

let expect_invalid_transition label expected = function
  | Error (`Invalid_transition message) ->
      Alcotest.(check string) label expected message
  | Ok _ -> Alcotest.fail (label ^ ": expected invalid transition")
  | Error error ->
      Alcotest.fail (label ^ ": " ^ Workflow_runtime_mongo.error_to_string error)

let bson_doc fields =
  List.fold_right (fun (name, element) acc -> Bson.add_element name element acc)
    fields Bson.empty

let bson_string name value = (name, Bson.create_string value)

let bson_int64 name value = (name, Bson.create_int64 value)

let bson_doc_element name value = (name, Bson.create_doc_element value)

let cursor_batch name doc =
  let cursor = Bson.get_doc_element (Bson.get_element "cursor" doc) in
  Bson.get_list (Bson.get_element name cursor) |> List.map Bson.get_doc_element

let index_names client ~db ~collection =
  Mongo_eio.direct_run_command client db
    [ ("listIndexes", Bson.create_string collection) ]
  |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
  |> expect_ok "list indexes"
  |> fun (response : Mongo_command.response) ->
  cursor_batch "firstBatch" response.body
  |> List.map (fun index -> Bson.get_string (Bson.get_element "name" index))

let test_mongo_validation () =
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
      let backend =
        Workflow_runtime_mongo.create ~client ~db ~collection:"validation_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend ~now_ms:1L
        Workflow_runtime.
          {
            id = "";
            tenant_id = "tenant";
            kind = "kind";
            subject_id = None;
            name = None;
            metadata = [];
          }
        (Workflow_runtime.enqueue_options ())
      |> expect_invalid_workflow "invalid workflow"
           "workflow id must not be empty";
      Workflow_runtime_mongo.enqueue backend ~now_ms:1L (workflow "valid_wf")
        (Workflow_runtime.enqueue_options ())
      |> expect_ok "enqueue valid";
      Workflow_runtime_mongo.claim_next backend ~worker_id:"" ~now_ms:1L
        ~lease_ms:100L
      |> expect_invalid_transition "empty worker id"
           "worker_id must not be empty";
      Workflow_runtime_mongo.claim_workflow backend ~workflow_id:"valid_wf"
        ~worker_id:"worker_a" ~now_ms:1L ~lease_ms:0L
      |> expect_invalid_transition "bad lease" "lease_ms must be positive";
      let invalid_policy : Workflow_runtime.retry_policy =
        {
          max_attempts = 0;
          initial_backoff_ms = 0L;
          max_backoff_ms = 0L;
          backoff_multiplier = 1.0;
        }
      in
      Workflow_runtime_mongo.retry backend ~workflow_id:"valid_wf"
        ~worker_id:"worker_a" ~now_ms:1L ~policy:invalid_policy
        ~message:"bad policy"
      |> expect_invalid_transition "bad retry policy"
           "retry max_attempts must be positive")

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
      let indexes = index_names client ~db ~collection in
      Alcotest.(check bool)
        "due claim index" true (List.mem "workflow_due_idx" indexes);
      Alcotest.(check bool)
        "due claim order index" true
        (List.mem "workflow_due_order_idx" indexes);
      Alcotest.(check bool)
        "kind due claim index" true
        (List.mem "workflow_kind_due_idx" indexes);
      Alcotest.(check bool)
        "kind due claim order index" true
        (List.mem "workflow_kind_due_order_idx" indexes);
      Alcotest.(check bool)
        "expired lease claim index" true
        (List.mem "workflow_expired_lease_idx" indexes);
      Alcotest.(check bool)
        "kind expired lease claim index" true
        (List.mem "workflow_kind_expired_lease_idx" indexes);
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
      Mongo_eio.direct_delete_many client ~db ~collection:"workflows_events"
        (bson_doc
           [
             bson_string "workflow_id" "wf_1";
             bson_string "kind" "workflow_completed";
           ])
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete completed event"
      |> ignore;
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
        (List.map (fun event -> event.Workflow_runtime.sequence) history);
      let replay =
        Workflow_runtime.replay history
        |> Result.fold ~ok:Fun.id ~error:(fun message -> Alcotest.fail message)
      in
      Alcotest.(check int) "replay claims" 2 replay.claim_count;
      let completion = replay.Workflow_runtime.completion |> Option.get in
      Alcotest.(check string) "replay completion" "succeeded"
        (Workflow_runtime.status_to_string completion.status))

let test_mongo_claims_same_due_time_by_workflow_id () =
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
      let backend =
        Workflow_runtime_mongo.create ~client ~db ~collection:"ordered_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend ~now_ms:1_000L (workflow "wf_b")
        (Workflow_runtime.enqueue_options ~run_at_ms:2_000L ())
      |> expect_ok "enqueue wf_b";
      Workflow_runtime_mongo.enqueue backend ~now_ms:1_000L (workflow "wf_a")
        (Workflow_runtime.enqueue_options ~run_at_ms:2_000L ())
      |> expect_ok "enqueue wf_a";
      let first =
        Workflow_runtime_mongo.claim_next backend ~worker_id:"worker_a"
          ~now_ms:2_000L ~lease_ms:100L
        |> expect_ok "claim first"
        |> Option.get
      in
      Alcotest.(check string)
        "same due time uses workflow id tie breaker" "wf_a"
        first.item.workflow.id;
      let second =
        Workflow_runtime_mongo.claim_next backend ~worker_id:"worker_a"
          ~now_ms:2_000L ~lease_ms:100L
        |> expect_ok "claim second"
        |> Option.get
      in
      Alcotest.(check string) "second workflow" "wf_b" second.item.workflow.id)

let test_mongo_owned_operations_require_active_lease () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"lease_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"lease_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:1_000L
        (workflow "wf_expired")
        (Workflow_runtime.enqueue_options ~run_at_ms:1_000L ())
      |> expect_ok "enqueue";
      Workflow_runtime_mongo.request_update backend_a ~workflow_id:"wf_expired"
        ~now_ms:1_001L ~update_id:"upd_1" ~name:"set_target" ()
      |> expect_ok "request update"
      |> Alcotest.(check bool) "update requested" true;
      let _claim =
        Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_expired"
          ~worker_id:"worker_a" ~now_ms:1_002L ~lease_ms:100L
        |> expect_ok "claim"
        |> Option.get
      in
      Workflow_runtime_mongo.heartbeat backend_a ~workflow_id:"wf_expired"
        ~worker_id:"worker_a" ~now_ms:1_103L ~lease_ms:100L
      |> expect_ok "stale heartbeat"
      |> Alcotest.(check bool) "stale heartbeat rejected" false;
      Workflow_runtime_mongo.complete backend_a ~workflow_id:"wf_expired"
        ~worker_id:"worker_a" ~now_ms:1_103L ~status:Succeeded
        ~message:"too late"
      |> expect_ok "stale complete"
      |> Alcotest.(check bool) "stale complete rejected" false;
      Workflow_runtime_mongo.reschedule backend_a ~workflow_id:"wf_expired"
        ~worker_id:"worker_a" ~now_ms:1_103L ~run_at_ms:2_000L
        ~message:"too late"
      |> expect_ok "stale reschedule"
      |> Alcotest.(check bool) "stale reschedule rejected" false;
      Workflow_runtime_mongo.schedule_timer backend_a ~workflow_id:"wf_expired"
        ~worker_id:"worker_a" ~now_ms:1_103L ~timer_id:"wake"
        ~run_at_ms:2_000L ~message:"too late" ()
      |> expect_ok "stale timer"
      |> Alcotest.(check bool) "stale timer rejected" false;
      Workflow_runtime_mongo.start_child backend_a
        ~parent_workflow_id:"wf_expired" ~worker_id:"worker_a"
        ~now_ms:1_103L (workflow "wf_stale_child")
        (Workflow_runtime.enqueue_options ())
      |> expect_ok "stale child"
      |> Alcotest.(check bool) "stale child rejected" false;
      Workflow_runtime_mongo.complete_update backend_a
        ~workflow_id:"wf_expired" ~worker_id:"worker_a" ~now_ms:1_103L
        ~update_id:"upd_1" ~status:Workflow_runtime.Update_completed_status ()
      |> expect_ok "stale complete update"
      |> Alcotest.(check bool) "stale update completion rejected" false;
      let policy = Workflow_runtime.retry_policy ~max_attempts:2 () in
      let retry_attempt =
        Workflow_runtime_mongo.retry backend_a ~workflow_id:"wf_expired"
          ~worker_id:"worker_a" ~now_ms:1_103L ~policy ~message:"too late"
        |> expect_ok "stale retry"
        |> function
        | Some (Workflow_runtime.Retried { attempt; _ })
        | Some (Retries_exhausted { attempt }) ->
            Some attempt
        | None -> None
      in
      Alcotest.(check (option int)) "stale retry rejected" None retry_attempt;
      let reclaimed =
        Workflow_runtime_mongo.claim_workflow backend_b ~workflow_id:"wf_expired"
          ~worker_id:"worker_b" ~now_ms:1_103L ~lease_ms:100L
        |> expect_ok "reclaim"
        |> Option.get
      in
      Alcotest.(check string) "reclaimed by worker b" "worker_b"
        reclaimed.worker_id;
      Workflow_runtime_mongo.complete backend_b ~workflow_id:"wf_expired"
        ~worker_id:"worker_b" ~now_ms:1_104L ~status:Succeeded ~message:"done"
      |> expect_ok "complete"
      |> Alcotest.(check bool) "fresh owner completes" true;
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_expired" backend_b
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "only valid owner events"
        [
          "workflow_enqueued";
          "update_requested";
          "workflow_claimed";
          "workflow_claimed";
          "workflow_completed";
        ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history);
      let updates =
        Workflow_runtime_mongo.updates ~workflow_id:"wf_expired" backend_b
        |> expect_ok "updates"
      in
      let update = List.hd updates in
      Alcotest.(check string) "update still pending" "pending"
        (Workflow_runtime.update_status_to_string update.status))

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
      let activity_event_filter =
        bson_doc
          [
            bson_string "workflow_id" "wf_activity";
            bson_string "kind" "activity_completed";
          ]
      in
      Mongo_eio.direct_delete_many client ~db
        ~collection:"activity_workflows_events" activity_event_filter
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete activity events"
      |> ignore;
      Workflow_runtime_mongo.record_activity_result backend_b ~now_ms:2_003L
        Workflow_runtime.
          {
            activity_id = "publish_call";
            workflow_id = "wf_activity";
            name = "Publish call";
            attempt = 1;
            status = Activity_succeeded;
            result_json = Some {|{"external_id":"456"}|};
            error = None;
            updated_at_ms = 0L;
          }
      |> expect_ok "record duplicate activity";
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
         Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind);
      let replay =
        Workflow_runtime.replay history
        |> Result.fold ~ok:Fun.id ~error:(fun message -> Alcotest.fail message)
      in
      Alcotest.(check int) "replay activity count" 1
        (List.length replay.activities);
      let activity = List.hd replay.activities in
      Alcotest.(check string) "replay activity id" "publish_call"
        activity.activity_id;
      Alcotest.(check (option string)) "replay activity result"
        (Some {|{"external_id":"123"}|}) activity.result_json)

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

let test_mongo_timer_survives_and_fires_across_backend_instances () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"timer_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"timer_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "durable timers" true capabilities.durable_timers;
      Alcotest.(check bool) "deterministic replay" true
        capabilities.deterministic_replay;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:4_000L (workflow "wf_timer")
        (Workflow_runtime.enqueue_options ~run_at_ms:4_000L ())
      |> expect_ok "enqueue";
      let claim =
        Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_timer"
          ~worker_id:"timer-worker" ~now_ms:4_000L ~lease_ms:10_000L
        |> expect_ok "claim"
        |> Option.get
      in
      Alcotest.(check int) "first attempt" 1 claim.item.attempt;
      Workflow_runtime_mongo.schedule_timer backend_a ~workflow_id:"wf_timer"
        ~worker_id:"timer-worker" ~now_ms:4_001L ~timer_id:"sleep_1"
        ~run_at_ms:5_000L ~payload_json:{|{"reason":"wait"}|}
        ~message:"sleep until dependency ready" ()
      |> expect_ok "schedule timer"
      |> Alcotest.(check bool) "scheduled" true;
      Alcotest.(check bool)
        "not claimable before due" true
        (Workflow_runtime_mongo.claim_workflow backend_b ~workflow_id:"wf_timer"
           ~worker_id:"early-worker" ~now_ms:4_999L ~lease_ms:10_000L
         |> expect_ok "early claim"
         |> Option.is_none);
      let timers =
        Workflow_runtime_mongo.timers ~workflow_id:"wf_timer" backend_b
        |> expect_ok "timers"
      in
      Alcotest.(check int) "one timer" 1 (List.length timers);
      Alcotest.(check (option int64)) "not fired" None
        (List.hd timers).fired_at_ms;
      Mongo_eio.direct_update_one client ~db ~collection:"timer_workflows_timers"
        ~upsert:false
        (bson_doc [ bson_string "_id" "wf_timer:sleep_1" ])
        (bson_doc
           [
             bson_doc_element "$set"
               (bson_doc
                  [ bson_int64 "fired_at_ms" 5_000L; bson_int64 "updated_at_ms" 5_000L ]);
           ])
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "simulate fired timer without event"
      |> ignore;
      let fired =
        Workflow_runtime_mongo.claim_workflow backend_b ~workflow_id:"wf_timer"
          ~worker_id:"late-worker" ~now_ms:5_000L ~lease_ms:10_000L
        |> expect_ok "late claim"
        |> Option.get
      in
      Alcotest.(check int) "second attempt" 2 fired.item.attempt;
      let timers =
        Workflow_runtime_mongo.timers ~workflow_id:"wf_timer" backend_a
        |> expect_ok "fired timers"
      in
      Alcotest.(check (option int64)) "fired" (Some 5_000L)
        (List.hd timers).fired_at_ms;
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_timer" backend_a
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "timer history"
        [
          "workflow_enqueued";
          "workflow_claimed";
          "timer_scheduled";
          "timer_fired";
          "workflow_claimed";
        ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history);
      let replay =
        Workflow_runtime.replay history
        |> Result.fold ~ok:Fun.id ~error:(fun message -> Alcotest.fail message)
      in
      Alcotest.(check int) "replay claims" 2 replay.claim_count;
      Alcotest.(check int) "replay timer count" 1 (List.length replay.timers);
      let timer = List.hd replay.timers in
      Alcotest.(check string) "replay timer id" "sleep_1" timer.timer_id;
      Alcotest.(check (option int64)) "replay fired" (Some 5_000L)
        timer.fired_at_ms)

let test_mongo_signals_queries_and_compaction_across_backend_instances () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"signal_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"signal_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "signals" true capabilities.signals;
      Alcotest.(check bool) "queries" true capabilities.queries;
      Alcotest.(check bool) "history compaction" true
        capabilities.history_compaction;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:6_000L
        (workflow "wf_signal")
        (Workflow_runtime.enqueue_options ~run_at_ms:6_000L ())
      |> expect_ok "enqueue";
      Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_signal"
        ~worker_id:"worker_a" ~now_ms:6_001L ~lease_ms:10_000L
      |> expect_ok "claim"
      |> Option.get
      |> ignore;
      Workflow_runtime_mongo.record_activity_result backend_a ~now_ms:6_002L
        Workflow_runtime.
          {
            activity_id = "prepare_payload";
            workflow_id = "wf_signal";
            name = "prepare payload";
            attempt = 1;
            status = Activity_succeeded;
            result_json = Some {|{"ok":true}|};
            error = None;
            updated_at_ms = 0L;
          }
      |> expect_ok "record activity";
      Workflow_runtime_mongo.schedule_timer backend_a ~workflow_id:"wf_signal"
        ~worker_id:"worker_a" ~now_ms:6_003L ~timer_id:"wait_for_auth"
        ~run_at_ms:20_000L ~message:"wait for external auth" ()
      |> expect_ok "schedule timer"
      |> Alcotest.(check bool) "timer scheduled" true;
      Workflow_runtime_mongo.signal backend_b ~workflow_id:"wf_signal"
        ~now_ms:6_004L ~signal_id:"sig_1" ~name:"auth_ready"
        ~payload_json:{|{"account":"personal"}|} ()
      |> expect_ok "signal"
      |> Alcotest.(check bool) "signal accepted" true;
      Workflow_runtime_mongo.signal backend_a ~workflow_id:"wf_signal"
        ~now_ms:6_005L ~signal_id:"sig_1" ~name:"auth_ready"
        ~payload_json:{|{"account":"personal"}|} ()
      |> expect_ok "duplicate signal"
      |> Alcotest.(check bool) "duplicate accepted idempotently" true;
      let signals =
        Workflow_runtime_mongo.signals ~workflow_id:"wf_signal" backend_a
        |> expect_ok "signals"
      in
      Alcotest.(check int) "stored signals" 1 (List.length signals);
      let state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_signal" backend_b
        |> expect_ok "query state"
        |> Option.get
      in
      Alcotest.(check int) "query activities" 1 (List.length state.activities);
      Alcotest.(check int) "query timers" 1 (List.length state.timers);
      Alcotest.(check int) "query signals" 1 (List.length state.signals);
      let signal = List.hd state.signals in
      Alcotest.(check string) "signal name" "auth_ready" signal.name;
      Alcotest.(check (option string)) "signal payload"
        (Some {|{"account":"personal"}|})
        signal.payload_json;
      Workflow_runtime_mongo.compact_history ~workflow_id:"wf_signal" backend_b
      |> expect_ok "compact"
      |> Option.get
      |> ignore;
      let compacted_history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_signal" backend_a
        |> expect_ok "compacted history"
      in
      Alcotest.(check (list string))
        "history compacted"
        [ "history_compacted" ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           compacted_history);
      let compacted_state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_signal" backend_a
        |> expect_ok "query compacted"
        |> Option.get
      in
      Alcotest.(check int) "compacted activities" 1
        (List.length compacted_state.activities);
      Alcotest.(check int) "compacted timers" 1
        (List.length compacted_state.timers);
      Alcotest.(check int) "compacted signals" 1
        (List.length compacted_state.signals);
      Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_signal"
        ~worker_id:"worker_b" ~now_ms:6_006L ~lease_ms:10_000L
      |> expect_ok "claim after signal"
      |> Option.get
      |> fun claim ->
      Alcotest.(check string) "claimed after signal" "wf_signal"
        claim.Workflow_runtime.item.workflow.id)

let test_mongo_duplicate_messages_repair_missing_events () =
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
      let collection = "repair_workflows" in
      let backend_a = Workflow_runtime_mongo.create ~client ~db ~collection () in
      let backend_b = Workflow_runtime_mongo.create ~client ~db ~collection () in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:10_000L
        (workflow "wf_repair")
        (Workflow_runtime.enqueue_options ~run_at_ms:20_000L ())
      |> expect_ok "enqueue";
      Workflow_runtime_mongo.signal backend_a ~workflow_id:"wf_repair"
        ~now_ms:10_001L ~signal_id:"sig_repair" ~name:"dependency_ready"
        ~payload_json:{|{"ready":true}|} ()
      |> expect_ok "signal"
      |> Alcotest.(check bool) "signal accepted" true;
      Workflow_runtime_mongo.request_update backend_a ~workflow_id:"wf_repair"
        ~now_ms:10_002L ~update_id:"upd_repair" ~name:"change_target"
        ~payload_json:{|{"target":"blog"}|} ()
      |> expect_ok "request update"
      |> Alcotest.(check bool) "update accepted" true;
      let event_filter =
        bson_doc
          [
            bson_string "workflow_id" "wf_repair";
            bson_doc_element "kind"
              (bson_doc
                 [
                   ( "$in",
                     Bson.create_list
                       [
                         Bson.create_string "signal_received";
                         Bson.create_string "update_requested";
                       ] );
                 ]);
          ]
      in
      Mongo_eio.direct_delete_many client ~db
        ~collection:(collection ^ "_events") event_filter
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete events"
      |> ignore;
      Workflow_runtime_mongo.signal backend_b ~workflow_id:"wf_repair"
        ~now_ms:10_003L ~signal_id:"sig_repair" ~name:"dependency_ready"
        ~payload_json:{|{"ready":true}|} ()
      |> expect_ok "duplicate signal repair"
      |> Alcotest.(check bool) "signal repaired" true;
      Workflow_runtime_mongo.request_update backend_b ~workflow_id:"wf_repair"
        ~now_ms:10_004L ~update_id:"upd_repair" ~name:"change_target"
        ~payload_json:{|{"target":"blog"}|} ()
      |> expect_ok "duplicate update repair"
      |> Alcotest.(check bool) "update repaired" true;
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_repair" backend_a
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "repaired events"
        [ "workflow_enqueued"; "signal_received"; "update_requested" ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history);
      let state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_repair" backend_b
        |> expect_ok "query state"
        |> Option.get
      in
      Alcotest.(check int) "one signal" 1 (List.length state.signals);
      Alcotest.(check int) "one update" 1 (List.length state.updates))

let test_mongo_cancellation_across_backend_instances () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"cancel_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"cancel_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "cancellation" true capabilities.cancellation;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:7_000L
        (workflow "wf_cancel")
        (Workflow_runtime.enqueue_options ~run_at_ms:7_000L ())
      |> expect_ok "enqueue";
      Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_cancel"
        ~worker_id:"worker_a" ~now_ms:7_001L ~lease_ms:60_000L
      |> expect_ok "claim"
      |> Option.get
      |> ignore;
      Workflow_runtime_mongo.cancel backend_b ~workflow_id:"wf_cancel"
        ~now_ms:7_002L ~reason:"user requested stop"
      |> expect_ok "cancel"
      |> Alcotest.(check bool) "cancelled" true;
      Workflow_runtime_mongo.cancel backend_a ~workflow_id:"wf_cancel"
        ~now_ms:7_003L ~reason:"duplicate"
      |> expect_ok "cancel duplicate"
      |> Alcotest.(check bool) "duplicate ignored" false;
      Alcotest.(check bool)
        "cancelled workflow is not claimable" true
        (Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_cancel"
           ~worker_id:"worker_b" ~now_ms:7_004L ~lease_ms:10_000L
         |> expect_ok "claim after cancel"
         |> Option.is_none);
      Workflow_runtime_mongo.signal backend_b ~workflow_id:"wf_cancel"
        ~now_ms:7_005L ~signal_id:"late_signal" ~name:"resume" ()
      |> expect_ok "signal after cancel"
      |> Alcotest.(check bool) "signal rejected" false;
      Mongo_eio.direct_delete_many client ~db
        ~collection:"cancel_workflows_events"
        (bson_doc
           [
             bson_string "workflow_id" "wf_cancel";
             bson_string "kind" "workflow_cancelled";
           ])
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete cancelled event"
      |> ignore;
      let snapshot =
        Workflow_runtime_mongo.snapshot backend_b |> expect_ok "snapshot"
      in
      let item = List.hd snapshot in
      Alcotest.(check string) "cancelled status" "cancelled"
        (Workflow_runtime.status_to_string item.status);
      Alcotest.(check int) "cancelled stat" 1
        (Workflow_runtime.stats snapshot).cancelled;
      let history =
        Workflow_runtime_mongo.history ~workflow_id:"wf_cancel" backend_a
        |> expect_ok "history"
      in
      Alcotest.(check (list string))
        "cancel history"
        [ "workflow_enqueued"; "workflow_claimed"; "workflow_cancelled" ]
        (List.map
           (fun event ->
             Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
           history);
      let state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_cancel" backend_b
        |> expect_ok "query state"
        |> Option.get
      in
      let completion = Option.get state.Workflow_runtime.completion in
      Alcotest.(check string) "replay status" "cancelled"
        (Workflow_runtime.status_to_string completion.status);
      Alcotest.(check (option string)) "replay reason"
        (Some "user requested stop") completion.message)

let test_mongo_child_workflows_can_nest_across_backend_instances () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"child_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"child_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "child workflows" true capabilities.child_workflows;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:8_000L
        (workflow_kind "wf_parent" "root")
        (Workflow_runtime.enqueue_options ~run_at_ms:8_000L ())
      |> expect_ok "enqueue parent";
      Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_parent"
        ~worker_id:"parent_worker" ~now_ms:8_001L ~lease_ms:10_000L
      |> expect_ok "claim parent"
      |> Option.get
      |> ignore;
      Workflow_runtime_mongo.start_child backend_a ~parent_workflow_id:"wf_parent"
        ~worker_id:"parent_worker" ~now_ms:8_002L
        (workflow_kind "wf_child" "child_step")
        (Workflow_runtime.enqueue_options ~payload_json:{|{"step":1}|} ())
      |> expect_ok "start child"
      |> Alcotest.(check bool) "child started" true;
      Mongo_eio.direct_delete_many client ~db
        ~collection:"child_workflows_child_workflows"
        (bson_doc [ bson_string "_id" "wf_parent:wf_child" ])
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete child link"
      |> ignore;
      Mongo_eio.direct_delete_many client ~db
        ~collection:"child_workflows_events"
        (bson_doc
           [
             bson_string "workflow_id" "wf_parent";
             bson_string "kind" "child_workflow_started";
           ])
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete parent child event"
      |> ignore;
      Workflow_runtime_mongo.start_child backend_a ~parent_workflow_id:"wf_parent"
        ~worker_id:"parent_worker" ~now_ms:8_003L
        (workflow_kind "wf_child" "child_step")
        (Workflow_runtime.enqueue_options ~payload_json:{|{"step":1}|} ())
      |> expect_ok "start duplicate child"
      |> Alcotest.(check bool) "duplicate child idempotent" true;
      let children =
        Workflow_runtime_mongo.children ~parent_workflow_id:"wf_parent" backend_b
        |> expect_ok "children"
      in
      Alcotest.(check int) "one child" 1 (List.length children);
      Alcotest.(check string) "child id" "wf_child"
        (List.hd children).Workflow_runtime.workflow.id;
      Workflow_runtime_mongo.claim_workflow backend_b ~workflow_id:"wf_child"
        ~worker_id:"child_worker" ~now_ms:8_004L ~lease_ms:10_000L
      |> expect_ok "claim child"
      |> Option.get
      |> ignore;
      Workflow_runtime_mongo.start_child backend_b ~parent_workflow_id:"wf_child"
        ~worker_id:"child_worker" ~now_ms:8_005L
        (workflow_kind "wf_grandchild" "grandchild_step")
        (Workflow_runtime.enqueue_options ())
      |> expect_ok "start grandchild"
      |> Alcotest.(check bool) "grandchild started" true;
      let grandchildren =
        Workflow_runtime_mongo.children ~parent_workflow_id:"wf_child" backend_a
        |> expect_ok "grandchildren"
      in
      Alcotest.(check int) "one grandchild" 1 (List.length grandchildren);
      Alcotest.(check string) "grandchild id" "wf_grandchild"
        (List.hd grandchildren).Workflow_runtime.workflow.id;
      let parent_state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_parent" backend_b
        |> expect_ok "query parent"
        |> Option.get
      in
      Alcotest.(check int) "parent replay child count" 1
        (List.length parent_state.child_workflows);
      let child_state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_child" backend_a
        |> expect_ok "query child"
        |> Option.get
      in
      Alcotest.(check int) "child replay child count" 1
        (List.length child_state.child_workflows))

let test_mongo_updates_survive_backend_instances () =
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
        Workflow_runtime_mongo.create ~client ~db ~collection:"update_workflows" ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection:"update_workflows" ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      let capabilities = Workflow_runtime_mongo.capabilities backend_a in
      Alcotest.(check bool) "updates" true capabilities.updates;
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:9_000L
        (workflow "wf_update")
        (Workflow_runtime.enqueue_options ~run_at_ms:10_000L ())
      |> expect_ok "enqueue";
      Workflow_runtime_mongo.request_update backend_b ~workflow_id:"wf_update"
        ~now_ms:9_001L ~update_id:"upd_accept" ~name:"change_target"
        ~payload_json:{|{"target":"blog"}|} ()
      |> expect_ok "request update"
      |> Alcotest.(check bool) "update requested" true;
      Workflow_runtime_mongo.request_update backend_a ~workflow_id:"wf_update"
        ~now_ms:9_002L ~update_id:"upd_accept" ~name:"change_target"
        ~payload_json:{|{"target":"blog"}|} ()
      |> expect_ok "duplicate request update"
      |> Alcotest.(check bool) "duplicate update idempotent" true;
      Workflow_runtime_mongo.request_update backend_b ~workflow_id:"wf_update"
        ~now_ms:9_003L ~update_id:"upd_reject" ~name:"set_bad_target"
        ~payload_json:{|{"target":""}|} ()
      |> expect_ok "request rejected update"
      |> Alcotest.(check bool) "second update requested" true;
      let claim =
        Workflow_runtime_mongo.claim_workflow backend_a ~workflow_id:"wf_update"
          ~worker_id:"worker_a" ~now_ms:9_004L ~lease_ms:10_000L
        |> expect_ok "claim"
        |> Option.get
      in
      Alcotest.(check string) "claimed update workflow" "wf_update"
        claim.item.workflow.id;
      Workflow_runtime_mongo.complete_update backend_a ~workflow_id:"wf_update"
        ~worker_id:"worker_a" ~now_ms:9_005L ~update_id:"upd_accept"
        ~status:Workflow_runtime.Update_completed_status
        ~result_json:{|{"accepted":true}|} ()
      |> expect_ok "complete update"
      |> Alcotest.(check bool) "update completed" true;
      Workflow_runtime_mongo.complete_update backend_a ~workflow_id:"wf_update"
        ~worker_id:"worker_a" ~now_ms:9_006L ~update_id:"upd_reject"
        ~status:Workflow_runtime.Update_rejected ~error:"target required" ()
      |> expect_ok "reject update"
      |> Alcotest.(check bool) "update rejected" true;
      let updates =
        Workflow_runtime_mongo.updates ~workflow_id:"wf_update" backend_b
        |> expect_ok "updates"
      in
      Alcotest.(check int) "two updates" 2 (List.length updates);
      let accepted =
        updates
        |> List.find (fun update ->
               String.equal update.Workflow_runtime.update_id "upd_accept")
      in
      Alcotest.(check string) "accepted status" "completed"
        (Workflow_runtime.update_status_to_string accepted.status);
      Alcotest.(check (option string)) "accepted result"
        (Some {|{"accepted":true}|})
        accepted.result_json;
      let rejected =
        updates
        |> List.find (fun update ->
               String.equal update.Workflow_runtime.update_id "upd_reject")
      in
      Alcotest.(check string) "rejected status" "rejected"
        (Workflow_runtime.update_status_to_string rejected.status);
      Alcotest.(check (option string)) "rejected error"
        (Some "target required") rejected.error;
      let state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_update" backend_a
        |> expect_ok "query state"
        |> Option.get
      in
      Alcotest.(check int) "replay update count" 2
        (List.length state.updates);
      let completed_event_filter =
        bson_doc
          [
            bson_string "workflow_id" "wf_update";
            bson_string "kind" "update_completed";
          ]
      in
      Mongo_eio.direct_delete_many client ~db
        ~collection:"update_workflows_events" completed_event_filter
      |> Result.map_error (fun error -> `Mongo (Mongo_error.to_string error))
      |> expect_ok "delete update completed events"
      |> ignore;
      Workflow_runtime_mongo.complete_update backend_a ~workflow_id:"wf_update"
        ~worker_id:"worker_a" ~now_ms:9_007L ~update_id:"upd_accept"
        ~status:Workflow_runtime.Update_completed_status
        ~result_json:{|{"accepted":true}|} ()
      |> expect_ok "duplicate complete update"
      |> Alcotest.(check bool) "duplicate completion idempotent" true;
      let repaired_state =
        Workflow_runtime_mongo.query_state ~workflow_id:"wf_update" backend_b
        |> expect_ok "repaired query state"
        |> Option.get
      in
      Alcotest.(check int) "repaired replay update count" 2
        (List.length repaired_state.updates);
      let repaired_accepted =
        repaired_state.updates
        |> List.find (fun update ->
               String.equal update.Workflow_runtime.update_id "upd_accept")
      in
      Alcotest.(check string) "repaired accepted status" "completed"
        (Workflow_runtime.update_status_to_string repaired_accepted.status))

let () =
  Mirage_crypto_rng_unix.use_default ();
  Alcotest.run "workflow-runtime-mongo"
    [
      ( "mongo",
        [
          Alcotest.test_case "validation" `Quick test_mongo_validation;
          Alcotest.test_case "claims and lease recovery" `Quick
            test_mongo_claims_and_lease_recovery;
          Alcotest.test_case "claims same due time by workflow id" `Quick
            test_mongo_claims_same_due_time_by_workflow_id;
          Alcotest.test_case "owned operations require active lease" `Quick
            test_mongo_owned_operations_require_active_lease;
          Alcotest.test_case "activity results survive backend instances" `Quick
            test_mongo_activity_results_survive_backend_instances;
          Alcotest.test_case "kind claim filter and retry policy" `Quick
            test_mongo_kind_claim_filter_and_retry_policy;
          Alcotest.test_case "timer survives and fires across backend instances"
            `Quick
            test_mongo_timer_survives_and_fires_across_backend_instances;
          Alcotest.test_case
            "signals queries and compaction across backend instances" `Quick
            test_mongo_signals_queries_and_compaction_across_backend_instances;
          Alcotest.test_case "duplicate messages repair missing events" `Quick
            test_mongo_duplicate_messages_repair_missing_events;
          Alcotest.test_case "cancellation across backend instances" `Quick
            test_mongo_cancellation_across_backend_instances;
          Alcotest.test_case "child workflows can nest across backend instances"
            `Quick
            test_mongo_child_workflows_can_nest_across_backend_instances;
          Alcotest.test_case "updates survive backend instances" `Quick
            test_mongo_updates_survive_backend_instances;
        ] );
    ]
