let workflow ?(tenant_id = "tenant_a") id =
  Workflow_runtime.
    {
      id;
      tenant_id;
      kind = "publish_attempt";
      subject_id = Some ("post_" ^ id);
      name = Some "personal_blog";
      metadata = [ ("destination", "personal_blog") ];
    }

let clock_from values =
  let values = ref values in
  fun () ->
    match !values with
    | value :: rest ->
        values := rest;
        value
    | [] -> Alcotest.fail "clock exhausted"

let status_of_single json =
  match Yojson.Safe.Util.(json |> member "workflows" |> to_list) with
  | [ item ] -> Yojson.Safe.Util.(item |> member "status" |> to_string)
  | _ -> ""

let expect_ok label = function
  | Ok value -> value
  | Error error -> Alcotest.fail (label ^ ": " ^ Workflow_runtime.Memory_backend.error_to_string error)

let test_lifecycle_and_stats () =
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms = clock_from [ 1_000L; 1_001L; 1_002L; 1_003L ]
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  let workflow = workflow "wf_1" in
  Runtime.ensure backend |> expect_ok "ensure";
  Runtime.enqueue backend workflow (Workflow_runtime.enqueue_options ()) |> expect_ok "enqueue";
  let claim =
    Runtime.claim_next backend ~worker_id:"worker_a" ~lease_ms:30_000L
    |> expect_ok "claim"
    |> Option.get
  in
  Alcotest.(check string) "claimed id" "wf_1" claim.item.workflow.id;
  Runtime.complete backend ~workflow_id:"wf_1" ~worker_id:"worker_a"
    ~status:Blocked ~message:"missing connection"
  |> expect_ok "complete"
  |> Alcotest.(check bool) "completed" true;
  let snapshot = Runtime.snapshot backend |> expect_ok "snapshot" in
  Alcotest.(check int) "one workflow" 1 (List.length snapshot);
  Alcotest.(check string)
    "blocked JSON" "blocked"
    (Runtime.snapshot_json backend |> expect_ok "snapshot_json" |> status_of_single);
  Alcotest.(check int) "blocked stat" 1 (Workflow_runtime.stats snapshot).blocked

let test_multi_worker_claims_are_exclusive_and_expire () =
  let now = ref 10_000L in
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms () = !now
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_1") (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue 1";
  Runtime.enqueue backend (workflow "wf_2") (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue 2";
  let first =
    Runtime.claim_next backend ~worker_id:"worker_a" ~lease_ms:100L
    |> expect_ok "claim a"
    |> Option.get
  in
  let second =
    Runtime.claim_next backend ~worker_id:"worker_b" ~lease_ms:100L
    |> expect_ok "claim b"
    |> Option.get
  in
  Alcotest.(check bool)
    "different claims" true
    (not (String.equal first.item.workflow.id second.item.workflow.id));
  Alcotest.(check (option string))
    "nothing left" None
    (Runtime.claim_next backend ~worker_id:"worker_c" ~lease_ms:100L
     |> expect_ok "claim c"
     |> Option.map (fun (claim : Workflow_runtime.claim) ->
            claim.item.workflow.id));
  now := 10_101L;
  let recovered =
    Runtime.claim_next backend ~worker_id:"worker_c" ~lease_ms:100L
    |> expect_ok "claim expired"
    |> Option.get
  in
  Alcotest.(check int) "attempt incremented" 2 recovered.item.attempt

let test_history_targeted_claim_and_activity_result () =
  let now = ref 100L in
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms () =
        let value = !now in
        now := Int64.add value 1L;
        value
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_1") (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue wf_1";
  Runtime.enqueue backend (workflow "wf_2") (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue wf_2";
  let targeted =
    Runtime.claim_workflow backend ~workflow_id:"wf_2" ~worker_id:"worker_a"
      ~lease_ms:1_000L
    |> expect_ok "claim wf_2"
    |> Option.get
  in
  Alcotest.(check string) "targeted claim" "wf_2" targeted.item.workflow.id;
  Runtime.record_activity_result backend
    Workflow_runtime.
      {
        activity_id = "write_blog_file";
        workflow_id = "wf_2";
        name = "write blog file";
        attempt = 1;
        status = Activity_succeeded;
        result_json = Some {|{"sha":"abc"}|};
        error = None;
        updated_at_ms = 0L;
      }
  |> expect_ok "record activity";
  Runtime.complete backend ~workflow_id:"wf_2" ~worker_id:"worker_a"
    ~status:Succeeded ~message:"done"
  |> expect_ok "complete wf_2"
  |> Alcotest.(check bool) "completed" true;
  let history =
    Runtime.history backend ~workflow_id:"wf_2" |> expect_ok "history"
  in
  Alcotest.(check (list string))
    "event history"
    [
      "workflow_enqueued";
      "workflow_claimed";
      "activity_completed";
      "workflow_completed";
    ]
    (List.map
       (fun event -> Workflow_runtime.event_kind_to_string event.Workflow_runtime.kind)
       history);
  Alcotest.(check (list int))
    "event sequence"
    [ 1; 2; 3; 4 ]
    (List.map (fun event -> event.Workflow_runtime.sequence) history);
  let result =
    Runtime.find_activity_result backend ~workflow_id:"wf_2"
      ~activity_id:"write_blog_file"
    |> expect_ok "find activity"
    |> Option.get
  in
  Alcotest.(check (option string))
    "activity result preserved" (Some {|{"sha":"abc"}|}) result.result_json

let test_kind_claim_filter_and_retry_policy () =
  let now = ref 1_000L in
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms () = !now
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  let publish = workflow "publish_1" in
  let email =
    Workflow_runtime.{ (workflow "email_1") with kind = "send_email" }
  in
  Runtime.enqueue backend publish (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue publish";
  Runtime.enqueue backend email (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue email";
  let capabilities = Runtime.capabilities backend in
  Alcotest.(check bool) "task queue filtering" true
    capabilities.task_queue_filtering;
  let claim =
    Runtime.claim_next backend ~kind:"send_email" ~worker_id:"email-worker"
      ~lease_ms:10_000L
    |> expect_ok "claim kind"
    |> Option.get
  in
  Alcotest.(check string) "claimed email workflow" "send_email"
    claim.item.workflow.kind;
  let policy =
    Workflow_runtime.retry_policy ~max_attempts:2 ~initial_backoff_ms:250L
      ~max_backoff_ms:1_000L ()
  in
  let decision =
    Runtime.retry backend ~workflow_id:"email_1" ~worker_id:"email-worker"
      ~policy ~message:"temporary smtp error"
    |> expect_ok "retry"
    |> Option.get
  in
  let retry_run_at =
    match decision with
    | Workflow_runtime.Retried { attempt; run_at_ms } ->
        Alcotest.(check int) "retry attempt" 1 attempt;
        run_at_ms
    | Retries_exhausted _ -> Alcotest.fail "expected retry"
  in
  Alcotest.(check int64) "backoff run_at" 1_250L retry_run_at;
  now := retry_run_at;
  let retry_claim =
    Runtime.claim_workflow backend ~workflow_id:"email_1"
      ~worker_id:"email-worker" ~lease_ms:10_000L
    |> expect_ok "claim retry"
    |> Option.get
  in
  Alcotest.(check int) "second attempt" 2 retry_claim.item.attempt;
  let final =
    Runtime.retry backend ~workflow_id:"email_1" ~worker_id:"email-worker"
      ~policy ~message:"smtp still down"
    |> expect_ok "retry exhausted"
    |> Option.get
  in
  (match final with
  | Workflow_runtime.Retries_exhausted { attempt } ->
      Alcotest.(check int) "exhausted attempt" 2 attempt
  | Retried _ -> Alcotest.fail "expected exhausted retries");
  let snapshot = Runtime.snapshot backend |> expect_ok "snapshot" in
  let email_item =
    snapshot
    |> List.find_opt (fun item -> String.equal item.Workflow_runtime.workflow.id "email_1")
    |> Option.get
  in
  Alcotest.(check string) "failed after exhausted retries" "failed"
    (Workflow_runtime.status_to_string email_item.status)

let test_grouping_filtering_and_reschedule () =
  let now = ref 10L in
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms () =
        let value = !now in
        now := Int64.add value 1L;
        value
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  Runtime.enqueue backend (workflow "wf_1") (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue a";
  Runtime.enqueue backend
    (workflow ~tenant_id:"tenant_b" "wf_2")
    (Workflow_runtime.enqueue_options ())
  |> expect_ok "enqueue b";
  let claim =
    Runtime.claim_next backend ~worker_id:"worker_a" ~lease_ms:100L
    |> expect_ok "claim"
    |> Option.get
  in
  Runtime.reschedule backend ~workflow_id:claim.item.workflow.id
    ~worker_id:"worker_a" ~run_at_ms:1_000L ~message:"wait"
  |> expect_ok "reschedule"
  |> Alcotest.(check bool) "rescheduled" true;
  let grouped =
    Runtime.snapshot_json backend ~group_by_tenant:true
    |> expect_ok "grouped snapshot"
  in
  let tenants =
    match Yojson.Safe.Util.(grouped |> member "tenants") with
    | `Assoc tenants -> tenants
    | _ -> Alcotest.fail "unexpected grouped workflow JSON"
  in
  Alcotest.(check bool) "tenant a present" true (List.mem_assoc "tenant_a" tenants);
  Alcotest.(check int)
    "tenant filter" 1
    (Runtime.snapshot backend ~tenant_id:"tenant_b" |> expect_ok "tenant snapshot" |> List.length)

let test_validation () =
  let module Runtime =
    Workflow_runtime.Make (struct
      let now_ms () = 1L
    end) (Workflow_runtime.Memory_backend)
  in
  let backend = Workflow_runtime.Memory_backend.create () in
  match
    Runtime.enqueue backend
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
  with
  | Error (`Invalid_workflow _) -> ()
  | Ok () -> Alcotest.fail "expected validation error"
  | Error error ->
      Alcotest.fail
        ("unexpected error: " ^ Workflow_runtime.Memory_backend.error_to_string error)

let () =
  Alcotest.run "workflow-runtime"
    [
      ( "runtime",
        [
          Alcotest.test_case "lifecycle and stats" `Quick test_lifecycle_and_stats;
          Alcotest.test_case "multi-worker exclusive claims" `Quick
            test_multi_worker_claims_are_exclusive_and_expire;
          Alcotest.test_case "history targeted claim and activity result" `Quick
            test_history_targeted_claim_and_activity_result;
          Alcotest.test_case "kind claim filter and retry policy" `Quick
            test_kind_claim_filter_and_retry_policy;
          Alcotest.test_case "grouping filtering and reschedule" `Quick
            test_grouping_filtering_and_reschedule;
          Alcotest.test_case "validation" `Quick test_validation;
        ] );
    ]
