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
  match json with
  | `Assoc fields -> (
      match List.assoc_opt "workflows" fields with
      | Some (`List [ item ]) ->
          Yojson.Safe.Util.(item |> member "status" |> to_string)
      | _ -> "")
  | _ -> ""

let test_lifecycle_and_stats () =
  let runtime =
    Workflow_runtime.create
      ~clock:(clock_from [ 1_000L; 1_001L; 1_002L; 1_003L ])
      ()
  in
  let workflow = workflow "wf_1" in
  Workflow_runtime.record_queued runtime workflow;
  Workflow_runtime.record_running runtime workflow;
  Workflow_runtime.record_blocked runtime workflow "missing connection";
  let snapshot = Workflow_runtime.snapshot runtime in
  Alcotest.(check int) "one workflow" 1 (List.length snapshot);
  Alcotest.(check string)
    "blocked JSON" "blocked"
    (Workflow_runtime.snapshot_json runtime |> status_of_single);
  Alcotest.(check int) "blocked stat" 1 (Workflow_runtime.stats runtime).blocked

let test_grouping_filtering_and_eviction () =
  let runtime =
    Workflow_runtime.create
      ~clock:(clock_from [ 10L; 11L; 12L; 13L; 14L; 15L; 16L ])
      ~max_items:2 ()
  in
  let first = workflow "wf_1" in
  let second = workflow ~tenant_id:"tenant_b" "wf_2" in
  let third = workflow ~tenant_id:"tenant_b" "wf_3" in
  Workflow_runtime.record_queued runtime first;
  Workflow_runtime.record_succeeded runtime first "done";
  Workflow_runtime.record_running runtime second;
  Workflow_runtime.record_queued runtime third;
  let groups = Workflow_runtime.grouped_by_tenant runtime in
  Alcotest.(check bool)
    "oldest terminal evicted" false
    (List.mem_assoc "tenant_a" groups);
  Alcotest.(check int)
    "tenant b retained" 2
    (match List.assoc_opt "tenant_b" groups with
    | Some items -> List.length items
    | None -> 0);
  Alcotest.(check int)
    "tenant filter" 2
    (Workflow_runtime.snapshot ~tenant_id:"tenant_b" runtime |> List.length)

let test_validation () =
  let runtime = Workflow_runtime.create ~clock:(fun () -> 1L) () in
  Alcotest.check_raises "empty id rejected"
    (Invalid_argument "Workflow_runtime: workflow id must not be empty")
    (fun () ->
      Workflow_runtime.record_queued runtime
        (Workflow_runtime.
           {
             id = "";
             tenant_id = "tenant";
             kind = "kind";
             subject_id = None;
             name = None;
             metadata = [];
           }))

let () =
  Alcotest.run "workflow-runtime"
    [
      ( "runtime",
        [
          Alcotest.test_case "lifecycle and stats" `Quick test_lifecycle_and_stats;
          Alcotest.test_case "grouping filtering and eviction" `Quick
            test_grouping_filtering_and_eviction;
          Alcotest.test_case "validation" `Quick test_validation;
        ] );
    ]
