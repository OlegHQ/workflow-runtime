type status = Queued | Running | Succeeded | Blocked | Failed

type workflow = {
  id : string;
  tenant_id : string;
  kind : string;
  subject_id : string option;
  name : string option;
  metadata : (string * string) list;
}

type item = {
  workflow : workflow;
  status : status;
  started_at_ms : int64 option;
  finished_at_ms : int64 option;
  message : string option;
  updated_at_ms : int64;
}

type stats = {
  total : int;
  queued : int;
  running : int;
  succeeded : int;
  blocked : int;
  failed : int;
}

type t = {
  mutex : Mutex.t;
  workflows : (string, item) Hashtbl.t;
  clock : unit -> int64;
  max_items : int;
}

let create ?(max_items = 1_000) ~clock () =
  if max_items < 1 then invalid_arg "Workflow_runtime.create: max_items < 1";
  { mutex = Mutex.create (); workflows = Hashtbl.create 128; clock; max_items }

let status_to_string = function
  | Queued -> "queued"
  | Running -> "running"
  | Succeeded -> "succeeded"
  | Blocked -> "blocked"
  | Failed -> "failed"

let terminal = function
  | Succeeded | Blocked | Failed -> true
  | Queued | Running -> false

let with_lock t f =
  Mutex.lock t.mutex;
  Fun.protect f ~finally:(fun () -> Mutex.unlock t.mutex)

let initial_item t workflow =
  {
    workflow;
    status = Queued;
    started_at_ms = None;
    finished_at_ms = None;
    message = None;
    updated_at_ms = t.clock ();
  }

let validate_workflow workflow =
  if String.equal workflow.id "" then
    invalid_arg "Workflow_runtime: workflow id must not be empty";
  if String.equal workflow.tenant_id "" then
    invalid_arg "Workflow_runtime: tenant_id must not be empty";
  if String.equal workflow.kind "" then
    invalid_arg "Workflow_runtime: kind must not be empty"

let evict_if_needed t =
  let overflow = Hashtbl.length t.workflows - t.max_items in
  if overflow > 0 then
    let removable =
      t.workflows |> Hashtbl.to_seq_values |> List.of_seq
      |> List.filter (fun item -> terminal item.status)
      |> List.sort (fun a b -> Int64.compare a.updated_at_ms b.updated_at_ms)
    in
    removable |> List.to_seq |> Seq.take overflow
    |> Seq.iter (fun item -> Hashtbl.remove t.workflows item.workflow.id)

let update t workflow f =
  validate_workflow workflow;
  with_lock t (fun () ->
      let previous =
        match Hashtbl.find_opt t.workflows workflow.id with
        | Some item -> { item with workflow }
        | None -> initial_item t workflow
      in
      Hashtbl.replace t.workflows workflow.id (f previous);
      evict_if_needed t)

let record_queued t workflow =
  update t workflow (fun item ->
      { item with status = Queued; message = None; updated_at_ms = t.clock () })

let record_running t workflow =
  let now = t.clock () in
  update t workflow (fun item ->
      {
        item with
        status = Running;
        started_at_ms = Some now;
        finished_at_ms = None;
        message = None;
        updated_at_ms = now;
      })

let finish t workflow status message =
  let now = t.clock () in
  update t workflow (fun item ->
      {
        item with
        status;
        finished_at_ms = Some now;
        message = Some message;
        updated_at_ms = now;
      })

let record_succeeded t workflow message = finish t workflow Succeeded message
let record_blocked t workflow message = finish t workflow Blocked message
let record_failed t workflow message = finish t workflow Failed message

let filtered_items t tenant_id =
  with_lock t (fun () ->
      t.workflows |> Hashtbl.to_seq_values |> List.of_seq
      |> List.filter (fun item ->
          match tenant_id with
          | Some tenant_id -> String.equal item.workflow.tenant_id tenant_id
          | None -> true))

let newest_first items =
  List.sort (fun a b -> Int64.compare b.updated_at_ms a.updated_at_ms) items

let snapshot ?tenant_id t = filtered_items t tenant_id |> newest_first

let grouped_by_tenant ?tenant_id t =
  let groups =
    snapshot ?tenant_id t
    |> List.fold_left
         (fun groups item ->
           let tenant_id = item.workflow.tenant_id in
           let existing =
             Option.value (List.assoc_opt tenant_id groups) ~default:[]
           in
           (tenant_id, item :: existing) :: List.remove_assoc tenant_id groups)
         []
  in
  groups
  |> List.sort (fun (left, _) (right, _) -> String.compare left right)
  |> List.map (fun (tenant_id, items) -> (tenant_id, newest_first items))

let empty_stats =
  { total = 0; queued = 0; running = 0; succeeded = 0; blocked = 0; failed = 0 }

let stats ?tenant_id t =
  snapshot ?tenant_id t
  |> List.fold_left
       (fun stats item ->
         match item.status with
         | Queued -> { stats with total = stats.total + 1; queued = stats.queued + 1 }
         | Running ->
             { stats with total = stats.total + 1; running = stats.running + 1 }
         | Succeeded ->
             {
               stats with
               total = stats.total + 1;
               succeeded = stats.succeeded + 1;
             }
         | Blocked ->
             { stats with total = stats.total + 1; blocked = stats.blocked + 1 }
         | Failed ->
             { stats with total = stats.total + 1; failed = stats.failed + 1 })
       empty_stats

let int64_json value = `Intlit (Int64.to_string value)
let option_json f = function Some value -> f value | None -> `Null

let item_to_yojson item =
  let workflow = item.workflow in
  `Assoc
    [
      ("id", `String workflow.id);
      ("tenant_id", `String workflow.tenant_id);
      ("kind", `String workflow.kind);
      ( "subject_id",
        option_json (fun value -> `String value) workflow.subject_id );
      ("name", option_json (fun value -> `String value) workflow.name);
      ( "metadata",
        `Assoc (List.map (fun (k, v) -> (k, `String v)) workflow.metadata) );
      ("status", `String (status_to_string item.status));
      ("started_at_ms", option_json int64_json item.started_at_ms);
      ("finished_at_ms", option_json int64_json item.finished_at_ms);
      ("message", option_json (fun value -> `String value) item.message);
      ("updated_at_ms", int64_json item.updated_at_ms);
    ]

let stats_json stats =
  `Assoc
    [
      ("total", `Int stats.total);
      ("queued", `Int stats.queued);
      ("running", `Int stats.running);
      ("succeeded", `Int stats.succeeded);
      ("blocked", `Int stats.blocked);
      ("failed", `Int stats.failed);
    ]

let grouped_json groups =
  groups
  |> List.map (fun (tenant_id, items) ->
         (tenant_id, `List (List.map item_to_yojson items)))
  |> fun tenants -> `Assoc [ ("tenants", `Assoc tenants) ]

let snapshot_json ?tenant_id ?(group_by_tenant = false) t =
  let stats = stats ?tenant_id t in
  if group_by_tenant then
    `Assoc
      [
        ("stats", stats_json stats);
        ( "tenants",
          match grouped_json (grouped_by_tenant ?tenant_id t) with
          | `Assoc [ ("tenants", tenants) ] -> tenants
          | _ -> `Assoc [] );
      ]
  else
    `Assoc
      [
        ("stats", stats_json stats);
        ("workflows", `List (List.map item_to_yojson (snapshot ?tenant_id t)));
      ]
