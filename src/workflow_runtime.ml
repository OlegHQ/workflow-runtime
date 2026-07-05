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
  run_at_ms : int64;
  attempt : int;
  lease_owner : string option;
  lease_expires_at_ms : int64 option;
  payload_json : string option;
  started_at_ms : int64 option;
  finished_at_ms : int64 option;
  message : string option;
  created_at_ms : int64;
  updated_at_ms : int64;
}

type claim = { item : item; worker_id : string; lease_expires_at_ms : int64 }

type enqueue_options = {
  run_at_ms : int64;
  payload_json : string option;
}

type stats = {
  total : int;
  queued : int;
  running : int;
  succeeded : int;
  blocked : int;
  failed : int;
}

let enqueue_options ?(run_at_ms = 0L) ?payload_json () =
  { run_at_ms; payload_json }

let status_to_string = function
  | Queued -> "queued"
  | Running -> "running"
  | Succeeded -> "succeeded"
  | Blocked -> "blocked"
  | Failed -> "failed"

let status_of_string = function
  | "queued" -> Ok Queued
  | "running" -> Ok Running
  | "succeeded" -> Ok Succeeded
  | "blocked" -> Ok Blocked
  | "failed" -> Ok Failed
  | value -> Error ("unknown workflow status: " ^ value)

let validate_workflow workflow =
  if String.equal workflow.id "" then Error "workflow id must not be empty"
  else if String.equal workflow.tenant_id "" then
    Error "workflow tenant_id must not be empty"
  else if String.equal workflow.kind "" then Error "workflow kind must not be empty"
  else Ok ()

let newest_first items =
  List.sort (fun a b -> Int64.compare b.updated_at_ms a.updated_at_ms) items

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
      ("run_at_ms", int64_json item.run_at_ms);
      ("attempt", `Int item.attempt);
      ("lease_owner", option_json (fun value -> `String value) item.lease_owner);
      ("lease_expires_at_ms", option_json int64_json item.lease_expires_at_ms);
      ("payload_json", option_json (fun value -> `String value) item.payload_json);
      ("started_at_ms", option_json int64_json item.started_at_ms);
      ("finished_at_ms", option_json int64_json item.finished_at_ms);
      ("message", option_json (fun value -> `String value) item.message);
      ("created_at_ms", int64_json item.created_at_ms);
      ("updated_at_ms", int64_json item.updated_at_ms);
    ]

let empty_stats =
  { total = 0; queued = 0; running = 0; succeeded = 0; blocked = 0; failed = 0 }

let stats items =
  List.fold_left
    (fun stats item ->
      match item.status with
      | Queued -> { stats with total = stats.total + 1; queued = stats.queued + 1 }
      | Running ->
          { stats with total = stats.total + 1; running = stats.running + 1 }
      | Succeeded ->
          { stats with total = stats.total + 1; succeeded = stats.succeeded + 1 }
      | Blocked ->
          { stats with total = stats.total + 1; blocked = stats.blocked + 1 }
      | Failed -> { stats with total = stats.total + 1; failed = stats.failed + 1 })
    empty_stats items

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

let items_to_yojson ?(group_by_tenant = false) items =
  let items = newest_first items in
  if group_by_tenant then
    let groups =
      List.fold_left
        (fun groups item ->
          let tenant_id = item.workflow.tenant_id in
          let existing = Option.value (List.assoc_opt tenant_id groups) ~default:[] in
          (tenant_id, item :: existing) :: List.remove_assoc tenant_id groups)
        [] items
      |> List.sort (fun (left, _) (right, _) -> String.compare left right)
    in
    `Assoc
      [
        ("stats", stats_json (stats items));
        ( "tenants",
          `Assoc
            (List.map
               (fun (tenant_id, items) ->
                 (tenant_id, `List (items |> newest_first |> List.map item_to_yojson)))
               groups) );
      ]
  else
    `Assoc
      [
        ("stats", stats_json (stats items));
        ("workflows", `List (List.map item_to_yojson items));
      ]

module type BACKEND = sig
  type t
  type error

  val error_to_string : error -> string
  val ensure : t -> (unit, error) result
  val enqueue : t -> now_ms:int64 -> workflow -> enqueue_options -> (unit, error) result

  val claim_next :
    t ->
    worker_id:string ->
    now_ms:int64 ->
    lease_ms:int64 ->
    (claim option, error) result

  val heartbeat :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    lease_ms:int64 ->
    (bool, error) result

  val complete :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    status:status ->
    message:string ->
    (bool, error) result

  val reschedule :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    run_at_ms:int64 ->
    message:string ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> t -> (item list, error) result
end

module type CLOCK = sig
  val now_ms : unit -> int64
end

module type S = sig
  type backend
  type error

  val error_to_string : error -> string
  val ensure : backend -> (unit, error) result
  val enqueue : backend -> workflow -> enqueue_options -> (unit, error) result

  val claim_next :
    backend ->
    worker_id:string ->
    lease_ms:int64 ->
    (claim option, error) result

  val heartbeat :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    lease_ms:int64 ->
    (bool, error) result

  val complete :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    status:status ->
    message:string ->
    (bool, error) result

  val reschedule :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    run_at_ms:int64 ->
    message:string ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> backend -> (item list, error) result
  val snapshot_json : ?tenant_id:string -> ?group_by_tenant:bool -> backend -> (Yojson.Safe.t, error) result
end

module Make (Clock : CLOCK) (Backend : BACKEND) = struct
  type backend = Backend.t
  type error = Backend.error

  let error_to_string = Backend.error_to_string
  let ensure = Backend.ensure
  let enqueue backend workflow options = Backend.enqueue backend ~now_ms:(Clock.now_ms ()) workflow options
  let claim_next backend ~worker_id ~lease_ms = Backend.claim_next backend ~worker_id ~now_ms:(Clock.now_ms ()) ~lease_ms
  let heartbeat backend ~workflow_id ~worker_id ~lease_ms = Backend.heartbeat backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~lease_ms
  let complete backend ~workflow_id ~worker_id ~status ~message = Backend.complete backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~status ~message
  let reschedule backend ~workflow_id ~worker_id ~run_at_ms ~message = Backend.reschedule backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~run_at_ms ~message
  let snapshot = Backend.snapshot

  let snapshot_json ?tenant_id ?(group_by_tenant = false) backend =
    Backend.snapshot ?tenant_id backend
    |> Result.map (items_to_yojson ~group_by_tenant)
end

module Memory_backend = struct
  type error =
    [ `Duplicate_workflow of string
    | `Invalid_workflow of string
    | `Invalid_transition of string ]

  type t = { mutex : Mutex.t; records : (string, item) Hashtbl.t }

  let create () = { mutex = Mutex.create (); records = Hashtbl.create 128 }

  let error_to_string = function
    | `Duplicate_workflow id -> "workflow already exists: " ^ id
    | `Invalid_workflow message -> message
    | `Invalid_transition message -> message

  let with_lock t f =
    Mutex.lock t.mutex;
    Fun.protect f ~finally:(fun () -> Mutex.unlock t.mutex)

  let ensure _ = Ok ()

  let enqueue t ~now_ms workflow options =
    match validate_workflow workflow with
    | Error message -> Error (`Invalid_workflow message)
    | Ok () ->
        with_lock t (fun () ->
            if Hashtbl.mem t.records workflow.id then
              Error (`Duplicate_workflow workflow.id)
            else
              let item =
                {
                  workflow;
                  status = Queued;
                  run_at_ms = options.run_at_ms;
                  attempt = 0;
                  lease_owner = None;
                  lease_expires_at_ms = None;
                  payload_json = options.payload_json;
                  started_at_ms = None;
                  finished_at_ms = None;
                  message = None;
                  created_at_ms = now_ms;
                  updated_at_ms = now_ms;
                }
              in
              Hashtbl.add t.records workflow.id item;
              Ok ())

  let lease_available ~now_ms (item : item) =
    match item.lease_expires_at_ms with
    | None -> true
    | Some expires -> expires <= now_ms

  let claimable ~now_ms (item : item) =
    item.run_at_ms <= now_ms
    &&
    match item.status with
    | Queued | Running -> lease_available ~now_ms item
    | Blocked | Succeeded | Failed -> false

  let claim_next t ~worker_id ~now_ms ~lease_ms =
    with_lock t (fun () ->
        let candidate =
          t.records |> Hashtbl.to_seq_values |> List.of_seq
          |> fun (items : item list) -> items
          |> List.filter (claimable ~now_ms)
          |> List.sort (fun (a : item) (b : item) ->
                 let by_run = Int64.compare a.run_at_ms b.run_at_ms in
                 if by_run <> 0 then by_run
                 else String.compare a.workflow.id b.workflow.id)
          |> List.find_opt (fun _ -> true)
        in
        match candidate with
        | None -> Ok None
        | Some item ->
            let lease_expires_at_ms = Int64.add now_ms lease_ms in
            let claimed =
              {
                item with
                status = Running;
                attempt = item.attempt + 1;
                lease_owner = Some worker_id;
                lease_expires_at_ms = Some lease_expires_at_ms;
                started_at_ms = Some (Option.value item.started_at_ms ~default:now_ms);
                finished_at_ms = None;
                updated_at_ms = now_ms;
              }
            in
            Hashtbl.replace t.records item.workflow.id claimed;
            Ok (Some { item = claimed; worker_id; lease_expires_at_ms }))

  let owned_by item worker_id =
    match item.lease_owner with
    | Some owner -> String.equal owner worker_id
    | None -> false

  let heartbeat t ~workflow_id ~worker_id ~now_ms ~lease_ms =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some item when owned_by item worker_id ->
            let next =
              {
                item with
                lease_expires_at_ms = Some (Int64.add now_ms lease_ms);
                updated_at_ms = now_ms;
              }
            in
            Hashtbl.replace t.records workflow_id next;
            Ok true
        | _ -> Ok false)

  let terminal_status = function
    | Succeeded | Blocked | Failed -> true
    | Queued | Running -> false

  let complete t ~workflow_id ~worker_id ~now_ms ~status ~message =
    if not (terminal_status status) then
      Error (`Invalid_transition "complete requires a terminal status")
    else
      with_lock t (fun () ->
          match Hashtbl.find_opt t.records workflow_id with
          | Some item when owned_by item worker_id ->
              Hashtbl.replace t.records workflow_id
                {
                  item with
                  status;
                  lease_owner = None;
                  lease_expires_at_ms = None;
                  finished_at_ms = Some now_ms;
                  message = Some message;
                  updated_at_ms = now_ms;
                };
              Ok true
          | _ -> Ok false)

  let reschedule t ~workflow_id ~worker_id ~now_ms ~run_at_ms ~message =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some item when owned_by item worker_id ->
            Hashtbl.replace t.records workflow_id
              {
                item with
                status = Queued;
                run_at_ms;
                lease_owner = None;
                lease_expires_at_ms = None;
                finished_at_ms = None;
                message = Some message;
                updated_at_ms = now_ms;
              };
            Ok true
        | _ -> Ok false)

  let snapshot ?tenant_id t =
    with_lock t (fun () ->
        t.records |> Hashtbl.to_seq_values |> List.of_seq
        |> List.filter (fun item ->
               match tenant_id with
               | Some tenant_id -> String.equal item.workflow.tenant_id tenant_id
               | None -> true)
        |> newest_first |> Result.ok)
end
