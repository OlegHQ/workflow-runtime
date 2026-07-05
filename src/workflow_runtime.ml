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

type backend_capabilities = {
  durable : bool;
  multi_worker_claims : bool;
  event_history : bool;
  activity_results : bool;
  task_queue_filtering : bool;
  retry_backoff : bool;
  durable_timers : bool;
}

type event_kind =
  | Workflow_enqueued
  | Workflow_claimed
  | Workflow_heartbeat
  | Workflow_completed
  | Workflow_rescheduled
  | Activity_scheduled
  | Activity_started
  | Activity_completed
  | Activity_failed
  | Timer_scheduled
  | Timer_fired

type event = {
  id : string;
  workflow_id : string;
  sequence : int;
  kind : event_kind;
  worker_id : string option;
  payload_json : string option;
  message : string option;
  occurred_at_ms : int64;
}

type activity_status = Activity_succeeded | Activity_failed

type activity_result = {
  activity_id : string;
  workflow_id : string;
  name : string;
  attempt : int;
  status : activity_status;
  result_json : string option;
  error : string option;
  updated_at_ms : int64;
}

type timer = {
  timer_id : string;
  workflow_id : string;
  run_at_ms : int64;
  payload_json : string option;
  fired_at_ms : int64 option;
  created_at_ms : int64;
  updated_at_ms : int64;
}

type retry_policy = {
  max_attempts : int;
  initial_backoff_ms : int64;
  max_backoff_ms : int64;
  backoff_multiplier : float;
}

type retry_decision =
  | Retried of { attempt : int; run_at_ms : int64 }
  | Retries_exhausted of { attempt : int }

let enqueue_options ?(run_at_ms = 0L) ?payload_json () =
  { run_at_ms; payload_json }

let retry_policy ?(max_attempts = 3) ?(initial_backoff_ms = 1_000L)
    ?(max_backoff_ms = 300_000L) ?(backoff_multiplier = 2.0) () =
  { max_attempts; initial_backoff_ms; max_backoff_ms; backoff_multiplier }

let retry_delay_ms policy ~attempt =
  let exponent = max 0 (attempt - 1) in
  let multiplier = policy.backoff_multiplier ** float_of_int exponent in
  let delay =
    Int64.to_float policy.initial_backoff_ms *. multiplier
    |> Float.round |> Int64.of_float
  in
  min delay policy.max_backoff_ms

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

let event_kind_to_string = function
  | Workflow_enqueued -> "workflow_enqueued"
  | Workflow_claimed -> "workflow_claimed"
  | Workflow_heartbeat -> "workflow_heartbeat"
  | Workflow_completed -> "workflow_completed"
  | Workflow_rescheduled -> "workflow_rescheduled"
  | Activity_scheduled -> "activity_scheduled"
  | Activity_started -> "activity_started"
  | Activity_completed -> "activity_completed"
  | Activity_failed -> "activity_failed"
  | Timer_scheduled -> "timer_scheduled"
  | Timer_fired -> "timer_fired"

let event_kind_of_string = function
  | "workflow_enqueued" -> Ok Workflow_enqueued
  | "workflow_claimed" -> Ok Workflow_claimed
  | "workflow_heartbeat" -> Ok Workflow_heartbeat
  | "workflow_completed" -> Ok Workflow_completed
  | "workflow_rescheduled" -> Ok Workflow_rescheduled
  | "activity_scheduled" -> Ok Activity_scheduled
  | "activity_started" -> Ok Activity_started
  | "activity_completed" -> Ok Activity_completed
  | "activity_failed" -> Ok Activity_failed
  | "timer_scheduled" -> Ok Timer_scheduled
  | "timer_fired" -> Ok Timer_fired
  | value -> Error ("unknown workflow event kind: " ^ value)

let activity_status_to_string = function
  | Activity_succeeded -> "succeeded"
  | Activity_failed -> "failed"

let activity_status_of_string = function
  | "succeeded" -> Ok Activity_succeeded
  | "failed" -> Ok Activity_failed
  | value -> Error ("unknown activity status: " ^ value)

let validate_workflow (workflow : workflow) =
  if String.equal workflow.id "" then Error "workflow id must not be empty"
  else if String.equal workflow.tenant_id "" then
    Error "workflow tenant_id must not be empty"
  else if String.equal workflow.kind "" then Error "workflow kind must not be empty"
  else Ok ()

let newest_first (items : item list) =
  List.sort
    (fun (a : item) (b : item) ->
      Int64.compare b.updated_at_ms a.updated_at_ms)
    items

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

let event_to_yojson event =
  `Assoc
    [
      ("id", `String event.id);
      ("workflow_id", `String event.workflow_id);
      ("sequence", `Int event.sequence);
      ("kind", `String (event_kind_to_string event.kind));
      ("worker_id", option_json (fun value -> `String value) event.worker_id);
      ("payload_json", option_json (fun value -> `String value) event.payload_json);
      ("message", option_json (fun value -> `String value) event.message);
      ("occurred_at_ms", int64_json event.occurred_at_ms);
    ]

let activity_result_to_yojson result =
  `Assoc
    [
      ("activity_id", `String result.activity_id);
      ("workflow_id", `String result.workflow_id);
      ("name", `String result.name);
      ("attempt", `Int result.attempt);
      ("status", `String (activity_status_to_string result.status));
      ("result_json", option_json (fun value -> `String value) result.result_json);
      ("error", option_json (fun value -> `String value) result.error);
      ("updated_at_ms", int64_json result.updated_at_ms);
    ]

let timer_to_yojson timer =
  `Assoc
    [
      ("timer_id", `String timer.timer_id);
      ("workflow_id", `String timer.workflow_id);
      ("run_at_ms", int64_json timer.run_at_ms);
      ("payload_json", option_json (fun value -> `String value) timer.payload_json);
      ("fired_at_ms", option_json int64_json timer.fired_at_ms);
      ("created_at_ms", int64_json timer.created_at_ms);
      ("updated_at_ms", int64_json timer.updated_at_ms);
    ]

let empty_stats =
  { total = 0; queued = 0; running = 0; succeeded = 0; blocked = 0; failed = 0 }

let stats (items : item list) =
  List.fold_left
    (fun stats (item : item) ->
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

let items_to_yojson ?(group_by_tenant = false) (items : item list) =
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
  val capabilities : t -> backend_capabilities
  val ensure : t -> (unit, error) result
  val enqueue : t -> now_ms:int64 -> workflow -> enqueue_options -> (unit, error) result

  val claim_next :
    ?kind:string ->
    t ->
    worker_id:string ->
    now_ms:int64 ->
    lease_ms:int64 ->
    (claim option, error) result

  val claim_workflow :
    t ->
    workflow_id:string ->
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

  val retry :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    policy:retry_policy ->
    message:string ->
    (retry_decision option, error) result

  val schedule_timer :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    timer_id:string ->
    run_at_ms:int64 ->
    ?payload_json:string ->
    message:string ->
    unit ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> t -> (item list, error) result
  val history : workflow_id:string -> t -> (event list, error) result
  val timers : workflow_id:string -> t -> (timer list, error) result

  val record_activity_result :
    t -> now_ms:int64 -> activity_result -> (unit, error) result

  val find_activity_result :
    t ->
    workflow_id:string ->
    activity_id:string ->
    (activity_result option, error) result
end

module type CLOCK = sig
  val now_ms : unit -> int64
end

module type S = sig
  type backend
  type error

  val error_to_string : error -> string
  val capabilities : backend -> backend_capabilities
  val ensure : backend -> (unit, error) result
  val enqueue : backend -> workflow -> enqueue_options -> (unit, error) result

  val claim_next :
    ?kind:string ->
    backend ->
    worker_id:string ->
    lease_ms:int64 ->
    (claim option, error) result

  val claim_workflow :
    backend ->
    workflow_id:string ->
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

  val retry :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    policy:retry_policy ->
    message:string ->
    (retry_decision option, error) result

  val schedule_timer :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    timer_id:string ->
    run_at_ms:int64 ->
    ?payload_json:string ->
    message:string ->
    unit ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> backend -> (item list, error) result
  val snapshot_json : ?tenant_id:string -> ?group_by_tenant:bool -> backend -> (Yojson.Safe.t, error) result
  val history : workflow_id:string -> backend -> (event list, error) result
  val timers : workflow_id:string -> backend -> (timer list, error) result

  val record_activity_result :
    backend -> activity_result -> (unit, error) result

  val find_activity_result :
    backend ->
    workflow_id:string ->
    activity_id:string ->
    (activity_result option, error) result
end

module Make (Clock : CLOCK) (Backend : BACKEND) = struct
  type backend = Backend.t
  type error = Backend.error

  let error_to_string = Backend.error_to_string
  let capabilities = Backend.capabilities
  let ensure = Backend.ensure
  let enqueue backend workflow options = Backend.enqueue backend ~now_ms:(Clock.now_ms ()) workflow options
  let claim_next ?kind backend ~worker_id ~lease_ms = Backend.claim_next ?kind backend ~worker_id ~now_ms:(Clock.now_ms ()) ~lease_ms
  let claim_workflow backend ~workflow_id ~worker_id ~lease_ms = Backend.claim_workflow backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~lease_ms
  let heartbeat backend ~workflow_id ~worker_id ~lease_ms = Backend.heartbeat backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~lease_ms
  let complete backend ~workflow_id ~worker_id ~status ~message = Backend.complete backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~status ~message
  let reschedule backend ~workflow_id ~worker_id ~run_at_ms ~message = Backend.reschedule backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~run_at_ms ~message
  let retry backend ~workflow_id ~worker_id ~policy ~message = Backend.retry backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ()) ~policy ~message
  let schedule_timer backend ~workflow_id ~worker_id ~timer_id ~run_at_ms ?payload_json ~message () =
    Backend.schedule_timer backend ~workflow_id ~worker_id ~now_ms:(Clock.now_ms ())
      ~timer_id ~run_at_ms ?payload_json ~message ()
  let snapshot = Backend.snapshot
  let history = Backend.history
  let timers = Backend.timers
  let record_activity_result backend result =
    Backend.record_activity_result backend ~now_ms:(Clock.now_ms ()) result
  let find_activity_result = Backend.find_activity_result

  let snapshot_json ?tenant_id ?(group_by_tenant = false) backend =
    Backend.snapshot ?tenant_id backend
    |> Result.map (items_to_yojson ~group_by_tenant)
end

module Memory_backend = struct
  type error =
    [ `Duplicate_workflow of string
    | `Invalid_workflow of string
    | `Invalid_transition of string ]

  type record = { mutable item : item; mutable event_sequence : int }

  type t = {
    mutex : Mutex.t;
    records : (string, record) Hashtbl.t;
    events : (string, event list) Hashtbl.t;
    activity_results : (string, activity_result) Hashtbl.t;
    timers : (string, timer) Hashtbl.t;
  }

  let create () =
    {
      mutex = Mutex.create ();
      records = Hashtbl.create 128;
      events = Hashtbl.create 128;
      activity_results = Hashtbl.create 128;
      timers = Hashtbl.create 128;
    }

  let error_to_string = function
    | `Duplicate_workflow id -> "workflow already exists: " ^ id
    | `Invalid_workflow message -> message
    | `Invalid_transition message -> message

  let with_lock t f =
    Mutex.lock t.mutex;
    Fun.protect f ~finally:(fun () -> Mutex.unlock t.mutex)

  let ensure _ = Ok ()
  let capabilities _ =
    {
      durable = false;
      multi_worker_claims = false;
      event_history = true;
      activity_results = true;
      task_queue_filtering = true;
      retry_backoff = true;
      durable_timers = false;
    }

  let activity_key ~workflow_id ~activity_id = workflow_id ^ "\000" ^ activity_id
  let timer_key ~workflow_id ~timer_id = workflow_id ^ "\000" ^ timer_id

  let append_event record ~workflow_id ~kind ?worker_id ?payload_json ?message
      ~occurred_at_ms t =
    let sequence = record.event_sequence + 1 in
    record.event_sequence <- sequence;
    let event =
      {
        id = workflow_id ^ ":" ^ string_of_int sequence;
        workflow_id;
        sequence;
        kind;
        worker_id;
        payload_json;
        message;
        occurred_at_ms;
      }
    in
    let existing = Option.value (Hashtbl.find_opt t.events workflow_id) ~default:[] in
    Hashtbl.replace t.events workflow_id (event :: existing)

  let enqueue t ~now_ms workflow (options : enqueue_options) =
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
              let record = { item; event_sequence = 0 } in
              Hashtbl.add t.records workflow.id record;
              append_event record ~workflow_id:workflow.id ~kind:Workflow_enqueued
                ?payload_json:options.payload_json ~occurred_at_ms:now_ms t;
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

  let kind_matches kind (item : item) =
    match kind with
    | None -> true
    | Some kind -> String.equal item.workflow.kind kind

  let timer_payload ~timer_id ~run_at_ms =
    Printf.sprintf {|{"timer_id":%S,"run_at_ms":%Ld}|} timer_id run_at_ms

  let fire_due_timers t record ~now_ms =
    t.timers |> Hashtbl.to_seq_values |> List.of_seq
    |> List.filter (fun timer ->
           String.equal timer.workflow_id record.item.workflow.id
           && timer.run_at_ms <= now_ms
           && Option.is_none timer.fired_at_ms)
    |> List.sort (fun a b ->
           let by_due = Int64.compare a.run_at_ms b.run_at_ms in
           if by_due <> 0 then by_due else String.compare a.timer_id b.timer_id)
    |> List.iter (fun timer ->
           let fired = { timer with fired_at_ms = Some now_ms; updated_at_ms = now_ms } in
           Hashtbl.replace t.timers
             (timer_key ~workflow_id:timer.workflow_id ~timer_id:timer.timer_id)
             fired;
           append_event record ~workflow_id:timer.workflow_id ~kind:Timer_fired
             ?payload_json:timer.payload_json ~message:timer.timer_id
             ~occurred_at_ms:now_ms t)

  let claim_next ?kind t ~worker_id ~now_ms ~lease_ms =
    with_lock t (fun () ->
        let candidate =
          t.records |> Hashtbl.to_seq_values |> List.of_seq
          |> List.filter (fun record ->
                 claimable ~now_ms record.item && kind_matches kind record.item)
          |> List.sort (fun a b ->
                 let by_run = Int64.compare a.item.run_at_ms b.item.run_at_ms in
                 if by_run <> 0 then by_run
                 else String.compare a.item.workflow.id b.item.workflow.id)
          |> List.find_opt (fun _ -> true)
        in
        match candidate with
        | None -> Ok None
        | Some record ->
            let item = record.item in
            let lease_expires_at_ms = Int64.add now_ms lease_ms in
            fire_due_timers t record ~now_ms;
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
            record.item <- claimed;
            append_event record ~workflow_id:item.workflow.id ~kind:Workflow_claimed
              ~worker_id ~occurred_at_ms:now_ms t;
            Ok (Some { item = claimed; worker_id; lease_expires_at_ms }))

  let claim_workflow t ~workflow_id ~worker_id ~now_ms ~lease_ms =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when claimable ~now_ms record.item ->
            let item = record.item in
            let lease_expires_at_ms = Int64.add now_ms lease_ms in
            fire_due_timers t record ~now_ms;
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
            record.item <- claimed;
            append_event record ~workflow_id ~kind:Workflow_claimed ~worker_id
              ~occurred_at_ms:now_ms t;
            Ok (Some { item = claimed; worker_id; lease_expires_at_ms })
        | _ -> Ok None)

  let owned_by item worker_id =
    match item.lease_owner with
    | Some owner -> String.equal owner worker_id
    | None -> false

  let schedule_timer t ~workflow_id ~worker_id ~now_ms ~timer_id ~run_at_ms
      ?payload_json ~message () =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when owned_by record.item worker_id ->
            let item = record.item in
            let timer =
              {
                timer_id;
                workflow_id;
                run_at_ms;
                payload_json;
                fired_at_ms = None;
                created_at_ms = now_ms;
                updated_at_ms = now_ms;
              }
            in
            Hashtbl.replace t.timers (timer_key ~workflow_id ~timer_id) timer;
            record.item <-
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
            append_event record ~workflow_id ~kind:Timer_scheduled ~worker_id
              ~payload_json:(timer_payload ~timer_id ~run_at_ms)
              ~message ~occurred_at_ms:now_ms t;
            Ok true
        | _ -> Ok false)

  let heartbeat t ~workflow_id ~worker_id ~now_ms ~lease_ms =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when owned_by record.item worker_id ->
            let item = record.item in
            let next =
              {
                item with
                lease_expires_at_ms = Some (Int64.add now_ms lease_ms);
                updated_at_ms = now_ms;
              }
            in
            record.item <- next;
            append_event record ~workflow_id ~kind:Workflow_heartbeat ~worker_id
              ~occurred_at_ms:now_ms t;
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
          | Some record when owned_by record.item worker_id ->
              let item = record.item in
              record.item <-
                {
                  item with
                  status;
                  lease_owner = None;
                  lease_expires_at_ms = None;
                  finished_at_ms = Some now_ms;
                  message = Some message;
                  updated_at_ms = now_ms;
                };
              append_event record ~workflow_id ~kind:Workflow_completed
                ~worker_id ~message ~occurred_at_ms:now_ms t;
              Ok true
          | _ -> Ok false)

  let reschedule t ~workflow_id ~worker_id ~now_ms ~run_at_ms ~message =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when owned_by record.item worker_id ->
            let item = record.item in
            record.item <-
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
            append_event record ~workflow_id ~kind:Workflow_rescheduled
              ~worker_id ~message ~occurred_at_ms:now_ms t;
            Ok true
        | _ -> Ok false)

  let retry t ~workflow_id ~worker_id ~now_ms ~policy ~message =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when owned_by record.item worker_id ->
            let item = record.item in
            if item.attempt >= policy.max_attempts then (
              record.item <-
                {
                  item with
                  status = Failed;
                  lease_owner = None;
                  lease_expires_at_ms = None;
                  finished_at_ms = Some now_ms;
                  message = Some message;
                  updated_at_ms = now_ms;
                };
              append_event record ~workflow_id ~kind:Workflow_completed
                ~worker_id ~message ~occurred_at_ms:now_ms t;
              Ok (Some (Retries_exhausted { attempt = item.attempt })))
            else
              let run_at_ms =
                Int64.add now_ms (retry_delay_ms policy ~attempt:item.attempt)
              in
              record.item <-
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
              append_event record ~workflow_id ~kind:Workflow_rescheduled
                ~worker_id ~message ~occurred_at_ms:now_ms t;
              Ok (Some (Retried { attempt = item.attempt; run_at_ms }))
        | _ -> Ok None)

  let snapshot ?tenant_id t =
    with_lock t (fun () ->
      t.records |> Hashtbl.to_seq_values |> List.of_seq
      |> List.map (fun record -> record.item)
      |> List.filter (fun item ->
               match tenant_id with
               | Some tenant_id -> String.equal item.workflow.tenant_id tenant_id
               | None -> true)
        |> newest_first |> Result.ok)

  let history ~workflow_id t =
    with_lock t (fun () ->
        Hashtbl.find_opt t.events workflow_id
        |> Option.value ~default:[]
        |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
        |> Result.ok)

  let timers ~workflow_id t =
    with_lock t (fun () ->
        t.timers |> Hashtbl.to_seq_values |> List.of_seq
        |> List.filter (fun timer -> String.equal timer.workflow_id workflow_id)
        |> List.sort (fun a b ->
               let by_due = Int64.compare a.run_at_ms b.run_at_ms in
               if by_due <> 0 then by_due else String.compare a.timer_id b.timer_id)
        |> Result.ok)

  let record_activity_result t ~now_ms (result : activity_result) =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records result.workflow_id with
        | None -> Error (`Invalid_workflow ("unknown workflow: " ^ result.workflow_id))
        | Some record ->
            let result = { result with updated_at_ms = now_ms } in
            Hashtbl.replace t.activity_results
              (activity_key ~workflow_id:result.workflow_id
                 ~activity_id:result.activity_id)
              result;
            let kind =
              match result.status with
              | Activity_succeeded -> Activity_completed
              | Activity_failed -> Activity_failed
            in
            append_event record ~workflow_id:result.workflow_id ~kind
              ?payload_json:result.result_json ?message:result.error
              ~occurred_at_ms:now_ms t;
            Ok ())

  let find_activity_result t ~workflow_id ~activity_id =
    with_lock t (fun () ->
        Hashtbl.find_opt t.activity_results
          (activity_key ~workflow_id ~activity_id)
        |> Result.ok)
end
