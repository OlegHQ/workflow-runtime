type status = Queued | Running | Succeeded | Blocked | Failed | Cancelled

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
  cancelled : int;
}

type backend_capabilities = {
  durable : bool;
  multi_worker_claims : bool;
  event_history : bool;
  activity_results : bool;
  task_queue_filtering : bool;
  retry_backoff : bool;
  durable_timers : bool;
  deterministic_replay : bool;
  signals : bool;
  queries : bool;
  history_compaction : bool;
  cancellation : bool;
  child_workflows : bool;
  updates : bool;
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
  | Signal_received
  | History_compacted
  | Workflow_cancelled
  | Child_workflow_started
  | Update_requested
  | Update_completed

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

type signal = {
  signal_id : string;
  workflow_id : string;
  name : string;
  payload_json : string option;
  received_at_ms : int64;
}

type child_workflow = {
  parent_workflow_id : string;
  child_workflow_id : string;
  child_kind : string;
  started_at_ms : int64;
}

type update_status =
  | Update_pending
  | Update_completed_status
  | Update_rejected
  | Update_failed

type workflow_update = {
  update_id : string;
  workflow_id : string;
  name : string;
  payload_json : string option;
  status : update_status;
  result_json : string option;
  error : string option;
  requested_at_ms : int64;
  completed_at_ms : int64 option;
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

type replay_completion = {
  status : status;
  message : string option;
  completed_at_ms : int64;
}

type replay_timer = {
  timer_id : string;
  run_at_ms : int64;
  scheduled_at_ms : int64;
  fired_at_ms : int64 option;
}

type replay_activity = {
  activity_id : string;
  name : string;
  attempt : int;
  status : activity_status;
  result_json : string option;
  error : string option;
  completed_at_ms : int64;
}

type replay_state = {
  workflow_id : string option;
  enqueued_at_ms : int64 option;
  claim_count : int;
  completion : replay_completion option;
  timers : replay_timer list;
  activities : replay_activity list;
  signals : signal list;
  child_workflows : child_workflow list;
  updates : workflow_update list;
  compacted_at_sequence : int option;
}

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
  | Cancelled -> "cancelled"

let status_of_string = function
  | "queued" -> Ok Queued
  | "running" -> Ok Running
  | "succeeded" -> Ok Succeeded
  | "blocked" -> Ok Blocked
  | "failed" -> Ok Failed
  | "cancelled" -> Ok Cancelled
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
  | Signal_received -> "signal_received"
  | History_compacted -> "history_compacted"
  | Workflow_cancelled -> "workflow_cancelled"
  | Child_workflow_started -> "child_workflow_started"
  | Update_requested -> "update_requested"
  | Update_completed -> "update_completed"

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
  | "signal_received" -> Ok Signal_received
  | "history_compacted" -> Ok History_compacted
  | "workflow_cancelled" -> Ok Workflow_cancelled
  | "child_workflow_started" -> Ok Child_workflow_started
  | "update_requested" -> Ok Update_requested
  | "update_completed" -> Ok Update_completed
  | value -> Error ("unknown workflow event kind: " ^ value)

let activity_status_to_string = function
  | Activity_succeeded -> "succeeded"
  | Activity_failed -> "failed"

let activity_status_of_string = function
  | "succeeded" -> Ok Activity_succeeded
  | "failed" -> Ok Activity_failed
  | value -> Error ("unknown activity status: " ^ value)

let update_status_to_string = function
  | Update_pending -> "pending"
  | Update_completed_status -> "completed"
  | Update_rejected -> "rejected"
  | Update_failed -> "failed"

let update_status_of_string = function
  | "pending" -> Ok Update_pending
  | "completed" -> Ok Update_completed_status
  | "rejected" -> Ok Update_rejected
  | "failed" -> Ok Update_failed
  | value -> Error ("unknown update status: " ^ value)

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
let string_option_json = option_json (fun value -> `String value)

let string_option_member json name =
  match Yojson.Safe.Util.member name json with
  | `Null -> Ok None
  | `String value -> Ok (Some value)
  | _ -> Error ("expected string or null field: " ^ name)

let string_member json name =
  match Yojson.Safe.Util.member name json with
  | `String value -> Ok value
  | _ -> Error ("expected string field: " ^ name)

let int_member json name =
  match Yojson.Safe.Util.member name json with
  | `Int value -> Ok value
  | _ -> Error ("expected int field: " ^ name)

let int64_member json name =
  match Yojson.Safe.Util.member name json with
  | `Int value -> Ok (Int64.of_int value)
  | `Intlit value -> (
      match Int64.of_string_opt value with
      | Some value -> Ok value
      | None -> Error ("expected int64 field: " ^ name))
  | _ -> Error ("expected int64 field: " ^ name)

let payload_json (event : event) =
  match event.payload_json with
  | None -> Error ("event missing payload: " ^ event_kind_to_string event.kind)
  | Some payload -> (
      try Ok (Yojson.Safe.from_string payload)
      with Yojson.Json_error message -> Error message)

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
      ("payload_json", string_option_json item.payload_json);
      ("started_at_ms", option_json int64_json item.started_at_ms);
      ("finished_at_ms", option_json int64_json item.finished_at_ms);
      ("message", option_json (fun value -> `String value) item.message);
      ("created_at_ms", int64_json item.created_at_ms);
      ("updated_at_ms", int64_json item.updated_at_ms);
    ]

let event_to_yojson (event : event) =
  `Assoc
    [
      ("id", `String event.id);
      ("workflow_id", `String event.workflow_id);
      ("sequence", `Int event.sequence);
      ("kind", `String (event_kind_to_string event.kind));
      ("worker_id", string_option_json event.worker_id);
      ("payload_json", string_option_json event.payload_json);
      ("message", string_option_json event.message);
      ("occurred_at_ms", int64_json event.occurred_at_ms);
    ]

let activity_result_to_yojson (result : activity_result) =
  `Assoc
    [
      ("activity_id", `String result.activity_id);
      ("workflow_id", `String result.workflow_id);
      ("name", `String result.name);
      ("attempt", `Int result.attempt);
      ("status", `String (activity_status_to_string result.status));
      ("result_json", string_option_json result.result_json);
      ("error", string_option_json result.error);
      ("updated_at_ms", int64_json result.updated_at_ms);
    ]

let timer_to_yojson (timer : timer) =
  `Assoc
    [
      ("timer_id", `String timer.timer_id);
      ("workflow_id", `String timer.workflow_id);
      ("run_at_ms", int64_json timer.run_at_ms);
      ("payload_json", string_option_json timer.payload_json);
      ("fired_at_ms", option_json int64_json timer.fired_at_ms);
      ("created_at_ms", int64_json timer.created_at_ms);
      ("updated_at_ms", int64_json timer.updated_at_ms);
    ]

let signal_to_yojson (signal : signal) =
  `Assoc
    [
      ("signal_id", `String signal.signal_id);
      ("workflow_id", `String signal.workflow_id);
      ("name", `String signal.name);
      ("payload_json", string_option_json signal.payload_json);
      ("received_at_ms", int64_json signal.received_at_ms);
    ]

let child_workflow_to_yojson (child : child_workflow) =
  `Assoc
    [
      ("parent_workflow_id", `String child.parent_workflow_id);
      ("child_workflow_id", `String child.child_workflow_id);
      ("child_kind", `String child.child_kind);
      ("started_at_ms", int64_json child.started_at_ms);
    ]

let workflow_update_to_yojson (update : workflow_update) =
  `Assoc
    [
      ("update_id", `String update.update_id);
      ("workflow_id", `String update.workflow_id);
      ("name", `String update.name);
      ("payload_json", string_option_json update.payload_json);
      ("status", `String (update_status_to_string update.status));
      ("result_json", string_option_json update.result_json);
      ("error", string_option_json update.error);
      ("requested_at_ms", int64_json update.requested_at_ms);
      ("completed_at_ms", option_json int64_json update.completed_at_ms);
    ]

let replay_completion_to_yojson (completion : replay_completion) =
  `Assoc
    [
      ("status", `String (status_to_string completion.status));
      ("message", string_option_json completion.message);
      ("completed_at_ms", int64_json completion.completed_at_ms);
    ]

let replay_timer_to_yojson (timer : replay_timer) =
  `Assoc
    [
      ("timer_id", `String timer.timer_id);
      ("run_at_ms", int64_json timer.run_at_ms);
      ("scheduled_at_ms", int64_json timer.scheduled_at_ms);
      ("fired_at_ms", option_json int64_json timer.fired_at_ms);
    ]

let replay_activity_to_yojson (activity : replay_activity) =
  `Assoc
    [
      ("activity_id", `String activity.activity_id);
      ("name", `String activity.name);
      ("attempt", `Int activity.attempt);
      ("status", `String (activity_status_to_string activity.status));
      ("result_json", string_option_json activity.result_json);
      ("error", string_option_json activity.error);
      ("completed_at_ms", int64_json activity.completed_at_ms);
    ]

let replay_state_to_yojson (state : replay_state) =
  `Assoc
    [
      ("workflow_id", string_option_json state.workflow_id);
      ("enqueued_at_ms", option_json int64_json state.enqueued_at_ms);
      ("claim_count", `Int state.claim_count);
      ( "completion",
        option_json replay_completion_to_yojson state.completion );
      ("timers", `List (List.map replay_timer_to_yojson state.timers));
      ("activities", `List (List.map replay_activity_to_yojson state.activities));
      ("signals", `List (List.map signal_to_yojson state.signals));
      ( "child_workflows",
        `List (List.map child_workflow_to_yojson state.child_workflows) );
      ("updates", `List (List.map workflow_update_to_yojson state.updates));
      ( "compacted_at_sequence",
        option_json (fun value -> `Int value) state.compacted_at_sequence );
    ]

let completion_payload ~status ~message =
  `Assoc
    [
      ("status", `String (status_to_string status));
      ("message", string_option_json (Some message));
    ]
  |> Yojson.Safe.to_string

let timer_payload ~timer_id ~run_at_ms =
  `Assoc
    [
      ("timer_id", `String timer_id);
      ("run_at_ms", int64_json run_at_ms);
    ]
  |> Yojson.Safe.to_string

let activity_payload (result : activity_result) =
  `Assoc
    [
      ("activity_id", `String result.activity_id);
      ("name", `String result.name);
      ("attempt", `Int result.attempt);
      ("status", `String (activity_status_to_string result.status));
      ("result_json", string_option_json result.result_json);
      ("error", string_option_json result.error);
    ]
  |> Yojson.Safe.to_string

let signal_payload (signal : signal) =
  `Assoc
    [
      ("signal_id", `String signal.signal_id);
      ("name", `String signal.name);
      ("payload_json", string_option_json signal.payload_json);
    ]
  |> Yojson.Safe.to_string

let child_workflow_payload (child : child_workflow) =
  child_workflow_to_yojson child |> Yojson.Safe.to_string

let update_payload (update : workflow_update) =
  workflow_update_to_yojson update |> Yojson.Safe.to_string

let empty_replay_state =
  {
    workflow_id = None;
    enqueued_at_ms = None;
    claim_count = 0;
    completion = None;
    timers = [];
    activities = [];
    signals = [];
    child_workflows = [];
    updates = [];
    compacted_at_sequence = None;
  }

let replay_state_of_yojson json =
  let ( let* ) = Result.bind in
  let list_member json name decode =
    match Yojson.Safe.Util.member name json with
    | `Null -> Ok []
    | `List values ->
        List.fold_left
          (fun acc value ->
            match acc with
            | Error _ as error -> error
            | Ok values ->
                let* decoded = decode value in
                Ok (decoded :: values))
          (Ok []) values
        |> Result.map List.rev
    | _ -> Error ("expected list field: " ^ name)
  in
  let workflow_id =
    match Yojson.Safe.Util.member "workflow_id" json with
    | `Null -> Ok None
    | `String value -> Ok (Some value)
    | _ -> Error "expected workflow_id string or null"
  in
  let enqueued_at_ms =
    match Yojson.Safe.Util.member "enqueued_at_ms" json with
    | `Null -> Ok None
    | value ->
        int64_member (`Assoc [ ("value", value) ]) "value" |> Result.map Option.some
  in
  let claim_count =
    match Yojson.Safe.Util.member "claim_count" json with
    | `Null -> Ok 0
    | `Int value -> Ok value
    | _ -> Error "expected claim_count int"
  in
  let completion =
    match Yojson.Safe.Util.member "completion" json with
    | `Null -> Ok None
    | value ->
        let* status =
          let* status = string_member value "status" in
          status_of_string status
        in
        let* message = string_option_member value "message" in
        let* completed_at_ms = int64_member value "completed_at_ms" in
        Ok (Some { status; message; completed_at_ms })
  in
  let timers =
    list_member json "timers" (fun value ->
        let* timer_id = string_member value "timer_id" in
        let* run_at_ms = int64_member value "run_at_ms" in
        let* scheduled_at_ms = int64_member value "scheduled_at_ms" in
        let fired_at_ms =
          match Yojson.Safe.Util.member "fired_at_ms" value with
          | `Null -> Ok None
          | value ->
              int64_member (`Assoc [ ("value", value) ]) "value"
              |> Result.map Option.some
        in
        let* fired_at_ms = fired_at_ms in
        Ok { timer_id; run_at_ms; scheduled_at_ms; fired_at_ms })
  in
  let activities =
    list_member json "activities" (fun value ->
        let* activity_id = string_member value "activity_id" in
        let* name = string_member value "name" in
        let* attempt = int_member value "attempt" in
        let* status =
          let* status = string_member value "status" in
          activity_status_of_string status
        in
        let* result_json = string_option_member value "result_json" in
        let* error = string_option_member value "error" in
        let* completed_at_ms = int64_member value "completed_at_ms" in
        Ok
          {
            activity_id;
            name;
            attempt;
            status;
            result_json;
            error;
            completed_at_ms;
          })
  in
  let signals =
    list_member json "signals" (fun value ->
        let* signal_id = string_member value "signal_id" in
        let* workflow_id = string_member value "workflow_id" in
        let* name = string_member value "name" in
        let* payload_json = string_option_member value "payload_json" in
        let* received_at_ms = int64_member value "received_at_ms" in
        Ok { signal_id; workflow_id; name; payload_json; received_at_ms })
  in
  let child_workflows =
    list_member json "child_workflows" (fun value ->
        let* parent_workflow_id = string_member value "parent_workflow_id" in
        let* child_workflow_id = string_member value "child_workflow_id" in
        let* child_kind = string_member value "child_kind" in
        let* started_at_ms = int64_member value "started_at_ms" in
        Ok { parent_workflow_id; child_workflow_id; child_kind; started_at_ms })
  in
  let updates =
    list_member json "updates" (fun value ->
        let* update_id = string_member value "update_id" in
        let* workflow_id = string_member value "workflow_id" in
        let* name = string_member value "name" in
        let* payload_json = string_option_member value "payload_json" in
        let* status =
          let* status = string_member value "status" in
          update_status_of_string status
        in
        let* result_json = string_option_member value "result_json" in
        let* error = string_option_member value "error" in
        let* requested_at_ms = int64_member value "requested_at_ms" in
        let completed_at_ms =
          match Yojson.Safe.Util.member "completed_at_ms" value with
          | `Null -> Ok None
          | value ->
              int64_member (`Assoc [ ("value", value) ]) "value"
              |> Result.map Option.some
        in
        let* completed_at_ms = completed_at_ms in
        Ok
          {
            update_id;
            workflow_id;
            name;
            payload_json;
            status;
            result_json;
            error;
            requested_at_ms;
            completed_at_ms;
          })
  in
  let compacted_at_sequence =
    match Yojson.Safe.Util.member "compacted_at_sequence" json with
    | `Null -> Ok None
    | `Int value -> Ok (Some value)
    | _ -> Error "expected compacted_at_sequence int or null"
  in
  let* workflow_id = workflow_id in
  let* enqueued_at_ms = enqueued_at_ms in
  let* claim_count = claim_count in
  let* completion = completion in
  let* timers = timers in
  let* activities = activities in
  let* signals = signals in
  let* child_workflows = child_workflows in
  let* updates = updates in
  let* compacted_at_sequence = compacted_at_sequence in
  Ok
    {
      workflow_id;
      enqueued_at_ms;
      claim_count;
      completion;
      timers;
      activities;
      signals;
      child_workflows;
      updates;
      compacted_at_sequence;
    }

let upsert_update updates update =
  update
  :: List.filter
       (fun existing -> not (String.equal existing.update_id update.update_id))
       updates

let upsert_timer timers timer =
  timer :: List.filter (fun existing -> not (String.equal existing.timer_id timer.timer_id)) timers

let fire_timer timers ~timer_id ~fired_at_ms =
  List.map
    (fun timer ->
      if String.equal timer.timer_id timer_id then { timer with fired_at_ms = Some fired_at_ms }
      else timer)
    timers

let replay_event state (event : event) =
  let workflow_id =
    match state.workflow_id with
    | None -> Some event.workflow_id
    | Some existing when String.equal existing event.workflow_id -> Some existing
    | Some existing ->
        invalid_arg
          ("history contains multiple workflow ids: " ^ existing ^ " and "
         ^ event.workflow_id)
  in
  let state = { state with workflow_id } in
  match event.kind with
  | History_compacted ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* compacted = replay_state_of_yojson json in
      Ok { compacted with compacted_at_sequence = Some event.sequence }
  | Workflow_enqueued ->
      Ok { state with enqueued_at_ms = Some event.occurred_at_ms }
  | Workflow_claimed ->
      Ok { state with claim_count = state.claim_count + 1 }
  | Workflow_completed ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* status =
        let* status = string_member json "status" in
        status_of_string status
      in
      let* message = string_option_member json "message" in
      Ok
        {
          state with
          completion =
            Some { status; message; completed_at_ms = event.occurred_at_ms };
        }
  | Workflow_cancelled ->
      Ok
        {
          state with
          completion =
            Some
              {
                status = Cancelled;
                message = event.message;
                completed_at_ms = event.occurred_at_ms;
              };
        }
  | Child_workflow_started ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* parent_workflow_id = string_member json "parent_workflow_id" in
      let* child_workflow_id = string_member json "child_workflow_id" in
      let* child_kind = string_member json "child_kind" in
      let* started_at_ms = int64_member json "started_at_ms" in
      Ok
        {
          state with
          child_workflows =
            {
              parent_workflow_id;
              child_workflow_id;
              child_kind;
              started_at_ms;
            }
            :: List.filter
                 (fun existing ->
                   not (String.equal existing.child_workflow_id child_workflow_id))
                 state.child_workflows;
        }
  | Update_requested ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* update_id = string_member json "update_id" in
      let* name = string_member json "name" in
      let* payload_json = string_option_member json "payload_json" in
      Ok
        {
          state with
          updates =
            upsert_update state.updates
              {
                update_id;
                workflow_id = event.workflow_id;
                name;
                payload_json;
                status = Update_pending;
                result_json = None;
                error = None;
                requested_at_ms = event.occurred_at_ms;
                completed_at_ms = None;
              };
        }
  | Update_completed ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* update_id = string_member json "update_id" in
      let* workflow_id = string_member json "workflow_id" in
      let* name = string_member json "name" in
      let* payload_json = string_option_member json "payload_json" in
      let* status =
        let* status = string_member json "status" in
        update_status_of_string status
      in
      let* result_json = string_option_member json "result_json" in
      let* error = string_option_member json "error" in
      let* requested_at_ms = int64_member json "requested_at_ms" in
      Ok
        {
          state with
          updates =
            upsert_update state.updates
              {
                update_id;
                workflow_id;
                name;
                payload_json;
                status;
                result_json;
                error;
                requested_at_ms;
                completed_at_ms = Some event.occurred_at_ms;
              };
        }
  | Timer_scheduled ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* timer_id = string_member json "timer_id" in
      let* run_at_ms = int64_member json "run_at_ms" in
      Ok
        {
          state with
          timers =
            upsert_timer state.timers
              { timer_id; run_at_ms; scheduled_at_ms = event.occurred_at_ms; fired_at_ms = None };
        }
  | Timer_fired -> (
      match event.message with
      | Some timer_id ->
          Ok
            {
              state with
              timers = fire_timer state.timers ~timer_id ~fired_at_ms:event.occurred_at_ms;
            }
      | None -> Error "timer_fired event missing timer id message")
  | Activity_completed | Activity_failed ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* activity_id = string_member json "activity_id" in
      let* name = string_member json "name" in
      let* attempt = int_member json "attempt" in
      let* status =
        let* status = string_member json "status" in
        activity_status_of_string status
      in
      let* result_json = string_option_member json "result_json" in
      let* error = string_option_member json "error" in
      Ok
        {
          state with
          activities =
            {
              activity_id;
              name;
              attempt;
              status;
              result_json;
              error;
              completed_at_ms = event.occurred_at_ms;
            }
            :: List.filter
                 (fun existing ->
                   not (String.equal existing.activity_id activity_id))
                 state.activities;
        }
  | Signal_received ->
      let ( let* ) = Result.bind in
      let* json = payload_json event in
      let* signal_id = string_member json "signal_id" in
      let* name = string_member json "name" in
      let* payload_json = string_option_member json "payload_json" in
      Ok
        {
          state with
          signals =
            {
              signal_id;
              workflow_id = event.workflow_id;
              name;
              payload_json;
              received_at_ms = event.occurred_at_ms;
            }
            :: List.filter
                 (fun existing -> not (String.equal existing.signal_id signal_id))
                 state.signals;
        }
  | Workflow_heartbeat | Workflow_rescheduled | Activity_scheduled
  | Activity_started ->
      Ok state

let replay events =
  let sorted =
    List.sort
      (fun left right -> Int.compare left.sequence right.sequence)
      events
  in
  try
    List.fold_left
      (fun acc event ->
        match acc with
        | Error _ as error -> error
        | Ok state -> replay_event state event)
      (Ok empty_replay_state) sorted
    |> Result.map (fun state ->
           {
             state with
             timers =
               List.sort
                 (fun left right ->
                   let by_due = Int64.compare left.run_at_ms right.run_at_ms in
                   if by_due <> 0 then by_due
                   else String.compare left.timer_id right.timer_id)
                 state.timers;
             activities =
               List.sort
                 (fun left right ->
                   String.compare left.activity_id right.activity_id)
                 state.activities;
             signals =
               List.sort
                 (fun left right -> String.compare left.signal_id right.signal_id)
                 state.signals;
             child_workflows =
               List.sort
                 (fun left right ->
                   String.compare left.child_workflow_id right.child_workflow_id)
                 state.child_workflows;
             updates =
               List.sort
                 (fun left right -> String.compare left.update_id right.update_id)
                 state.updates;
           })
  with Invalid_argument message -> Error message

let empty_stats =
  {
    total = 0;
    queued = 0;
    running = 0;
    succeeded = 0;
    blocked = 0;
    failed = 0;
    cancelled = 0;
  }

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
      | Failed -> { stats with total = stats.total + 1; failed = stats.failed + 1 }
      | Cancelled ->
          { stats with total = stats.total + 1; cancelled = stats.cancelled + 1 })
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
      ("cancelled", `Int stats.cancelled);
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

  val signal :
    t ->
    workflow_id:string ->
    now_ms:int64 ->
    signal_id:string ->
    name:string ->
    ?payload_json:string ->
    unit ->
    (bool, error) result

  val cancel :
    t ->
    workflow_id:string ->
    now_ms:int64 ->
    reason:string ->
    (bool, error) result

  val start_child :
    t ->
    parent_workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    workflow ->
    enqueue_options ->
    (bool, error) result

  val request_update :
    t ->
    workflow_id:string ->
    now_ms:int64 ->
    update_id:string ->
    name:string ->
    ?payload_json:string ->
    unit ->
    (bool, error) result

  val complete_update :
    t ->
    workflow_id:string ->
    worker_id:string ->
    now_ms:int64 ->
    update_id:string ->
    status:update_status ->
    ?result_json:string ->
    ?error:string ->
    unit ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> t -> (item list, error) result
  val children : parent_workflow_id:string -> t -> (item list, error) result
  val history : workflow_id:string -> t -> (event list, error) result
  val timers : workflow_id:string -> t -> (timer list, error) result
  val signals : workflow_id:string -> t -> (signal list, error) result
  val updates : workflow_id:string -> t -> (workflow_update list, error) result
  val query_state : workflow_id:string -> t -> (replay_state option, error) result
  val compact_history : workflow_id:string -> t -> (int option, error) result

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

  val signal :
    backend ->
    workflow_id:string ->
    signal_id:string ->
    name:string ->
    ?payload_json:string ->
    unit ->
    (bool, error) result

  val cancel :
    backend ->
    workflow_id:string ->
    reason:string ->
    (bool, error) result

  val start_child :
    backend ->
    parent_workflow_id:string ->
    worker_id:string ->
    workflow ->
    enqueue_options ->
    (bool, error) result

  val request_update :
    backend ->
    workflow_id:string ->
    update_id:string ->
    name:string ->
    ?payload_json:string ->
    unit ->
    (bool, error) result

  val complete_update :
    backend ->
    workflow_id:string ->
    worker_id:string ->
    update_id:string ->
    status:update_status ->
    ?result_json:string ->
    ?error:string ->
    unit ->
    (bool, error) result

  val snapshot : ?tenant_id:string -> backend -> (item list, error) result
  val children : parent_workflow_id:string -> backend -> (item list, error) result
  val snapshot_json : ?tenant_id:string -> ?group_by_tenant:bool -> backend -> (Yojson.Safe.t, error) result
  val history : workflow_id:string -> backend -> (event list, error) result
  val timers : workflow_id:string -> backend -> (timer list, error) result
  val signals : workflow_id:string -> backend -> (signal list, error) result
  val updates : workflow_id:string -> backend -> (workflow_update list, error) result
  val query_state : workflow_id:string -> backend -> (replay_state option, error) result
  val compact_history : workflow_id:string -> backend -> (int option, error) result

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
  let signal backend ~workflow_id ~signal_id ~name ?payload_json () =
    Backend.signal backend ~workflow_id ~now_ms:(Clock.now_ms ()) ~signal_id
      ~name ?payload_json ()
  let cancel backend ~workflow_id ~reason =
    Backend.cancel backend ~workflow_id ~now_ms:(Clock.now_ms ()) ~reason
  let start_child backend ~parent_workflow_id ~worker_id workflow options =
    Backend.start_child backend ~parent_workflow_id ~worker_id
      ~now_ms:(Clock.now_ms ()) workflow options
  let request_update backend ~workflow_id ~update_id ~name ?payload_json () =
    Backend.request_update backend ~workflow_id ~now_ms:(Clock.now_ms ())
      ~update_id ~name ?payload_json ()
  let complete_update backend ~workflow_id ~worker_id ~update_id ~status
      ?result_json ?error () =
    Backend.complete_update backend ~workflow_id ~worker_id
      ~now_ms:(Clock.now_ms ()) ~update_id ~status ?result_json ?error ()
  let snapshot = Backend.snapshot
  let children = Backend.children
  let history = Backend.history
  let timers = Backend.timers
  let signals = Backend.signals
  let updates = Backend.updates
  let query_state = Backend.query_state
  let compact_history = Backend.compact_history
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
    signals : (string, signal) Hashtbl.t;
    child_workflows : (string, child_workflow) Hashtbl.t;
    updates : (string, workflow_update) Hashtbl.t;
  }

  let create () =
    {
      mutex = Mutex.create ();
      records = Hashtbl.create 128;
      events = Hashtbl.create 128;
      activity_results = Hashtbl.create 128;
      timers = Hashtbl.create 128;
      signals = Hashtbl.create 128;
      child_workflows = Hashtbl.create 128;
      updates = Hashtbl.create 128;
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
      deterministic_replay = true;
      signals = true;
      queries = true;
      history_compaction = true;
      cancellation = true;
      child_workflows = true;
      updates = true;
    }

  let activity_key ~workflow_id ~activity_id = workflow_id ^ "\000" ^ activity_id
  let timer_key ~workflow_id ~timer_id = workflow_id ^ "\000" ^ timer_id
  let signal_key ~workflow_id ~signal_id = workflow_id ^ "\000" ^ signal_id
  let child_key ~parent_workflow_id ~child_workflow_id =
    parent_workflow_id ^ "\000" ^ child_workflow_id
  let update_key ~workflow_id ~update_id = workflow_id ^ "\000" ^ update_id

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
    | Blocked | Succeeded | Failed | Cancelled -> false

  let kind_matches kind (item : item) =
    match kind with
    | None -> true
    | Some kind -> String.equal item.workflow.kind kind

  let timer_payload ~timer_id ~run_at_ms =
    Printf.sprintf {|{"timer_id":%S,"run_at_ms":%Ld}|} timer_id run_at_ms

  let fire_due_timers t record ~now_ms =
    t.timers |> Hashtbl.to_seq_values |> List.of_seq
    |> List.filter (fun (timer : timer) ->
           String.equal timer.workflow_id record.item.workflow.id
           && timer.run_at_ms <= now_ms
           && Option.is_none timer.fired_at_ms)
    |> List.sort (fun (a : timer) (b : timer) ->
           let by_due = Int64.compare a.run_at_ms b.run_at_ms in
           if by_due <> 0 then by_due else String.compare a.timer_id b.timer_id)
    |> List.iter (fun (timer : timer) ->
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

  let active_owned_by ~now_ms (item : item) worker_id =
    match (item.status, item.lease_owner, item.lease_expires_at_ms) with
    | Running, Some owner, Some lease_expires_at_ms ->
        String.equal owner worker_id && lease_expires_at_ms > now_ms
    | _ -> false

  let schedule_timer t ~workflow_id ~worker_id ~now_ms ~timer_id ~run_at_ms
      ?payload_json ~message () =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when active_owned_by ~now_ms record.item worker_id ->
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
        | Some record when active_owned_by ~now_ms record.item worker_id ->
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
    | Succeeded | Blocked | Failed | Cancelled -> true
    | Queued | Running -> false

  let complete t ~workflow_id ~worker_id ~now_ms ~status ~message =
    if not (terminal_status status) then
      Error (`Invalid_transition "complete requires a terminal status")
    else
      with_lock t (fun () ->
          match Hashtbl.find_opt t.records workflow_id with
          | Some record when active_owned_by ~now_ms record.item worker_id ->
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
                ~worker_id ~payload_json:(completion_payload ~status ~message)
                ~message ~occurred_at_ms:now_ms t;
              Ok true
          | _ -> Ok false)

  let reschedule t ~workflow_id ~worker_id ~now_ms ~run_at_ms ~message =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | Some record when active_owned_by ~now_ms record.item worker_id ->
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
        | Some record when active_owned_by ~now_ms record.item worker_id ->
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
                ~worker_id
                ~payload_json:(completion_payload ~status:Failed ~message)
                ~message ~occurred_at_ms:now_ms t;
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

  let children ~parent_workflow_id t =
    with_lock t (fun () ->
        t.child_workflows |> Hashtbl.to_seq_values |> List.of_seq
        |> List.filter (fun (child : child_workflow) ->
               String.equal child.parent_workflow_id parent_workflow_id)
        |> List.filter_map (fun child ->
               Hashtbl.find_opt t.records child.child_workflow_id
               |> Option.map (fun record -> record.item))
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
        |> List.filter (fun (timer : timer) -> String.equal timer.workflow_id workflow_id)
        |> List.sort (fun (a : timer) (b : timer) ->
               let by_due = Int64.compare a.run_at_ms b.run_at_ms in
               if by_due <> 0 then by_due else String.compare a.timer_id b.timer_id)
        |> Result.ok)

  let signals ~workflow_id t =
    with_lock t (fun () ->
        t.signals |> Hashtbl.to_seq_values |> List.of_seq
        |> List.filter (fun (signal : signal) -> String.equal signal.workflow_id workflow_id)
        |> List.sort (fun (a : signal) (b : signal) ->
               Int64.compare a.received_at_ms b.received_at_ms)
        |> Result.ok)

  let updates ~workflow_id t =
    with_lock t (fun () ->
        t.updates |> Hashtbl.to_seq_values |> List.of_seq
        |> List.filter (fun (update : workflow_update) ->
               String.equal update.workflow_id workflow_id)
        |> List.sort (fun (a : workflow_update) (b : workflow_update) ->
               Int64.compare a.requested_at_ms b.requested_at_ms)
        |> Result.ok)

  let query_state ~workflow_id t =
    with_lock t (fun () ->
        let events =
          Hashtbl.find_opt t.events workflow_id
          |> Option.value ~default:[]
          |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
        in
        if events = [] then Ok None
        else
          match replay events with
          | Ok state -> Ok (Some state)
          | Error message -> Error (`Invalid_workflow message))

  let compact_history ~workflow_id t =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | None -> Ok None
        | Some record ->
            let events =
              Hashtbl.find_opt t.events workflow_id
              |> Option.value ~default:[]
              |> List.sort (fun a b -> Int.compare a.sequence b.sequence)
            in
            match replay events with
            | Error message -> Error (`Invalid_workflow message)
            | Ok state ->
                let sequence = record.event_sequence + 1 in
                record.event_sequence <- sequence;
                let event =
                  {
                    id = workflow_id ^ ":" ^ string_of_int sequence;
                    workflow_id;
                    sequence;
                    kind = History_compacted;
                    worker_id = None;
                    payload_json =
                      Some (replay_state_to_yojson state |> Yojson.Safe.to_string);
                    message = Some "history compacted";
                    occurred_at_ms = record.item.updated_at_ms;
                  }
                in
                Hashtbl.replace t.events workflow_id [ event ];
                Ok (Some sequence))

  let signal t ~workflow_id ~now_ms ~signal_id ~name ?payload_json () =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | None -> Ok false
        | Some record when terminal_status record.item.status -> Ok false
        | Some record ->
            let key = signal_key ~workflow_id ~signal_id in
            if Hashtbl.mem t.signals key then Ok true
            else
            let signal =
              { signal_id; workflow_id; name; payload_json; received_at_ms = now_ms }
            in
            Hashtbl.replace t.signals key signal;
            let item = record.item in
            record.item <-
              {
                item with
                status = Queued;
                run_at_ms = min item.run_at_ms now_ms;
                lease_owner = None;
                lease_expires_at_ms = None;
                message = Some ("signal: " ^ name);
                updated_at_ms = now_ms;
              };
            append_event record ~workflow_id ~kind:Signal_received
              ~payload_json:(signal_payload signal) ~message:name
              ~occurred_at_ms:now_ms t;
            Ok true)

  let cancel t ~workflow_id ~now_ms ~reason =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | None -> Ok false
        | Some record when terminal_status record.item.status -> Ok false
        | Some record ->
            let item = record.item in
            record.item <-
              {
                item with
                status = Cancelled;
                lease_owner = None;
                lease_expires_at_ms = None;
                finished_at_ms = Some now_ms;
                message = Some reason;
                updated_at_ms = now_ms;
              };
            append_event record ~workflow_id ~kind:Workflow_cancelled
              ~message:reason ~occurred_at_ms:now_ms t;
            Ok true)

  let start_child t ~parent_workflow_id ~worker_id ~now_ms workflow
      (options : enqueue_options) =
    match validate_workflow workflow with
    | Error message -> Error (`Invalid_workflow message)
    | Ok () ->
        with_lock t (fun () ->
            match Hashtbl.find_opt t.records parent_workflow_id with
            | None -> Error (`Invalid_workflow ("unknown parent workflow: " ^ parent_workflow_id))
            | Some parent when terminal_status parent.item.status -> Ok false
            | Some parent
              when not (active_owned_by ~now_ms parent.item worker_id) ->
                Ok false
            | Some parent ->
                let key =
                  child_key ~parent_workflow_id
                    ~child_workflow_id:workflow.id
                in
                if Hashtbl.mem t.child_workflows key then Ok true
                else if Hashtbl.mem t.records workflow.id then
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
                  let child_record = { item; event_sequence = 0 } in
                  Hashtbl.add t.records workflow.id child_record;
                  append_event child_record ~workflow_id:workflow.id
                    ~kind:Workflow_enqueued ?payload_json:options.payload_json
                    ~occurred_at_ms:now_ms t;
                  let child =
                    {
                      parent_workflow_id;
                      child_workflow_id = workflow.id;
                      child_kind = workflow.kind;
                      started_at_ms = now_ms;
                    }
                  in
                  Hashtbl.replace t.child_workflows key child;
                  append_event parent ~workflow_id:parent_workflow_id
                    ~kind:Child_workflow_started
                    ~payload_json:(child_workflow_payload child)
                    ~message:workflow.id ~occurred_at_ms:now_ms t;
                  Ok true)

  let request_update t ~workflow_id ~now_ms ~update_id ~name ?payload_json () =
    with_lock t (fun () ->
        match Hashtbl.find_opt t.records workflow_id with
        | None -> Ok false
        | Some record when terminal_status record.item.status -> Ok false
        | Some record ->
            let key = update_key ~workflow_id ~update_id in
            if Hashtbl.mem t.updates key then Ok true
            else
              let update =
                {
                  update_id;
                  workflow_id;
                  name;
                  payload_json;
                  status = Update_pending;
                  result_json = None;
                  error = None;
                  requested_at_ms = now_ms;
                  completed_at_ms = None;
                }
              in
              Hashtbl.replace t.updates key update;
              let item = record.item in
              record.item <-
                {
                  item with
                  status = Queued;
                  run_at_ms = min item.run_at_ms now_ms;
                  lease_owner = None;
                  lease_expires_at_ms = None;
                  message = Some ("update: " ^ name);
                  updated_at_ms = now_ms;
                };
              append_event record ~workflow_id ~kind:Update_requested
                ~payload_json:(update_payload update) ~message:name
                ~occurred_at_ms:now_ms t;
              Ok true)

  let complete_update t ~workflow_id ~worker_id ~now_ms ~update_id ~status
      ?result_json ?error () =
    match status with
    | Update_pending ->
        Error (`Invalid_transition "complete_update requires a terminal update status")
    | Update_completed_status | Update_rejected | Update_failed ->
        with_lock t (fun () ->
            match Hashtbl.find_opt t.records workflow_id with
            | Some record when active_owned_by ~now_ms record.item worker_id -> (
                let key = update_key ~workflow_id ~update_id in
                match Hashtbl.find_opt t.updates key with
                | None -> Ok false
                | Some existing when existing.status <> Update_pending -> Ok true
                | Some existing ->
                    let completed =
                      {
                        existing with
                        status;
                        result_json;
                        error;
                        completed_at_ms = Some now_ms;
                      }
                    in
                    Hashtbl.replace t.updates key completed;
                    append_event record ~workflow_id ~kind:Update_completed
                      ~worker_id ~payload_json:(update_payload completed)
                      ?message:error ~occurred_at_ms:now_ms t;
                    Ok true)
            | _ -> Ok false)

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
              ~payload_json:(activity_payload result) ?message:result.error
              ~occurred_at_ms:now_ms t;
            Ok ())

  let find_activity_result t ~workflow_id ~activity_id =
    with_lock t (fun () ->
        Hashtbl.find_opt t.activity_results
          (activity_key ~workflow_id ~activity_id)
        |> Result.ok)
end
