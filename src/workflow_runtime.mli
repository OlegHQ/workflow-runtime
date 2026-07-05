(** Durable workflow runtime API.

    The runtime owns workflow state transitions, worker leases, retry/recovery
    visibility, and snapshots. Storage is supplied by a backend module. A
    production backend must implement atomic [claim_next] semantics so multiple
    worker processes cannot claim the same workflow concurrently. *)

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
  compacted_at_sequence : int option;
}

val enqueue_options :
  ?run_at_ms:int64 -> ?payload_json:string -> unit -> enqueue_options

val retry_policy :
  ?max_attempts:int ->
  ?initial_backoff_ms:int64 ->
  ?max_backoff_ms:int64 ->
  ?backoff_multiplier:float ->
  unit ->
  retry_policy

val status_to_string : status -> string
val status_of_string : string -> (status, string) result
val event_kind_to_string : event_kind -> string
val event_kind_of_string : string -> (event_kind, string) result
val activity_status_to_string : activity_status -> string
val activity_status_of_string : string -> (activity_status, string) result
val item_to_yojson : item -> Yojson.Safe.t
val event_to_yojson : event -> Yojson.Safe.t
val activity_result_to_yojson : activity_result -> Yojson.Safe.t
val timer_to_yojson : timer -> Yojson.Safe.t
val signal_to_yojson : signal -> Yojson.Safe.t
val replay_state_to_yojson : replay_state -> Yojson.Safe.t
val items_to_yojson : ?group_by_tenant:bool -> item list -> Yojson.Safe.t
val stats : item list -> stats
val retry_delay_ms : retry_policy -> attempt:int -> int64
val replay : event list -> (replay_state, string) result

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

  val snapshot : ?tenant_id:string -> t -> (item list, error) result
  val history : workflow_id:string -> t -> (event list, error) result
  val timers : workflow_id:string -> t -> (timer list, error) result
  val signals : workflow_id:string -> t -> (signal list, error) result
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

  val snapshot : ?tenant_id:string -> backend -> (item list, error) result
  val snapshot_json : ?tenant_id:string -> ?group_by_tenant:bool -> backend -> (Yojson.Safe.t, error) result
  val history : workflow_id:string -> backend -> (event list, error) result
  val timers : workflow_id:string -> backend -> (timer list, error) result
  val signals : workflow_id:string -> backend -> (signal list, error) result
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

module Make (Clock : CLOCK) (Backend : BACKEND) :
  S with type backend = Backend.t and type error = Backend.error

module Memory_backend : sig
  type error =
    [ `Duplicate_workflow of string
    | `Invalid_workflow of string
    | `Invalid_transition of string ]

  type t

  val create : unit -> t

  include BACKEND with type t := t and type error := error
end
