(** In-process workflow observation runtime.

    This library tracks live or recently completed workflow lifecycle state in
    memory. It is intended to sit beside a durable source of truth such as a
    database row, queue message, or append-only log. It does not provide durable
    replay, distributed leases, or exactly-once execution by itself. *)

type status = Queued | Running | Succeeded | Blocked | Failed
(** A workflow's observable lifecycle status. [Blocked] means the workflow
    stopped in an operator- or user-actionable state rather than succeeding or
    failing permanently. *)

type workflow = {
  id : string;
  tenant_id : string;
  kind : string;
  subject_id : string option;
  name : string option;
  metadata : (string * string) list;
}
(** Stable workflow identity and classification. [id] should be a durable id
    owned by the caller, not a transient process-local id. *)

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

type t

val create : ?max_items:int -> clock:(unit -> int64) -> unit -> t
(** [create ~clock ()] creates a thread-safe runtime. [max_items] bounds the
    number of retained items; when exceeded, the oldest terminal items are
    evicted first. Active [Queued] and [Running] workflows are never evicted by
    the retention pass. *)

val record_queued : t -> workflow -> unit
val record_running : t -> workflow -> unit
val record_succeeded : t -> workflow -> string -> unit
val record_blocked : t -> workflow -> string -> unit
val record_failed : t -> workflow -> string -> unit

val snapshot : ?tenant_id:string -> t -> item list
(** Returns newest-first items. *)

val grouped_by_tenant : ?tenant_id:string -> t -> (string * item list) list
(** Returns tenant groups sorted by tenant id; each group's items are
    newest-first. *)

val stats : ?tenant_id:string -> t -> stats
val status_to_string : status -> string
val item_to_yojson : item -> Yojson.Safe.t
val snapshot_json : ?tenant_id:string -> ?group_by_tenant:bool -> t -> Yojson.Safe.t
