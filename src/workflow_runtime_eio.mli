(** Eio worker runner for durable workflow runtimes. *)

module type RUNTIME = Workflow_runtime.S

type config = {
  worker_id : string;
  lease_ms : int64;
  poll_interval_ms : int64;
  heartbeat_interval_ms : int64;
  kind : string option;
}

type decision =
  | Complete of { status : Workflow_runtime.status; message : string }
  | Reschedule of { run_at_ms : int64; message : string }
  | Retry of { policy : Workflow_runtime.retry_policy; message : string }
  | Keep_running of { message : string }

type run_result =
  | No_work
  | Completed of { workflow_id : string; status : Workflow_runtime.status }
  | Rescheduled of { workflow_id : string; run_at_ms : int64 }
  | Retry_scheduled of {
      workflow_id : string;
      attempt : int;
      run_at_ms : int64;
    }
  | Retry_exhausted of { workflow_id : string; attempt : int }
  | Kept_running of { workflow_id : string }
  | Handler_failed of { workflow_id : string; error : string }
  | Lease_lost of { workflow_id : string }

val config :
  ?kind:string ->
  ?lease_ms:int64 ->
  ?poll_interval_ms:int64 ->
  ?heartbeat_interval_ms:int64 ->
  worker_id:string ->
  unit ->
  config

module Make (Runtime : RUNTIME) : sig
  type backend = Runtime.backend
  type error = Runtime.error
  type handler = Workflow_runtime.claim -> decision

  val run_once :
    clock:_ Eio.Time.clock ->
    backend ->
    config ->
    handler ->
    (run_result, error) result

  val run_forever :
    clock:_ Eio.Time.clock ->
    ?on_result:(run_result -> unit) ->
    ?on_error:(error -> unit) ->
    backend ->
    config ->
    handler ->
    unit
end
