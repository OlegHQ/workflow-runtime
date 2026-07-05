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

let config ?kind ?(lease_ms = 30_000L) ?(poll_interval_ms = 1_000L)
    ?(heartbeat_interval_ms = 10_000L) ~worker_id () =
  { worker_id; lease_ms; poll_interval_ms; heartbeat_interval_ms; kind }

let seconds ms = Int64.to_float ms /. 1_000.0

module Make (Runtime : RUNTIME) = struct
  type backend = Runtime.backend
  type error = Runtime.error
  type handler = Workflow_runtime.claim -> decision

  let map_bool workflow_id value result =
    match result with
    | Ok true -> Ok value
    | Ok false -> Ok (Lease_lost { workflow_id })
    | Error error -> Error error

  let protected_handler handler claim =
    try Ok (handler claim) with
    | exn -> Error (Printexc.to_string exn)

  let rec heartbeat_loop ~clock backend config (claim : Workflow_runtime.claim)
      =
    Eio.Time.sleep clock (seconds config.heartbeat_interval_ms);
    let workflow_id = claim.item.workflow.id in
    let _ =
      Runtime.heartbeat backend ~workflow_id ~worker_id:claim.worker_id
        ~lease_ms:config.lease_ms
    in
    heartbeat_loop ~clock backend config claim

  let with_heartbeat ~clock backend config claim f =
    if config.heartbeat_interval_ms <= 0L then f ()
    else
      Eio.Fiber.first f (fun () ->
          heartbeat_loop ~clock backend config claim)

  let apply_decision backend config (claim : Workflow_runtime.claim) decision =
    let workflow_id = claim.item.workflow.id in
    let worker_id = claim.worker_id in
    match decision with
    | Complete { status; message } ->
        Runtime.complete backend ~workflow_id ~worker_id ~status ~message
        |> map_bool workflow_id (Completed { workflow_id; status })
    | Reschedule { run_at_ms; message } ->
        Runtime.reschedule backend ~workflow_id ~worker_id ~run_at_ms ~message
        |> map_bool workflow_id (Rescheduled { workflow_id; run_at_ms })
    | Retry { policy; message } -> (
        match Runtime.retry backend ~workflow_id ~worker_id ~policy ~message with
        | Ok (Some (Workflow_runtime.Retried { attempt; run_at_ms })) ->
            Ok (Retry_scheduled { workflow_id; attempt; run_at_ms })
        | Ok (Some (Workflow_runtime.Retries_exhausted { attempt })) ->
            Ok (Retry_exhausted { workflow_id; attempt })
        | Ok None -> Ok (Lease_lost { workflow_id })
        | Error error -> Error error)
    | Keep_running { message = _ } ->
        Runtime.heartbeat backend ~workflow_id ~worker_id
          ~lease_ms:config.lease_ms
        |> map_bool workflow_id (Kept_running { workflow_id })

  let record_handler_failure backend config (claim : Workflow_runtime.claim)
      error =
    let workflow_id = claim.item.workflow.id in
    Runtime.complete backend ~workflow_id ~worker_id:claim.worker_id
      ~status:Workflow_runtime.Failed ~message:error
    |> map_bool workflow_id (Handler_failed { workflow_id; error })

  let run_claim ~clock backend config handler claim =
    with_heartbeat ~clock backend config claim (fun () ->
        match protected_handler handler claim with
        | Ok decision -> apply_decision backend config claim decision
        | Error error -> record_handler_failure backend config claim error)

  let run_once ~clock backend config handler =
    let ( let* ) = Result.bind in
    let* claim =
      Runtime.claim_next ?kind:config.kind backend ~worker_id:config.worker_id
        ~lease_ms:config.lease_ms
    in
    match claim with
    | None -> Ok No_work
    | Some claim -> run_claim ~clock backend config handler claim

  let run_forever ~clock ?on_result ?on_error backend config handler =
    let rec loop () =
      (match run_once ~clock backend config handler with
      | Ok No_work -> Eio.Time.sleep clock (seconds config.poll_interval_ms)
      | Ok result -> Option.iter (fun on_result -> on_result result) on_result
      | Error error ->
          Option.iter (fun on_error -> on_error error) on_error;
          Eio.Time.sleep clock (seconds config.poll_interval_ms));
      loop ()
    in
    loop ()
end
