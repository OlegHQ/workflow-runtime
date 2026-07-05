type error =
  [ `Bad_document of string
  | `Duplicate_workflow of string
  | `Mongo of string ]

type t = {
  client : Mongo_eio.direct_client;
  db : string;
  workflows_collection : string;
  events_collection : string;
  activity_results_collection : string;
  timers_collection : string;
  signals_collection : string;
  child_workflows_collection : string;
  updates_collection : string;
}

let create ~client ~db ~collection () =
  {
    client;
    db;
    workflows_collection = collection;
    events_collection = collection ^ "_events";
    activity_results_collection = collection ^ "_activity_results";
    timers_collection = collection ^ "_timers";
    signals_collection = collection ^ "_signals";
    child_workflows_collection = collection ^ "_child_workflows";
    updates_collection = collection ^ "_updates";
  }

let error_to_string = function
  | `Bad_document message -> "bad document: " ^ message
  | `Duplicate_workflow id -> "workflow already exists: " ^ id
  | `Mongo message -> "mongo: " ^ message

let capabilities _ =
  Workflow_runtime.
    {
      durable = true;
      multi_worker_claims = true;
      event_history = true;
      activity_results = true;
      task_queue_filtering = true;
      retry_backoff = true;
      durable_timers = true;
      deterministic_replay = true;
      signals = true;
      queries = true;
      history_compaction = true;
      cancellation = true;
      child_workflows = true;
      updates = true;
    }

let mongo_error error = `Mongo (Mongo_error.to_string error)

type metadata_doc = { key : string; value : string } [@@deriving bson]

type workflow_doc = {
  id : string; [@bson.key "_id"]
  tenant_id : string;
  kind : string;
  subject_id : string option;
  name : string option;
  metadata : metadata_doc list;
  status : string;
  run_at_ms : int64;
  attempt : int;
  lease_owner : string option;
  lease_expires_at_ms : int64 option;
  payload_json : string option;
  started_at_ms : int64 option;
  finished_at_ms : int64 option;
  message : string option;
  event_sequence : int option;
  created_at_ms : int64;
  updated_at_ms : int64;
}
[@@deriving bson]

type event_doc = {
  id : string; [@bson.key "_id"]
  workflow_id : string;
  sequence : int;
  kind : string;
  worker_id : string option;
  payload_json : string option;
  message : string option;
  occurred_at_ms : int64;
}
[@@deriving bson]

type activity_result_doc = {
  id : string; [@bson.key "_id"]
  activity_id : string;
  workflow_id : string;
  name : string;
  attempt : int;
  status : string;
  result_json : string option;
  error : string option;
  updated_at_ms : int64;
}
[@@deriving bson]

type timer_doc = {
  id : string; [@bson.key "_id"]
  timer_id : string;
  workflow_id : string;
  run_at_ms : int64;
  payload_json : string option;
  fired_at_ms : int64 option;
  created_at_ms : int64;
  updated_at_ms : int64;
}
[@@deriving bson]

type signal_doc = {
  id : string; [@bson.key "_id"]
  signal_id : string;
  workflow_id : string;
  name : string;
  payload_json : string option;
  received_at_ms : int64;
}
[@@deriving bson]

type child_workflow_doc = {
  id : string; [@bson.key "_id"]
  parent_workflow_id : string;
  child_workflow_id : string;
  child_kind : string;
  started_at_ms : int64;
}
[@@deriving bson]

type workflow_update_doc = {
  id : string; [@bson.key "_id"]
  update_id : string;
  workflow_id : string;
  name : string;
  payload_json : string option;
  status : string;
  result_json : string option;
  error : string option;
  requested_at_ms : int64;
  completed_at_ms : int64 option;
}
[@@deriving bson]

let doc fields =
  List.fold_right (fun (name, element) acc -> Bson.add_element name element acc)
    fields Bson.empty

let string name value = (name, Bson.create_string value)
let int32 name value = (name, Bson.create_int32 (Int32.of_int value))
let int64 name value = (name, Bson.create_int64 value)
let doc_element name value = (name, Bson.create_doc_element value)

let metadata_doc_of_pair (key, value) = { key; value }
let metadata_pair_of_doc { key; value } = (key, value)

let workflow_doc_of_item (item : Workflow_runtime.item) =
  let workflow = item.workflow in
  {
    id = workflow.id;
    tenant_id = workflow.tenant_id;
    kind = workflow.kind;
    subject_id = workflow.subject_id;
    name = workflow.name;
    metadata = List.map metadata_doc_of_pair workflow.metadata;
    status = Workflow_runtime.status_to_string item.status;
    run_at_ms = item.run_at_ms;
    attempt = item.attempt;
    lease_owner = item.lease_owner;
    lease_expires_at_ms = item.lease_expires_at_ms;
    payload_json = item.payload_json;
    started_at_ms = item.started_at_ms;
    finished_at_ms = item.finished_at_ms;
    message = item.message;
    event_sequence = None;
    created_at_ms = item.created_at_ms;
    updated_at_ms = item.updated_at_ms;
  }

let item_of_workflow_doc (doc : workflow_doc) =
  let ( let* ) = Result.bind in
  let* status =
    Workflow_runtime.status_of_string doc.status
    |> Result.map_error (fun message -> `Bad_document message)
  in
  Ok
    Workflow_runtime.
      {
        workflow =
          {
            id = doc.id;
            tenant_id = doc.tenant_id;
            kind = doc.kind;
            subject_id = doc.subject_id;
            name = doc.name;
            metadata = List.map metadata_pair_of_doc doc.metadata;
          };
        status;
        run_at_ms = doc.run_at_ms;
        attempt = doc.attempt;
        lease_owner = doc.lease_owner;
        lease_expires_at_ms = doc.lease_expires_at_ms;
        payload_json = doc.payload_json;
        started_at_ms = doc.started_at_ms;
        finished_at_ms = doc.finished_at_ms;
        message = doc.message;
        created_at_ms = doc.created_at_ms;
        updated_at_ms = doc.updated_at_ms;
      }

let event_doc_of_event (event : Workflow_runtime.event) =
  ({
     id = event.id;
     workflow_id = event.workflow_id;
     sequence = event.sequence;
     kind = Workflow_runtime.event_kind_to_string event.kind;
     worker_id = event.worker_id;
     payload_json = event.payload_json;
     message = event.message;
     occurred_at_ms = event.occurred_at_ms;
   }
    : event_doc)

let event_of_event_doc (doc : event_doc) =
  let ( let* ) = Result.bind in
  let* kind =
    Workflow_runtime.event_kind_of_string doc.kind
    |> Result.map_error (fun message -> `Bad_document message)
  in
  Ok
    Workflow_runtime.
      {
        id = doc.id;
        workflow_id = doc.workflow_id;
        sequence = doc.sequence;
        kind;
        worker_id = doc.worker_id;
        payload_json = doc.payload_json;
        message = doc.message;
        occurred_at_ms = doc.occurred_at_ms;
      }

let activity_result_key ~workflow_id ~activity_id =
  workflow_id ^ ":" ^ activity_id

let activity_result_doc_of_result (result : Workflow_runtime.activity_result) =
  ({
     id =
       activity_result_key ~workflow_id:result.workflow_id
         ~activity_id:result.activity_id;
     activity_id = result.activity_id;
     workflow_id = result.workflow_id;
     name = result.name;
     attempt = result.attempt;
     status = Workflow_runtime.activity_status_to_string result.status;
     result_json = result.result_json;
     error = result.error;
     updated_at_ms = result.updated_at_ms;
   }
    : activity_result_doc)

let activity_result_of_doc (doc : activity_result_doc) =
  let ( let* ) = Result.bind in
  let* status =
    Workflow_runtime.activity_status_of_string doc.status
    |> Result.map_error (fun message -> `Bad_document message)
  in
  Ok
    Workflow_runtime.
      {
        activity_id = doc.activity_id;
        workflow_id = doc.workflow_id;
        name = doc.name;
        attempt = doc.attempt;
        status;
        result_json = doc.result_json;
        error = doc.error;
        updated_at_ms = doc.updated_at_ms;
      }

let timer_key ~workflow_id ~timer_id = workflow_id ^ ":" ^ timer_id

let timer_doc_of_timer (timer : Workflow_runtime.timer) =
  ({
     id = timer_key ~workflow_id:timer.workflow_id ~timer_id:timer.timer_id;
     timer_id = timer.timer_id;
     workflow_id = timer.workflow_id;
     run_at_ms = timer.run_at_ms;
     payload_json = timer.payload_json;
     fired_at_ms = timer.fired_at_ms;
     created_at_ms = timer.created_at_ms;
     updated_at_ms = timer.updated_at_ms;
   }
    : timer_doc)

let timer_of_doc (doc : timer_doc) =
  Workflow_runtime.
    {
      timer_id = doc.timer_id;
      workflow_id = doc.workflow_id;
      run_at_ms = doc.run_at_ms;
      payload_json = doc.payload_json;
      fired_at_ms = doc.fired_at_ms;
      created_at_ms = doc.created_at_ms;
      updated_at_ms = doc.updated_at_ms;
    }

let signal_key ~workflow_id ~signal_id = workflow_id ^ ":" ^ signal_id

let signal_doc_of_signal (signal : Workflow_runtime.signal) =
  ({
     id = signal_key ~workflow_id:signal.workflow_id ~signal_id:signal.signal_id;
     signal_id = signal.signal_id;
     workflow_id = signal.workflow_id;
     name = signal.name;
     payload_json = signal.payload_json;
     received_at_ms = signal.received_at_ms;
   }
    : signal_doc)

let signal_of_doc (doc : signal_doc) =
  Workflow_runtime.
    {
      signal_id = doc.signal_id;
      workflow_id = doc.workflow_id;
      name = doc.name;
      payload_json = doc.payload_json;
      received_at_ms = doc.received_at_ms;
    }

let child_workflow_key ~parent_workflow_id ~child_workflow_id =
  parent_workflow_id ^ ":" ^ child_workflow_id

let child_workflow_doc_of_child (child : Workflow_runtime.child_workflow) =
  ({
     id =
       child_workflow_key ~parent_workflow_id:child.parent_workflow_id
         ~child_workflow_id:child.child_workflow_id;
     parent_workflow_id = child.parent_workflow_id;
     child_workflow_id = child.child_workflow_id;
     child_kind = child.child_kind;
     started_at_ms = child.started_at_ms;
   }
    : child_workflow_doc)

let child_of_doc (doc : child_workflow_doc) =
  Workflow_runtime.
    {
      parent_workflow_id = doc.parent_workflow_id;
      child_workflow_id = doc.child_workflow_id;
      child_kind = doc.child_kind;
      started_at_ms = doc.started_at_ms;
    }

let update_key ~workflow_id ~update_id = workflow_id ^ ":" ^ update_id

let update_doc_of_update (update : Workflow_runtime.workflow_update) =
  ({
     id = update_key ~workflow_id:update.workflow_id ~update_id:update.update_id;
     update_id = update.update_id;
     workflow_id = update.workflow_id;
     name = update.name;
     payload_json = update.payload_json;
     status = Workflow_runtime.update_status_to_string update.status;
     result_json = update.result_json;
     error = update.error;
     requested_at_ms = update.requested_at_ms;
     completed_at_ms = update.completed_at_ms;
   }
    : workflow_update_doc)

let update_of_doc (doc : workflow_update_doc) =
  let ( let* ) = Result.bind in
  let* status =
    Workflow_runtime.update_status_of_string doc.status
    |> Result.map_error (fun message -> `Bad_document message)
  in
  Ok
    Workflow_runtime.
      {
        update_id = doc.update_id;
        workflow_id = doc.workflow_id;
        name = doc.name;
        payload_json = doc.payload_json;
        status;
        result_json = doc.result_json;
        error = doc.error;
        requested_at_ms = doc.requested_at_ms;
        completed_at_ms = doc.completed_at_ms;
      }

let decode_workflow_doc bson =
  match workflow_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> Ok doc

let decode_item bson =
  let ( let* ) = Result.bind in
  let* doc = decode_workflow_doc bson in
  item_of_workflow_doc doc

let decode_event bson =
  match event_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> event_of_event_doc doc

let decode_activity_result bson =
  match activity_result_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> activity_result_of_doc doc

let decode_timer bson =
  match timer_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> Ok (timer_of_doc doc)

let decode_signal bson =
  match signal_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> Ok (signal_of_doc doc)

let decode_child bson =
  match child_workflow_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> Ok (child_of_doc doc)

let decode_update bson =
  match workflow_update_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> update_of_doc doc

let index_key fields =
  Bson.add_element "key" (Bson.create_doc_element (doc fields)) Bson.empty

let ensure t =
  let ( let* ) = Result.bind in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.workflows_collection
      (index_key [ int32 "status" 1; int32 "run_at_ms" 1 ])
      [ Mongo_index.Name "workflow_due_idx" ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.workflows_collection
      (index_key [ int32 "status" 1; int32 "kind" 1; int32 "run_at_ms" 1 ])
      [ Mongo_index.Name "workflow_kind_due_idx" ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.workflows_collection
      (index_key [ int32 "tenant_id" 1; int32 "updated_at_ms" (-1) ])
      [ Mongo_index.Name "workflow_tenant_updated_idx" ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.events_collection
      (index_key [ int32 "workflow_id" 1; int32 "sequence" 1 ])
      [ Mongo_index.Name "workflow_event_sequence_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.activity_results_collection
      (index_key [ int32 "workflow_id" 1; int32 "activity_id" 1 ])
      [ Mongo_index.Name "workflow_activity_result_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.timers_collection
      (index_key [ int32 "workflow_id" 1; int32 "timer_id" 1 ])
      [ Mongo_index.Name "workflow_timer_id_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.timers_collection
      (index_key [ int32 "workflow_id" 1; int32 "run_at_ms" 1 ])
      [ Mongo_index.Name "workflow_timer_due_idx" ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.signals_collection
      (index_key [ int32 "workflow_id" 1; int32 "signal_id" 1 ])
      [ Mongo_index.Name "workflow_signal_id_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.child_workflows_collection
      (index_key [ int32 "parent_workflow_id" 1; int32 "child_workflow_id" 1 ])
      [ Mongo_index.Name "workflow_child_id_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db
      ~collection:t.updates_collection
      (index_key [ int32 "workflow_id" 1; int32 "update_id" 1 ])
      [ Mongo_index.Name "workflow_update_id_idx"; Mongo_index.Unique true ]
    |> Result.map_error mongo_error
  in
  Ok ()

let event_id ~workflow_id ~sequence = workflow_id ^ ":" ^ string_of_int sequence

let append_event t ~workflow_id ~sequence ~kind ?worker_id ?payload_json ?message
    ~occurred_at_ms () =
  let event =
    Workflow_runtime.
      {
        id = event_id ~workflow_id ~sequence;
        workflow_id;
        sequence;
        kind;
        worker_id;
        payload_json;
        message;
        occurred_at_ms;
      }
  in
  Mongo_eio.direct_insert_one t.client ~db:t.db
    ~collection:t.events_collection
    (event_doc_to_bson_doc (event_doc_of_event event))
  |> Result.map (fun _ -> ())
  |> Result.map_error mongo_error

let timer_payload ~timer_id ~run_at_ms =
  Printf.sprintf {|{"timer_id":%S,"run_at_ms":%Ld}|} timer_id run_at_ms

let string_option_json = function
  | Some value -> `String value
  | None -> `Null

let completion_payload ~status ~message =
  `Assoc
    [
      ("status", `String (Workflow_runtime.status_to_string status));
      ("message", string_option_json (Some message));
    ]
  |> Yojson.Safe.to_string

let activity_payload (result : Workflow_runtime.activity_result) =
  `Assoc
    [
      ("activity_id", `String result.activity_id);
      ("name", `String result.name);
      ("attempt", `Int result.attempt);
      ("status", `String (Workflow_runtime.activity_status_to_string result.status));
      ("result_json", string_option_json result.result_json);
      ("error", string_option_json result.error);
    ]
  |> Yojson.Safe.to_string

let signal_payload (signal : Workflow_runtime.signal) =
  `Assoc
    [
      ("signal_id", `String signal.signal_id);
      ("name", `String signal.name);
      ("payload_json", string_option_json signal.payload_json);
    ]
  |> Yojson.Safe.to_string

let child_workflow_payload (child : Workflow_runtime.child_workflow) =
  Workflow_runtime.child_workflow_to_yojson child |> Yojson.Safe.to_string

let update_payload (update : Workflow_runtime.workflow_update) =
  Workflow_runtime.workflow_update_to_yojson update |> Yojson.Safe.to_string

let non_terminal_status_filter =
  doc_element "status"
    (doc
       [
         ( "$in",
           Bson.create_list
             [
               Bson.create_string (Workflow_runtime.status_to_string Queued);
               Bson.create_string (Workflow_runtime.status_to_string Running);
             ] );
       ])

let cancel_payload ~reason =
  `Assoc
    [
      ("status", `String (Workflow_runtime.status_to_string Cancelled));
      ("message", string_option_json (Some reason));
    ]
  |> Yojson.Safe.to_string

let enqueue t ~now_ms workflow (options : Workflow_runtime.enqueue_options) =
  let item =
    Workflow_runtime.
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
  let workflow_doc = { (workflow_doc_of_item item) with event_sequence = Some 1 } in
  let ( let* ) = Result.bind in
  let* () =
    Mongo_eio.direct_insert_one t.client ~db:t.db
      ~collection:t.workflows_collection
      (workflow_doc_to_bson_doc workflow_doc)
    |> Result.map (fun _ -> ())
    |> Result.map_error (fun error ->
           if Mongo_error.is_duplicate_key error then
             `Duplicate_workflow workflow.Workflow_runtime.id
           else mongo_error error)
  in
  append_event t ~workflow_id:workflow.Workflow_runtime.id ~sequence:1
    ~kind:Workflow_runtime.Workflow_enqueued ?payload_json:options.payload_json
    ~occurred_at_ms:now_ms ()

let find_and_modify t ~query ~update =
  Mongo_eio.direct_run_command t.client t.db
    [
      ("findAndModify", Bson.create_string t.workflows_collection);
      ("query", Bson.create_doc_element query);
      ("sort", Bson.create_doc_element (doc [ int32 "run_at_ms" 1 ]));
      ("update", Bson.create_doc_element update);
      ("new", Bson.create_boolean true);
    ]
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok response -> (
      match
        Bson.get_element "value" response.Mongo_command.body |> Bson.get_doc_element
      with
      | exception Bson.Wrong_bson_type -> Ok None
      | doc ->
          let ( let* ) = Result.bind in
          let* workflow_doc = decode_workflow_doc doc in
          let* item = item_of_workflow_doc workflow_doc in
          Ok (Some (item, Option.value workflow_doc.event_sequence ~default:0)))

let claim_query ?workflow_id ?kind ~now_ms () =
  let id_filter =
    match workflow_id with None -> [] | Some id -> [ string "_id" id ]
  in
  let kind_filter =
    match kind with None -> [] | Some kind -> [ string "kind" kind ]
  in
  doc
    (id_filter
    @ kind_filter
    @ [
        ( "$or",
          Bson.create_list
            [
              Bson.create_doc_element
                (doc
                   [
                     string "status" (Workflow_runtime.status_to_string Queued);
                     doc_element "run_at_ms" (doc [ ("$lte", Bson.create_int64 now_ms) ]);
                   ]);
              Bson.create_doc_element
                (doc
                   [
                     string "status" (Workflow_runtime.status_to_string Running);
                     doc_element "lease_expires_at_ms"
                       (doc [ ("$lte", Bson.create_int64 now_ms) ]);
                   ]);
            ] );
      ])

let increment_event_sequence t ~workflow_id =
  let query = doc [ string "_id" workflow_id ] in
  let update = doc [ doc_element "$inc" (doc [ int32 "event_sequence" 1 ]) ] in
  find_and_modify t ~query ~update
  |> Result.map (function
       | None -> None
       | Some (_item, sequence) -> Some sequence)

let due_timers t ~workflow_id ~now_ms =
  let filter =
    doc
      [
        string "workflow_id" workflow_id;
        doc_element "run_at_ms" (doc [ ("$lte", Bson.create_int64 now_ms) ]);
        ("fired_at_ms", Bson.create_null ());
      ]
  in
  let opts =
    {
      (Mongo_crud.default_find t.timers_collection filter) with
      sort = Some (doc [ int32 "run_at_ms" 1; int32 "timer_id" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.timers_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok timers -> (
              match decode_timer bson with
              | Ok timer -> Ok (timer :: timers)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let mark_timer_fired t ~now_ms (timer : Workflow_runtime.timer) =
  let selector =
    doc
      [
        string "_id"
          (timer_key ~workflow_id:timer.workflow_id ~timer_id:timer.timer_id);
        ("fired_at_ms", Bson.create_null ());
      ]
  in
  let update =
    doc
      [
        doc_element "$set"
          (doc [ int64 "fired_at_ms" now_ms; int64 "updated_at_ms" now_ms ]);
      ]
  in
  Mongo_eio.direct_update_one t.client ~db:t.db ~collection:t.timers_collection
    ~upsert:false selector update
  |> Result.map_error mongo_error
  |> Result.map (fun result -> result.Mongo_crud.matched_count = 1)

let fire_due_timers t ~workflow_id ~now_ms ~first_sequence =
  let ( let* ) = Result.bind in
  let* timers = due_timers t ~workflow_id ~now_ms in
  let rec loop next_sequence fired_any = function
    | [] ->
        if fired_any then
          match increment_event_sequence t ~workflow_id with
          | Error _ as error -> error
          | Ok (Some sequence) -> Ok sequence
          | Ok None -> Error (`Bad_document ("unknown workflow: " ^ workflow_id))
        else Ok next_sequence
    | timer :: rest ->
        let* marked = mark_timer_fired t ~now_ms timer in
        if not marked then loop next_sequence fired_any rest
        else
          let* () =
            append_event t ~workflow_id ~sequence:next_sequence
              ~kind:Workflow_runtime.Timer_fired ?payload_json:timer.payload_json
              ~message:timer.timer_id ~occurred_at_ms:now_ms ()
          in
          let* next_sequence =
            match rest with
            | [] -> Ok next_sequence
            | _ -> (
                match increment_event_sequence t ~workflow_id with
                | Error _ as error -> error
                | Ok (Some sequence) -> Ok sequence
                | Ok None ->
                    Error (`Bad_document ("unknown workflow: " ^ workflow_id)))
          in
          loop next_sequence true rest
  in
  loop first_sequence false timers

let claim_with_query t ~query ~worker_id ~now_ms ~lease_ms =
  let lease_expires_at_ms = Int64.add now_ms lease_ms in
  let update =
    doc
      [
        doc_element "$set"
          (doc
             [
               string "status" (Workflow_runtime.status_to_string Running);
               string "lease_owner" worker_id;
               int64 "lease_expires_at_ms" lease_expires_at_ms;
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "attempt" 1; int32 "event_sequence" 1 ]);
      ]
  in
  find_and_modify t ~query ~update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok None
  | Ok (Some (item, sequence)) ->
      let workflow_id = item.Workflow_runtime.workflow.id in
      let ( let* ) = Result.bind in
      let* sequence =
        fire_due_timers t ~workflow_id ~now_ms ~first_sequence:sequence
      in
      let* () =
        append_event t ~workflow_id ~sequence
          ~kind:Workflow_runtime.Workflow_claimed ~worker_id
          ~occurred_at_ms:now_ms ()
      in
      Ok (Some { Workflow_runtime.item; worker_id; lease_expires_at_ms })

let claim_next ?kind t ~worker_id ~now_ms ~lease_ms =
  claim_with_query t ~query:(claim_query ?kind ~now_ms ()) ~worker_id ~now_ms
    ~lease_ms

let claim_workflow t ~workflow_id ~worker_id ~now_ms ~lease_ms =
  claim_with_query t ~query:(claim_query ~workflow_id ~now_ms ()) ~worker_id
    ~now_ms ~lease_ms

let active_owned_query ~workflow_id ~worker_id ~now_ms =
  doc
    [
      string "_id" workflow_id;
      string "status" (Workflow_runtime.status_to_string Running);
      string "lease_owner" worker_id;
      doc_element "lease_expires_at_ms" (doc [ ("$gt", Bson.create_int64 now_ms) ]);
    ]

let update_owned t ~workflow_id ~worker_id ~now_ms update =
  find_and_modify t
    ~query:(active_owned_query ~workflow_id ~worker_id ~now_ms)
    ~update
  |> Result.map (Option.map snd)

let heartbeat t ~workflow_id ~worker_id ~now_ms ~lease_ms =
  let update =
    doc
      [
        doc_element "$set"
          (doc
             [
               int64 "lease_expires_at_ms" (Int64.add now_ms lease_ms);
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
      ]
  in
  update_owned t ~workflow_id ~worker_id ~now_ms update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_heartbeat ~worker_id
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let terminal_status = function
  | Workflow_runtime.Succeeded | Blocked | Failed | Cancelled -> true
  | Queued | Running -> false

let complete t ~workflow_id ~worker_id ~now_ms ~status ~message =
  if not (terminal_status status) then
    Error (`Bad_document "complete requires a terminal status")
  else
  let update =
    doc
      [
        doc_element "$set"
          (doc
             [
               string "status" (Workflow_runtime.status_to_string status);
               int64 "finished_at_ms" now_ms;
               string "message" message;
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
        doc_element "$unset"
          (doc [ string "lease_owner" ""; string "lease_expires_at_ms" "" ]);
      ]
  in
  update_owned t ~workflow_id ~worker_id ~now_ms update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_completed ~worker_id ~message
        ~payload_json:(completion_payload ~status ~message)
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let reschedule t ~workflow_id ~worker_id ~now_ms ~run_at_ms ~message =
  let update =
    doc
      [
        doc_element "$set"
          (doc
             [
               string "status" (Workflow_runtime.status_to_string Queued);
               int64 "run_at_ms" run_at_ms;
               string "message" message;
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
        doc_element "$unset"
          (doc
             [
               string "lease_owner" "";
               string "lease_expires_at_ms" "";
               string "finished_at_ms" "";
             ]);
      ]
  in
  update_owned t ~workflow_id ~worker_id ~now_ms update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_rescheduled ~worker_id ~message
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let retry t ~workflow_id ~worker_id ~now_ms ~policy ~message =
  let query = active_owned_query ~workflow_id ~worker_id ~now_ms in
  let find =
    Mongo_eio.direct_find_one t.client ~db:t.db
      ~collection:t.workflows_collection query
    |> Result.map_error mongo_error
  in
  let ( let* ) = Result.bind in
  let* current =
    match find with
    | Error _ as error -> error
    | Ok None -> Ok None
    | Ok (Some bson) -> decode_item bson |> Result.map Option.some
  in
  match current with
  | None -> Ok None
  | Some item ->
      if item.Workflow_runtime.attempt >= policy.Workflow_runtime.max_attempts
      then
        let* completed =
          complete t ~workflow_id ~worker_id ~now_ms ~status:Workflow_runtime.Failed
            ~message
        in
        if completed then
          Ok
            (Some
               (Workflow_runtime.Retries_exhausted
                  { attempt = item.Workflow_runtime.attempt }))
        else Ok None
      else
        let run_at_ms =
          Int64.add now_ms
            (Workflow_runtime.retry_delay_ms policy
               ~attempt:item.Workflow_runtime.attempt)
        in
        let* rescheduled =
          reschedule t ~workflow_id ~worker_id ~now_ms ~run_at_ms ~message
        in
        if rescheduled then
          Ok
            (Some
               (Workflow_runtime.Retried
                  { attempt = item.Workflow_runtime.attempt; run_at_ms }))
        else Ok None

let schedule_timer t ~workflow_id ~worker_id ~now_ms ~timer_id ~run_at_ms
    ?payload_json ~message () =
  let timer =
    Workflow_runtime.
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
  let timer_doc = timer_doc_of_timer timer in
  let timer_update =
    doc [ doc_element "$set" (timer_doc_to_bson_doc timer_doc) ]
  in
  let workflow_update =
    doc
      [
        doc_element "$set"
          (doc
             [
               string "status" (Workflow_runtime.status_to_string Queued);
               int64 "run_at_ms" run_at_ms;
               string "message" message;
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
        doc_element "$unset"
          (doc
             [
               string "lease_owner" "";
               string "lease_expires_at_ms" "";
               string "finished_at_ms" "";
             ]);
      ]
  in
  let ( let* ) = Result.bind in
  let* updated =
    update_owned t ~workflow_id ~worker_id ~now_ms workflow_update
  in
  match updated with
  | None -> Ok false
  | Some sequence ->
      let* _ =
        Mongo_eio.direct_update_one t.client ~db:t.db
          ~collection:t.timers_collection ~upsert:true
          (doc [ string "_id" timer_doc.id ])
          timer_update
        |> Result.map_error mongo_error
      in
      append_event t ~workflow_id ~sequence ~kind:Workflow_runtime.Timer_scheduled
        ~worker_id ~payload_json:(timer_payload ~timer_id ~run_at_ms) ~message
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let snapshot ?tenant_id t =
  let filter =
    match tenant_id with
    | None -> Bson.empty
    | Some tenant_id -> doc [ string "tenant_id" tenant_id ]
  in
  let opts =
    {
      (Mongo_crud.default_find t.workflows_collection filter) with
      sort = Some (doc [ int32 "updated_at_ms" (-1) ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db
    ~collection:t.workflows_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok items -> (
              match decode_item bson with
              | Ok item -> Ok (item :: items)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let children ~parent_workflow_id t =
  let filter = doc [ string "parent_workflow_id" parent_workflow_id ] in
  let opts =
    {
      (Mongo_crud.default_find t.child_workflows_collection filter) with
      sort = Some (doc [ int32 "started_at_ms" (-1); int32 "child_workflow_id" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db
    ~collection:t.child_workflows_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      let ( let* ) = Result.bind in
      let* children =
        List.fold_left
          (fun acc bson ->
            match acc with
            | Error _ as error -> error
            | Ok children -> (
                match decode_child bson with
                | Ok child -> Ok (child :: children)
                | Error _ as error -> error))
          (Ok []) docs
        |> Result.map List.rev
      in
      List.fold_left
        (fun acc (child : Workflow_runtime.child_workflow) ->
          match acc with
          | Error _ as error -> error
          | Ok items -> (
              Mongo_eio.direct_find_one t.client ~db:t.db
                ~collection:t.workflows_collection
                (doc [ string "_id" child.child_workflow_id ])
              |> Result.map_error mongo_error
              |> function
              | Error _ as error -> error
              | Ok None -> Ok items
              | Ok (Some bson) -> (
                  match decode_item bson with
                  | Ok item -> Ok (item :: items)
                  | Error _ as error -> error)))
        (Ok []) children
      |> Result.map List.rev

let history ~workflow_id t =
  let filter = doc [ string "workflow_id" workflow_id ] in
  let opts =
    {
      (Mongo_crud.default_find t.events_collection filter) with
      sort = Some (doc [ int32 "sequence" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.events_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok events -> (
              match decode_event bson with
              | Ok event -> Ok (event :: events)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let event_exists t ~workflow_id ~kind ~payload_json =
  let ( let* ) = Result.bind in
  let* events = history ~workflow_id t in
  Ok
    (List.exists
       (fun (event : Workflow_runtime.event) ->
         event.kind = kind && event.payload_json = Some payload_json)
       events)

let timers ~workflow_id t =
  let filter = doc [ string "workflow_id" workflow_id ] in
  let opts =
    {
      (Mongo_crud.default_find t.timers_collection filter) with
      sort = Some (doc [ int32 "run_at_ms" 1; int32 "timer_id" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.timers_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok timers -> (
              match decode_timer bson with
              | Ok timer -> Ok (timer :: timers)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let signals ~workflow_id t =
  let filter = doc [ string "workflow_id" workflow_id ] in
  let opts =
    {
      (Mongo_crud.default_find t.signals_collection filter) with
      sort = Some (doc [ int32 "received_at_ms" 1; int32 "signal_id" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.signals_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok signals -> (
              match decode_signal bson with
              | Ok signal -> Ok (signal :: signals)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let updates ~workflow_id t =
  let filter = doc [ string "workflow_id" workflow_id ] in
  let opts =
    {
      (Mongo_crud.default_find t.updates_collection filter) with
      sort = Some (doc [ int32 "requested_at_ms" 1; int32 "update_id" 1 ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.updates_collection opts
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok docs ->
      List.fold_left
        (fun acc bson ->
          match acc with
          | Error _ as error -> error
          | Ok updates -> (
              match decode_update bson with
              | Ok update -> Ok (update :: updates)
              | Error _ as error -> error))
        (Ok []) docs
      |> Result.map List.rev

let query_state ~workflow_id t =
  let ( let* ) = Result.bind in
  let* events = history ~workflow_id t in
  match events with
  | [] -> Ok None
  | events -> (
      match Workflow_runtime.replay events with
      | Ok state -> Ok (Some state)
      | Error message -> Error (`Bad_document message))

let compact_history ~workflow_id t =
  let ( let* ) = Result.bind in
  let* events = history ~workflow_id t in
  match events with
  | [] -> Ok None
  | events ->
      let* state =
        match Workflow_runtime.replay events with
        | Ok state -> Ok state
        | Error message -> Error (`Bad_document message)
      in
      let* sequence =
        match increment_event_sequence t ~workflow_id with
        | Error _ as error -> error
        | Ok (Some sequence) -> Ok sequence
        | Ok None -> Error (`Bad_document ("unknown workflow: " ^ workflow_id))
      in
      let occurred_at_ms =
        events
        |> List.fold_left
             (fun latest (event : Workflow_runtime.event) ->
               Int64.max latest event.occurred_at_ms)
             0L
      in
      let* () =
        append_event t ~workflow_id ~sequence
          ~kind:Workflow_runtime.History_compacted
          ~payload_json:
            (Workflow_runtime.replay_state_to_yojson state
            |> Yojson.Safe.to_string)
          ~message:"history compacted" ~occurred_at_ms ()
      in
      Mongo_eio.direct_delete_many t.client ~db:t.db
        ~collection:t.events_collection
        (doc
           [
             string "workflow_id" workflow_id;
             doc_element "sequence" (doc [ int32 "$lt" sequence ]);
           ])
      |> Result.map_error mongo_error
      |> Result.map (fun _ -> Some sequence)

let ensure_signal_event t ~workflow_id ~now_ms (signal : Workflow_runtime.signal) =
  let payload_json = signal_payload signal in
  let ( let* ) = Result.bind in
  let* exists =
    event_exists t ~workflow_id ~kind:Workflow_runtime.Signal_received
      ~payload_json
  in
  if exists then Ok true
  else
    let query = doc [ string "_id" workflow_id; non_terminal_status_filter ] in
    let update =
      doc
        [
          doc_element "$set"
            (doc
               [
                 string "status" (Workflow_runtime.status_to_string Queued);
                 string "message" ("signal: " ^ signal.name);
                 int64 "updated_at_ms" now_ms;
               ]);
          doc_element "$min" (doc [ int64 "run_at_ms" now_ms ]);
          doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
          doc_element "$unset"
            (doc [ string "lease_owner" ""; string "lease_expires_at_ms" "" ]);
        ]
    in
    find_and_modify t ~query ~update
    |> function
    | Error _ as error -> error
    | Ok None -> Ok false
    | Ok (Some (_item, sequence)) ->
        append_event t ~workflow_id ~sequence
          ~kind:Workflow_runtime.Signal_received ~payload_json
          ~message:signal.name ~occurred_at_ms:now_ms ()
        |> Result.map (fun () -> true)

let ensure_update_requested_event t ~workflow_id ~now_ms
    (update : Workflow_runtime.workflow_update) =
  let payload_json = update_payload update in
  let ( let* ) = Result.bind in
  let* exists =
    event_exists t ~workflow_id ~kind:Workflow_runtime.Update_requested
      ~payload_json
  in
  if exists then Ok true
  else
    let query = doc [ string "_id" workflow_id; non_terminal_status_filter ] in
    let workflow_update =
      doc
        [
          doc_element "$set"
            (doc
               [
                 string "status" (Workflow_runtime.status_to_string Queued);
                 string "message" ("update: " ^ update.name);
                 int64 "updated_at_ms" now_ms;
               ]);
          doc_element "$min" (doc [ int64 "run_at_ms" now_ms ]);
          doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
          doc_element "$unset"
            (doc [ string "lease_owner" ""; string "lease_expires_at_ms" "" ]);
        ]
    in
    find_and_modify t ~query ~update:workflow_update
    |> function
    | Error _ as error -> error
    | Ok None -> Ok false
    | Ok (Some (_item, sequence)) ->
        append_event t ~workflow_id ~sequence
          ~kind:Workflow_runtime.Update_requested ~payload_json
          ~message:update.name ~occurred_at_ms:now_ms ()
        |> Result.map (fun () -> true)

let signal t ~workflow_id ~now_ms ~signal_id ~name ?payload_json () =
  let ( let* ) = Result.bind in
  let* workflow_exists =
    Mongo_eio.direct_find_one t.client ~db:t.db
      ~collection:t.workflows_collection
      (doc [ string "_id" workflow_id; non_terminal_status_filter ])
    |> Result.map_error mongo_error
    |> Result.map Option.is_some
  in
  if not workflow_exists then Ok false
  else
    let signal =
      Workflow_runtime.
        { signal_id; workflow_id; name; payload_json; received_at_ms = now_ms }
    in
    let signal_doc = signal_doc_of_signal signal in
    let* signal_write =
      Mongo_eio.direct_update_one t.client ~db:t.db
        ~collection:t.signals_collection ~upsert:true
        (doc [ string "_id" signal_doc.id ])
        (doc
           [
             doc_element "$setOnInsert"
               (signal_doc_to_bson_doc signal_doc);
           ])
      |> Result.map_error mongo_error
    in
    if signal_write.Mongo_crud.upserted_ids = [] then
      Mongo_eio.direct_find_one t.client ~db:t.db
        ~collection:t.signals_collection
        (doc [ string "_id" signal_doc.id ])
      |> Result.map_error mongo_error
      |> function
      | Error _ as error -> error
      | Ok None -> Ok false
      | Ok (Some bson) ->
          let* signal = decode_signal bson in
          ensure_signal_event t ~workflow_id ~now_ms signal
    else
      ensure_signal_event t ~workflow_id ~now_ms signal

let cancel t ~workflow_id ~now_ms ~reason =
  let query = doc [ string "_id" workflow_id; non_terminal_status_filter ] in
  let update =
    doc
      [
        doc_element "$set"
          (doc
             [
               string "status" (Workflow_runtime.status_to_string Cancelled);
               int64 "finished_at_ms" now_ms;
               string "message" reason;
               int64 "updated_at_ms" now_ms;
             ]);
        doc_element "$inc" (doc [ int32 "event_sequence" 1 ]);
        doc_element "$unset"
          (doc [ string "lease_owner" ""; string "lease_expires_at_ms" "" ]);
      ]
  in
  find_and_modify t ~query ~update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some (_item, sequence)) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_cancelled
        ~payload_json:(cancel_payload ~reason) ~message:reason
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let find_child_link t ~parent_workflow_id ~child_workflow_id =
  Mongo_eio.direct_find_one t.client ~db:t.db
    ~collection:t.child_workflows_collection
    (doc
       [
         string "_id"
           (child_workflow_key ~parent_workflow_id ~child_workflow_id);
       ])
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok None -> Ok None
  | Ok (Some bson) -> decode_child bson |> Result.map Option.some

let child_started_event_exists t ~parent_workflow_id ~child_workflow_id =
  let ( let* ) = Result.bind in
  let* events = history ~workflow_id:parent_workflow_id t in
  Ok
    (List.exists
       (fun (event : Workflow_runtime.event) ->
         match (event.kind, event.message) with
         | Child_workflow_started, Some message ->
             String.equal message child_workflow_id
         | _ -> false)
       events)

let ensure_child_started_event t ~parent_workflow_id ~worker_id ~now_ms
    (child : Workflow_runtime.child_workflow) =
  let ( let* ) = Result.bind in
  let* exists =
    child_started_event_exists t ~parent_workflow_id
      ~child_workflow_id:child.child_workflow_id
  in
  if exists then Ok true
  else
    let update = doc [ doc_element "$inc" (doc [ int32 "event_sequence" 1 ]) ] in
    update_owned t ~workflow_id:parent_workflow_id ~worker_id ~now_ms update
    |> function
    | Error _ as error -> error
    | Ok None -> Ok false
    | Ok (Some sequence) ->
        append_event t ~workflow_id:parent_workflow_id ~sequence
          ~kind:Workflow_runtime.Child_workflow_started ~worker_id
          ~payload_json:(child_workflow_payload child)
          ~message:child.child_workflow_id ~occurred_at_ms:child.started_at_ms ()
        |> Result.map (fun () -> true)

let start_child t ~parent_workflow_id ~worker_id ~now_ms
    (workflow : Workflow_runtime.workflow)
    (options : Workflow_runtime.enqueue_options) =
  let ( let* ) = Result.bind in
  let* parent =
    Mongo_eio.direct_find_one t.client ~db:t.db
      ~collection:t.workflows_collection
      (active_owned_query ~workflow_id:parent_workflow_id ~worker_id ~now_ms)
    |> Result.map_error mongo_error
    |> function
    | Error _ as error -> error
    | Ok None -> Ok None
    | Ok (Some bson) -> decode_item bson |> Result.map Option.some
  in
  match parent with
  | None -> Ok false
  | Some parent when
      (match parent.Workflow_runtime.status with
      | Succeeded | Blocked | Failed | Cancelled -> true
      | Queued | Running -> false) ->
      Ok false
  | Some _ ->
      let* existing_child =
        find_child_link t ~parent_workflow_id ~child_workflow_id:workflow.id
      in
      if Option.is_some existing_child then
        ensure_child_started_event t ~parent_workflow_id ~worker_id ~now_ms
          (Option.get existing_child)
      else
        let item =
          Workflow_runtime.
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
        let workflow_doc =
          { (workflow_doc_of_item item) with event_sequence = Some 1 }
        in
        let child =
          Workflow_runtime.
            {
              parent_workflow_id;
              child_workflow_id = workflow.id;
              child_kind = workflow.kind;
              started_at_ms = now_ms;
            }
        in
        let child_doc = child_workflow_doc_of_child child in
        let* () =
          Mongo_eio.direct_insert_one t.client ~db:t.db
            ~collection:t.workflows_collection
            (workflow_doc_to_bson_doc workflow_doc)
          |> Result.map (fun _ -> ())
          |> Result.map_error (fun error ->
                 if Mongo_error.is_duplicate_key error then
                   `Duplicate_workflow workflow.Workflow_runtime.id
                 else mongo_error error)
        in
        let* () =
          append_event t ~workflow_id:workflow.id ~sequence:1
            ~kind:Workflow_runtime.Workflow_enqueued
            ?payload_json:options.payload_json ~occurred_at_ms:now_ms ()
        in
        let* () =
          Mongo_eio.direct_insert_one t.client ~db:t.db
            ~collection:t.child_workflows_collection
            (child_workflow_doc_to_bson_doc child_doc)
          |> Result.map (fun _ -> ())
          |> Result.map_error mongo_error
        in
        ensure_child_started_event t ~parent_workflow_id ~worker_id ~now_ms child

let request_update t ~workflow_id ~now_ms ~update_id ~name ?payload_json () =
  let ( let* ) = Result.bind in
  let* workflow_exists =
    Mongo_eio.direct_find_one t.client ~db:t.db
      ~collection:t.workflows_collection
      (doc [ string "_id" workflow_id; non_terminal_status_filter ])
    |> Result.map_error mongo_error
    |> Result.map Option.is_some
  in
  if not workflow_exists then Ok false
  else
    let update =
      Workflow_runtime.
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
    let update_doc = update_doc_of_update update in
    let* write =
      Mongo_eio.direct_update_one t.client ~db:t.db
        ~collection:t.updates_collection ~upsert:true
        (doc [ string "_id" update_doc.id ])
        (doc [ doc_element "$setOnInsert" (workflow_update_doc_to_bson_doc update_doc) ])
      |> Result.map_error mongo_error
    in
    if write.Mongo_crud.upserted_ids = [] then
      Mongo_eio.direct_find_one t.client ~db:t.db
        ~collection:t.updates_collection
        (doc [ string "_id" update_doc.id ])
      |> Result.map_error mongo_error
      |> function
      | Error _ as error -> error
      | Ok None -> Ok false
      | Ok (Some bson) ->
          let* update = decode_update bson in
          ensure_update_requested_event t ~workflow_id ~now_ms update
    else
      ensure_update_requested_event t ~workflow_id ~now_ms update

let complete_update t ~workflow_id ~worker_id ~now_ms ~update_id ~status
    ?result_json ?error () =
  match status with
  | Workflow_runtime.Update_pending ->
      Error (`Bad_document "complete_update requires a terminal update status")
  | Update_completed_status | Update_rejected | Update_failed ->
      let ( let* ) = Result.bind in
      let* owned =
        Mongo_eio.direct_find_one t.client ~db:t.db
          ~collection:t.workflows_collection
          (active_owned_query ~workflow_id ~worker_id ~now_ms)
        |> Result.map_error mongo_error
        |> Result.map Option.is_some
      in
      if not owned then Ok false
      else
        let id = update_key ~workflow_id ~update_id in
        let* current =
          Mongo_eio.direct_find_one t.client ~db:t.db
            ~collection:t.updates_collection
            (doc [ string "_id" id ])
          |> Result.map_error mongo_error
          |> function
          | Error _ as error -> error
          | Ok None -> Ok None
          | Ok (Some bson) -> decode_update bson |> Result.map Option.some
        in
        match current with
        | None -> Ok false
        | Some existing when existing.Workflow_runtime.status <> Update_pending ->
            Ok true
        | Some existing ->
            let completed =
              Workflow_runtime.
                {
                  existing with
                  status;
                  result_json;
                  error;
                  completed_at_ms = Some now_ms;
                }
            in
            let completed_doc = update_doc_of_update completed in
            let* _ =
              Mongo_eio.direct_update_one t.client ~db:t.db
                ~collection:t.updates_collection ~upsert:false
                (doc [ string "_id" id ])
                (doc
                   [
                     doc_element "$set"
                       (workflow_update_doc_to_bson_doc completed_doc);
                   ])
              |> Result.map_error mongo_error
            in
            let workflow_update =
              doc [ doc_element "$inc" (doc [ int32 "event_sequence" 1 ]) ]
            in
            update_owned t ~workflow_id ~worker_id ~now_ms workflow_update
            |> function
            | Error _ as error -> error
            | Ok None -> Ok false
            | Ok (Some sequence) ->
                append_event t ~workflow_id ~sequence
                  ~kind:Workflow_runtime.Update_completed ~worker_id
                  ~payload_json:(update_payload completed) ?message:error
                  ~occurred_at_ms:now_ms ()
                |> Result.map (fun () -> true)

let record_activity_result t ~now_ms (result : Workflow_runtime.activity_result) =
  let result = { result with Workflow_runtime.updated_at_ms = now_ms } in
  let result_doc = activity_result_doc_of_result result in
  let update =
    doc [ doc_element "$set" (activity_result_doc_to_bson_doc result_doc) ]
  in
  let filter = doc [ string "_id" result_doc.id ] in
  let ( let* ) = Result.bind in
  let* _ =
    Mongo_eio.direct_update_one t.client ~db:t.db
      ~collection:t.activity_results_collection ~upsert:true filter update
    |> Result.map_error mongo_error
  in
  let* sequence =
    match increment_event_sequence t ~workflow_id:result.workflow_id with
    | Error _ as error -> error
    | Ok (Some sequence) -> Ok sequence
    | Ok None -> Error (`Bad_document ("unknown workflow: " ^ result.workflow_id))
  in
  let kind =
    match result.status with
    | Workflow_runtime.Activity_succeeded -> Workflow_runtime.Activity_completed
    | Workflow_runtime.Activity_failed -> Workflow_runtime.Activity_failed
  in
  append_event t ~workflow_id:result.workflow_id ~sequence ~kind
    ~payload_json:(activity_payload result) ?message:result.error
    ~occurred_at_ms:now_ms ()

let find_activity_result t ~workflow_id ~activity_id =
  let id = activity_result_key ~workflow_id ~activity_id in
  Mongo_eio.direct_find_one t.client ~db:t.db
    ~collection:t.activity_results_collection
    (doc [ string "_id" id ])
  |> Result.map_error mongo_error
  |> function
  | Error _ as error -> error
  | Ok None -> Ok None
  | Ok (Some bson) -> decode_activity_result bson |> Result.map Option.some
