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
}

let create ~client ~db ~collection () =
  {
    client;
    db;
    workflows_collection = collection;
    events_collection = collection ^ "_events";
    activity_results_collection = collection ^ "_activity_results";
  }

let error_to_string = function
  | `Bad_document message -> "bad document: " ^ message
  | `Duplicate_workflow id -> "workflow already exists: " ^ id
  | `Mongo message -> "mongo: " ^ message

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

let enqueue t ~now_ms workflow options =
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

let claim_query ?workflow_id ~now_ms () =
  let id_filter =
    match workflow_id with None -> [] | Some id -> [ string "_id" id ]
  in
  doc
    (id_filter
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
      let* () =
        append_event t ~workflow_id ~sequence
          ~kind:Workflow_runtime.Workflow_claimed ~worker_id
          ~occurred_at_ms:now_ms ()
      in
      Ok (Some { Workflow_runtime.item; worker_id; lease_expires_at_ms })

let claim_next t ~worker_id ~now_ms ~lease_ms =
  claim_with_query t ~query:(claim_query ~now_ms ()) ~worker_id ~now_ms
    ~lease_ms

let claim_workflow t ~workflow_id ~worker_id ~now_ms ~lease_ms =
  claim_with_query t ~query:(claim_query ~workflow_id ~now_ms ()) ~worker_id
    ~now_ms ~lease_ms

let owned_query ~workflow_id ~worker_id =
  doc [ string "_id" workflow_id; string "lease_owner" worker_id ]

let update_owned t ~workflow_id ~worker_id update =
  find_and_modify t ~query:(owned_query ~workflow_id ~worker_id) ~update
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
  update_owned t ~workflow_id ~worker_id update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_heartbeat ~worker_id
        ~occurred_at_ms:now_ms ()
      |> Result.map (fun () -> true)

let complete t ~workflow_id ~worker_id ~now_ms ~status ~message =
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
  update_owned t ~workflow_id ~worker_id update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_completed ~worker_id ~message
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
  update_owned t ~workflow_id ~worker_id update
  |> function
  | Error _ as error -> error
  | Ok None -> Ok false
  | Ok (Some sequence) ->
      append_event t ~workflow_id ~sequence
        ~kind:Workflow_runtime.Workflow_rescheduled ~worker_id ~message
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

let increment_event_sequence t ~workflow_id =
  let query = doc [ string "_id" workflow_id ] in
  let update = doc [ doc_element "$inc" (doc [ int32 "event_sequence" 1 ]) ] in
  find_and_modify t ~query ~update
  |> Result.map (function
       | None -> None
       | Some (_item, sequence) -> Some sequence)

let record_activity_result t ~now_ms result =
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
    ?payload_json:result.result_json ?message:result.error ~occurred_at_ms:now_ms ()

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
