type error =
  [ `Bad_document of string
  | `Duplicate_workflow of string
  | `Mongo of string ]

type t = {
  client : Mongo_eio.direct_client;
  db : string;
  collection : string;
}

let create ~client ~db ~collection () = { client; db; collection }

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
  created_at_ms : int64;
  updated_at_ms : int64;
}
[@@deriving bson]

let doc fields =
  List.fold_right (fun (name, element) acc -> Bson.add_element name element acc)
    fields Bson.empty

let string name value = (name, Bson.create_string value)
let int32 name value = (name, Bson.create_int32 (Int32.of_int value))
let int64 name value = (name, Bson.create_int64 value)
let bool name value = (name, Bson.create_boolean value)
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
    created_at_ms = item.created_at_ms;
    updated_at_ms = item.updated_at_ms;
  }

let item_of_workflow_doc doc =
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

let decode_item bson =
  match workflow_doc_of_bson_doc_result bson with
  | Error message -> Error (`Bad_document message)
  | Ok doc -> item_of_workflow_doc doc

let ensure t =
  let key fields =
    Bson.add_element "key" (Bson.create_doc_element (doc fields)) Bson.empty
  in
  let ( let* ) = Result.bind in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db ~collection:t.collection
      (key [ int32 "status" 1; int32 "run_at_ms" 1 ])
      [ Mongo_index.Name "workflow_due_idx" ]
    |> Result.map_error mongo_error
  in
  let* () =
    Mongo_eio.direct_ensure_index t.client ~db:t.db ~collection:t.collection
      (key [ int32 "tenant_id" 1; int32 "updated_at_ms" (-1) ])
      [ Mongo_index.Name "workflow_tenant_updated_idx" ]
    |> Result.map_error mongo_error
  in
  Ok ()

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
  Mongo_eio.direct_insert_one t.client ~db:t.db ~collection:t.collection
    (workflow_doc_to_bson_doc (workflow_doc_of_item item))
  |> Result.map (fun _ -> ())
  |> Result.map_error (fun error ->
         if Mongo_error.is_duplicate_key error then
           `Duplicate_workflow workflow.Workflow_runtime.id
         else mongo_error error)

let find_and_modify t ~query ~update =
  Mongo_eio.direct_run_command t.client t.db
    [
      ("findAndModify", Bson.create_string t.collection);
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
      | doc -> decode_item doc |> Result.map Option.some)

let claim_next t ~worker_id ~now_ms ~lease_ms =
  let lease_expires_at_ms = Int64.add now_ms lease_ms in
  let query =
    doc
      [
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
      ]
  in
  let set_doc =
    doc
      [
        string "status" (Workflow_runtime.status_to_string Running);
        string "lease_owner" worker_id;
        int64 "lease_expires_at_ms" lease_expires_at_ms;
        int64 "updated_at_ms" now_ms;
      ]
  in
  let update =
    doc
      [
        doc_element "$set" set_doc;
        doc_element "$inc" (doc [ int32 "attempt" 1 ]);
        doc_element "$setOnInsert" Bson.empty;
      ]
  in
  find_and_modify t ~query ~update
  |> Result.map (Option.map (fun item -> { Workflow_runtime.item; worker_id; lease_expires_at_ms }))

let owned_query ~workflow_id ~worker_id =
  doc [ string "_id" workflow_id; string "lease_owner" worker_id ]

let update_owned t ~workflow_id ~worker_id update =
  Mongo_eio.direct_update_one t.client ~db:t.db ~collection:t.collection
    ~upsert:false (owned_query ~workflow_id ~worker_id) update
  |> Result.map_error mongo_error
  |> Result.map (fun result -> result.Mongo_crud.matched_count = 1)

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
      ]
  in
  update_owned t ~workflow_id ~worker_id update

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
        doc_element "$unset"
          (doc [ string "lease_owner" ""; string "lease_expires_at_ms" "" ]);
      ]
  in
  update_owned t ~workflow_id ~worker_id update

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

let snapshot ?tenant_id t =
  let filter =
    match tenant_id with
    | None -> Bson.empty
    | Some tenant_id -> doc [ string "tenant_id" tenant_id ]
  in
  let opts =
    {
      (Mongo_crud.default_find t.collection filter) with
      sort = Some (doc [ int32 "updated_at_ms" (-1) ]);
    }
  in
  Mongo_eio.direct_find t.client ~db:t.db ~collection:t.collection opts
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
