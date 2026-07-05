type error =
  [ `Bad_document of string
  | `Duplicate_workflow of string
  | `Mongo of string ]

type t

val create :
  client:Mongo_eio.direct_client -> db:string -> collection:string -> unit -> t

val error_to_string : error -> string

include
  Workflow_runtime.BACKEND
    with type t := t
     and type error := error
