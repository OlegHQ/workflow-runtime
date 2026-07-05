let env name default =
  match Sys.getenv_opt name with Some "" | None -> default | Some value -> value

let host = env "POSTER_MONGO_HOST" "oracle-vm"
let port = env "POSTER_MONGO_PORT" "27017" |> int_of_string

let db =
  Printf.sprintf "workflow_runtime_e2e_%d_%d" (Unix.getpid ()) (Random.bits ())

let collection = "workflows"

let workflow id =
  Workflow_runtime.
    {
      id;
      tenant_id = "tenant_a";
      kind = "integration";
      subject_id = Some ("subject_" ^ id);
      name = Some "mongo";
      metadata = [ ("test", "mongo_backend") ];
    }

let expect_ok label = function
  | Ok value -> value
  | Error error ->
      Alcotest.fail (label ^ ": " ^ Workflow_runtime_mongo.error_to_string error)

let test_mongo_claims_and_lease_recovery () =
  Eio_main.run @@ fun env ->
  Eio.Switch.run @@ fun sw ->
  let config =
    Mongo_config.
      {
        (default ~host ~port ~database:db ()) with
        direct_connection = true;
        server_selection_timeout_ms = 2_000;
        connect_timeout_ms = 2_000;
        socket_timeout_ms = Some 5_000;
        app_name = Some "workflow-runtime-e2e";
      }
  in
  let client =
    match
      Mongo_eio.connect ~sw ~net:(Eio.Stdenv.net env)
        ~clock:(Eio.Stdenv.clock env) ~config
    with
    | Ok client -> client
    | Error error ->
        Alcotest.fail ("connect: " ^ Mongo_error.to_string error)
  in
  Fun.protect
    ~finally:(fun () ->
      let _ =
        Mongo_eio.direct_run_command client db
          [ ("dropDatabase", Bson.create_int32 1l) ]
      in
      Mongo_eio.close_direct client)
    (fun () ->
      let backend_a =
        Workflow_runtime_mongo.create ~client ~db ~collection ()
      in
      let backend_b =
        Workflow_runtime_mongo.create ~client ~db ~collection ()
      in
      Workflow_runtime_mongo.ensure backend_a |> expect_ok "ensure";
      Workflow_runtime_mongo.enqueue backend_a ~now_ms:1_000L (workflow "wf_1")
        (Workflow_runtime.enqueue_options ~run_at_ms:1_000L ())
      |> expect_ok "enqueue wf_1";
      let first =
        Workflow_runtime_mongo.claim_next backend_a ~worker_id:"worker_a"
          ~now_ms:1_000L ~lease_ms:100L
        |> expect_ok "claim worker_a"
        |> Option.get
      in
      Alcotest.(check string) "first worker" "worker_a" first.worker_id;
      let second =
        Workflow_runtime_mongo.claim_next backend_b ~worker_id:"worker_b"
          ~now_ms:1_000L ~lease_ms:100L
        |> expect_ok "claim worker_b"
      in
      Alcotest.(check bool) "not double claimed" true (Option.is_none second);
      let recovered =
        Workflow_runtime_mongo.claim_next backend_b ~worker_id:"worker_b"
          ~now_ms:1_101L ~lease_ms:100L
        |> expect_ok "claim expired"
        |> Option.get
      in
      Alcotest.(check string) "recovery worker" "worker_b" recovered.worker_id;
      Alcotest.(check int) "attempt incremented" 2 recovered.item.attempt;
      Workflow_runtime_mongo.complete backend_b ~workflow_id:"wf_1"
        ~worker_id:"worker_b" ~now_ms:1_102L ~status:Succeeded ~message:"done"
      |> expect_ok "complete"
      |> Alcotest.(check bool) "completed" true;
      let snapshot =
        Workflow_runtime_mongo.snapshot backend_a |> expect_ok "snapshot"
      in
      Alcotest.(check int) "one workflow" 1 (List.length snapshot);
      Alcotest.(check string)
        "succeeded" "succeeded"
        (Workflow_runtime.status_to_string (List.hd snapshot).status))

let () =
  Mirage_crypto_rng_unix.use_default ();
  Alcotest.run "workflow-runtime-mongo"
    [
      ( "mongo",
        [
          Alcotest.test_case "claims and lease recovery" `Quick
            test_mongo_claims_and_lease_recovery;
        ] );
    ]
