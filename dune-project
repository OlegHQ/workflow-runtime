(lang dune 3.22)
(name workflow-runtime)
(generate_opam_files true)

(package
 (name workflow-runtime)
 (synopsis "Durable backend-backed workflow runtime for OCaml")
 (description
  "workflow-runtime tracks durable workflow lifecycle state, worker claims, leases, replay history, timers, retries, updates, child workflows, and backend-backed execution for OCaml services.")
 (authors OlegHQ)
 (maintainers OlegHQ)
 (license MIT)
 (homepage "https://github.com/OlegHQ/workflow-runtime")
 (bug_reports "https://github.com/OlegHQ/workflow-runtime/issues")
 (depends
  (ocaml (>= 5.2.0))
  (dune (>= 3.22))
  yojson
  base-threads
  bson
  mongo
  eio
  eio_main
  mirage-crypto-rng
  (alcotest :with-test)))
