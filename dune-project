(lang dune 3.22)
(name workflow-runtime)
(generate_opam_files true)

(package
 (name workflow-runtime)
 (synopsis "Small in-process workflow observation runtime for OCaml")
 (description
  "workflow-runtime tracks in-process workflow lifecycle state by durable workflow id, tenant, kind, subject, and metadata, with structured and JSON snapshots.")
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
  (alcotest :with-test)))
