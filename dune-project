(lang dune 2.1)

(name dune-distributed-storage)

(version 0.1.0)

(generate_opam_files true)

(license MIT)

(maintainers "Jane Street Group, LLC <opensource@janestreet.com>")

(authors "Jane Street Group, LLC <opensource@janestreet.com>")

(source
 (github ocaml/dune-distributed-storage))

(documentation "https://dune.readthedocs.io/")

(package
 (name dune-distributed-storage)
 (synopsis "Dune distributed artifact storage server")
 (depends
  async
  cmdliner
  (dune :dev)
  (dune-private-libs
   (>= 2.4.0))
  fmt
  logs
  logs-async
  (ocamlformat
   (and
    (= 0.15.0)
    :dev))
  uri))
