open Base
open Async.Rpc

(** Data being piped by block_get pipe RPC. An implementation detail, use
    [encode_block_get] and [decode_block_get] to make it intelligible. *)
type block_pipe

type block =
  { contents : string Async.Pipe.Reader.t
  ; executable : bool
  }

type hash = string

type contents = string

type category = string

type executable = bool

(** Get any block from the peer *)
val block_get : (hash, executable, string, unit) State_rpc.t

(** Fetch an index from the given category from the distributed storage *)
val index_get : (category * hash, string list option) Rpc.t

(** Upload an index in the given category to the distributed storage *)
val index_put : (category * hash * contents list, unit) Rpc.t

(** Upload a metadata file to the distributed storage *)
val metadata_put : (hash * contents, unit) Rpc.t
