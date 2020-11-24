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

type name = string

type executable = bool

val block_get : (hash, block_pipe, unit) Pipe_rpc.t

val block_has : (hash, bool) Rpc.t

val block_put : (hash * executable * contents, unit) Rpc.t

val index_get : (name * hash, string list option) Rpc.t

val index_put : (name * hash * contents list, unit) Rpc.t

val metadata_put : (hash * contents, unit) Rpc.t

(** Transform the response of the block_get pipe RPC into an intelligible stream
    of the file content and executable boolean *)
val decode_block_get :
     ((block_pipe Async.Pipe.Reader.t * _, _) Result.t, _) Result.t
  -> block option Async_kernel.Deferred.t

(** Transform an executable boolean and some file contents into a block_get pipe
    RPC response *)
val encode_block_get : block -> block_pipe Async.Pipe.Reader.t
