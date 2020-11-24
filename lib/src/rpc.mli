open Base
open Async.Rpc

type block_pipe

val block_get : (string, block_pipe, unit) Pipe_rpc.t

val block_has : (string, bool) Rpc.t

val block_put : (string * bool * string, unit) Rpc.t

val index_get : (string * string, string list option) Rpc.t

val index_put : (string * string * string list, unit) Rpc.t

val metadata_put : (string * string, unit) Rpc.t

(** Transform the response of the block_get pipe RPC into an intelligible stream
    of the file content and executable boolean *)
val decode_block_get :
     ((block_pipe Async.Pipe.Reader.t * _, _) Result.t, _) Result.t
  -> (string Async.Pipe.Reader.t * bool) option Async_kernel.Deferred.t

(** Transform an executable boolean and some file contents into a block_get pipe
    RPC response *)
val encode_block_get :
  bool -> string Async.Pipe.Reader.t -> block_pipe Async.Pipe.Reader.t
