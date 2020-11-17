open Async.Rpc

val block_get : (string, (string * bool) option) Rpc.t

val block_has : (string, bool) Rpc.t

val block_put : (string * bool * string, unit) Rpc.t

val index_get : (string * string, string list option) Rpc.t

val index_put : (string * string * string list, unit) Rpc.t

val metadata_put : (string * string, unit) Rpc.t
