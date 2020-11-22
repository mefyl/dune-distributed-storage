open Async
open Core

let ( let* ) = ( >>= )

let block_get =
  let bin_query = [%bin_type_class: string]
  and bin_response = [%bin_type_class: (string, bool) Either.t]
  and bin_error = [%bin_type_class: unit] in
  Async.Rpc.Pipe_rpc.create ~name:"get_block" ~version:0 ~bin_query
    ~bin_response ~bin_error ()

let block_has =
  let bin_query = [%bin_type_class: string]
  and bin_response = [%bin_type_class: bool] in
  Async.Rpc.Rpc.create ~name:"has_block" ~version:0 ~bin_query ~bin_response

let block_put =
  let bin_query = [%bin_type_class: string * bool * string]
  and bin_response = [%bin_type_class: unit] in
  Async.Rpc.Rpc.create ~name:"put_block" ~version:0 ~bin_query ~bin_response

let index_get =
  let bin_query = [%bin_type_class: string * string]
  and bin_response = [%bin_type_class: string list option] in
  Async.Rpc.Rpc.create ~name:"get_index" ~version:0 ~bin_query ~bin_response

let index_put =
  let bin_query = [%bin_type_class: string * string * string list]
  and bin_response = [%bin_type_class: unit] in
  Async.Rpc.Rpc.create ~name:"put_index" ~version:0 ~bin_query ~bin_response

let metadata_put =
  let bin_query = [%bin_type_class: string * string]
  and bin_response = [%bin_type_class: unit] in
  Async.Rpc.Rpc.create ~name:"put_metadata" ~version:0 ~bin_query ~bin_response

let decode_block_get = function
  | Result.Ok (Result.Ok (pipe, _)) -> (
    Async.Pipe.read pipe >>= function
    | `Eof
    | `Ok (Core.Either.First _) ->
      let* () =
        Logs_async.err (fun m -> m "missing executable metadata in block_put")
      in
      Async.return None
    | `Ok (Core.Either.Second executable) ->
      let f = function
        | Core.Either.First s -> s
        | Core.Either.Second _ ->
          (* Fail more gracefully *)
          failwith "metadata in the middle of put_block data"
      in
      Async.return @@ Some (Async.Pipe.map ~f pipe, executable) )
  | _ -> Async.return None
