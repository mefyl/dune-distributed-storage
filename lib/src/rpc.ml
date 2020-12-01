open Async
open Core

let ( let* ) = ( >>= )

type block_pipe = string

type block =
  { contents : string Async.Pipe.Reader.t
  ; executable : bool
  }

type hash = string

type contents = string

type category = string

type executable = bool

let block_get =
  let bin_query = [%bin_type_class: string]
  and bin_state = [%bin_type_class: bool]
  and bin_update = [%bin_type_class: string]
  and bin_error = [%bin_type_class: unit] in
  Async.Rpc.State_rpc.create ~name:"get_block" ~version:0 ~bin_query ~bin_state
    ~bin_update ~bin_error ()

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
    | `Eof ->
      let* () =
        Logs_async.err (fun m -> m "missing executable metadata in block_put")
      in
      Async.return None
    | `Ok s ->
      let len = String.length s in
      let flags = s.[0] |> Char.to_int
      and contents =
        if String.length s = 1 then
          [ pipe ]
        else
          [ Async.Pipe.of_list [ String.sub ~pos:1 ~len:(len - 1) s ]; pipe ]
      in
      Async.return
      @@ Option.return
           { contents = Async.Pipe.concat contents
           ; executable = flags land 1 <> 0
           } )
  | _ -> Async.return None

let encode_block_get { contents; executable } =
  let flag =
    if executable then
      1
    else
      0
  in
  Async.Pipe.concat
    [ Async.Pipe.of_list [ String.make 1 (Char.of_int_exn flag) ]; contents ]
