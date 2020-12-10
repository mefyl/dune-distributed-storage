open Async
open Core

type block_pipe = string

type block =
  { contents : string Pipe.Reader.t
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
  Rpc.State_rpc.create ~name:"get_block" ~version:0 ~bin_query ~bin_state
    ~bin_update ~bin_error ()

let index_get =
  let bin_query = [%bin_type_class: string * string]
  and bin_response = [%bin_type_class: string list option] in
  Rpc.Rpc.create ~name:"get_index" ~version:0 ~bin_query ~bin_response

let index_put =
  let bin_query = [%bin_type_class: string * string * string list]
  and bin_response = [%bin_type_class: unit] in
  Rpc.Rpc.create ~name:"put_index" ~version:0 ~bin_query ~bin_response

let metadata_put =
  let bin_query = [%bin_type_class: string * string]
  and bin_response = [%bin_type_class: unit] in
  Rpc.Rpc.create ~name:"put_metadata" ~version:0 ~bin_query ~bin_response
