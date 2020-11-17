open Core

let block_get =
  let bin_query = [%bin_type_class: string]
  and bin_response = [%bin_type_class: (string * bool) option] in
  Async.Rpc.Rpc.create ~name:"get_block" ~version:0 ~bin_query ~bin_response

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
