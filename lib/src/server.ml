open Stdune
module Async_rpc = Async.Rpc.Rpc

let ( >>= ) = Async.( >>= )

let ( >>| ) = Async.( >>| )

let ( let* ) = ( >>= )

let ( let+ ) = ( >>| )

module Unix_caml = Unix
module Unix = Async.Unix

type t =
  { root : Path.t
  ; ranges : Config.range list
  }

exception Misdirected of Digest.t * Config.range list

exception Bad_request of string

exception Method_not_allowed of Httpaf.Method.t

let status_to_string = function
  | `Code i -> Format.sprintf "%i" i
  | #Httpaf.Status.standard as status ->
    Format.sprintf "%s %s"
      (Httpaf.Status.to_string status)
      (Httpaf.Status.default_reason_phrase status)

let response reqd status =
  let headers = Httpaf.Headers.of_list [ ("Content-length", "0") ] in
  let response = Httpaf.Response.create ~headers status in
  Async.return @@ Httpaf.Reqd.respond_with_string reqd response ""

let error reqd status =
  let error reqd status reason =
    let* () =
      Logs_async.info (fun m -> m "> %s: %s" (status_to_string status) reason)
    in
    if status = `No_content then
      let response =
        Httpaf.Response.create ~headers:Httpaf.Headers.empty status
      in
      Async.return @@ Httpaf.Reqd.respond_with_string reqd response ""
    else
      let contents = Format.sprintf "{\"reason\": %S}" reason in
      let headers =
        Httpaf.Headers.of_list
          [ ("Content-type", "application/json")
          ; ("Content-length", Int.to_string (String.length contents))
          ]
      in
      let response = Httpaf.Response.create ~headers status in
      Async.return @@ Httpaf.Reqd.respond_with_string reqd response contents
  in
  Format.ksprintf (error reqd status)

module Blocks = struct
  let check_range ranges address =
    if not (Config.ranges_include ranges address) then
      raise (Misdirected (address, ranges))

  let handle_error hash path = function
    | Unix.Unix_error (Unix.EISDIR, _, _) ->
      let* () =
        Logs_async.err (fun m ->
            m "block file is a directory: %S" (Path.to_string path))
      in
      failwith (Fmt.str "unable to read block %S" (Digest.to_string hash))
    | Unix.Unix_error (Unix.EACCES, _, _) ->
      let* () =
        Logs_async.err (fun m ->
            m "permission denied on block: %S" (Path.to_string path))
      in
      failwith (Fmt.str "unable to read block %S" (Digest.to_string hash))
    | exn ->
      let exn = Base.Exn.to_string exn in
      let* () = Logs_async.err (fun m -> m "unexpected exception: %s" exn) in
      failwith (Fmt.str "unexpected exception: %s" exn)

  let file_path ?path root hash =
    let+ root =
      match path with
      | Some path ->
        let path = Path.relative root path in
        let+ () =
          Async.try_with ~extract_exn:true (fun () ->
              Unix.mkdir ~p:() ~perm:0o700 (Path.to_string path))
          >>| function
          | Result.Ok ()
          | Result.Error (Unix.Unix_error (Unix.EEXIST, _, _)) ->
            ()
          | Result.Error exn -> raise exn
        in
        path
      | None -> Async.return root
    in
    Path.relative root (Digest.to_string hash)

  let head t hash =
    let* path = file_path t.root hash in
    Async.return @@ Path.exists path

  let get ?path ~f { ranges; root } hash =
    let f path stats executable =
      let* () =
        let file_size = stats.Core.Unix.Native_file.st_size in
        Logs_async.info (fun m -> m "> artifact [%i bytes]" file_size)
      in
      Async.return @@ (f @@ Path.to_string path, executable)
    in
    let () = check_range ranges hash in
    let* path = file_path ?path root hash in
    let read () =
      let stats = Path.stat path in
      let executable = stats.st_perm land 0o100 <> 0 in
      f path stats executable
    in
    Async.try_with ~extract_exn:true read >>= function
    | Result.Ok v -> Async.return (Some v)
    | Result.Error (Unix.Unix_error (Unix.ENOENT, _, _)) -> Async.return None
    | Result.Error e -> handle_error hash path e

  type disk_buffer =
    { buffer : Bytes.t
    ; pos : int
    ; size : int
    }

  let put ?path ~f { root; ranges } hash executable data =
    let () = check_range ranges hash in
    let* path = file_path ?path root hash in
    let put () =
      let perm =
        if executable then
          0o700
        else
          0o600
      and f c = f c data in
      Async.return @@ Core.Out_channel.with_file ~perm (Path.to_string path) ~f
    in
    Async.try_with ~extract_exn:true put >>= function
    | Result.Ok v -> Async.return v
    | Result.Error e -> handle_error hash path e
end

let string_of_sockaddr = function
  | `Unix p -> p
  | `Inet (addr, port) ->
    Format.sprintf "%s:%i" (Unix.Inet_addr.to_string addr) port

let trim { root; ranges } ~goal =
  let files =
    match Path.readdir_unsorted root with
    | Result.Ok l -> l
    | Result.Error e ->
      User_error.raise
        [ Pp.textf "unable to read storage root %s: %s" (Path.to_string root)
            (Unix_caml.error_message e)
        ]
  in
  let f path =
    let* () = Async.return () (* yield *) in
    let path = Path.relative root path in
    let* stats =
      try
        let stats = Path.stat path in
        Async.return @@ Option.some_if (stats.st_kind = Unix_caml.S_REG) stats
      with Unix.Unix_error (e, _, _) ->
        let* () =
          Logs_async.warn (fun m ->
              m "unable to stat %s: %s" (Path.to_string path)
                (Unix_caml.error_message e))
        in
        Async.return None
    in
    Async.return
    @@ Option.map
         ~f:(fun stats -> (path, Int64.of_int stats.st_size, stats.st_mtime))
         stats
  and compare (_, _, t1) (_, _, t2) = Ordering.of_int (Stdlib.compare t1 t2) in
  let* files = Async.Deferred.List.filter_map ~f files in
  let* size =
    let f size (path, bytes, _) =
      match
        let ( let* ) v f = Option.bind ~f v in
        let basename = Path.basename path in
        let* hash = Digest.from_hex basename in
        Some (Config.ranges_include ranges hash)
      with
      | Some true -> Async.return @@ Int64.add size bytes
      | Some false ->
        let* () =
          Logs_async.warn (fun m ->
              m "remove out-of-range file: %s" (Path.to_string path))
        in
        let () = Path.unlink path in
        Async.return size
      | None ->
        let* () =
          Logs_async.warn (fun m ->
              m "remove unrecognized file: %s" (Path.to_string path))
        in
        Async.return size
    in
    Async.Deferred.List.fold ~f ~init:0L files
  in
  if size > goal then
    let goal = Int64.sub size goal in
    let* () = Logs_async.info (fun m -> m "trimming %Li bytes" goal) in
    let files = List.sort ~compare files
    and delete (trimmed : int64) (path, bytes, _) =
      if trimmed >= goal then
        trimmed
      else
        let () = Path.unlink path in
        Int64.add trimmed bytes
    in
    let trimmed = List.fold_left ~init:0L ~f:delete files in
    Logs_async.info (fun m -> m "trimming freed %Li bytes" trimmed)
  else
    Logs_async.debug (fun m -> m "skip trimming")

let run config host port root trim_period trim_size =
  let ranges =
    match config with
    | None -> [ Config.range_total ]
    | Some path -> (
      match Config.of_file path with
      | Result.Ok c -> (
        let self = Format.sprintf "%s:%d" (Unix.gethostname ()) port in
        let f { Config.hostname; _ } =
          (* FIXME: also lookup ourselves by IP *)
          String.equal hostname self
        in
        match List.find ~f c.nodes with
        | Some { space; _ } -> space
        | None ->
          User_error.raise
            [ Pp.textf "unable to find self (%s) in configuration file" self ] )
      | Result.Error e ->
        User_error.raise
          [ Pp.textf "configuration error in %s: %s" (Path.to_string path) e ] )
  in
  let main () =
    let trim_period = float_of_int trim_period in
    let* () =
      ( Async.try_with ~extract_exn:true @@ fun () ->
        Unix.mkdir ~p:() ~perm:0o700 (Path.to_string root) )
      >>| function
      | Result.Ok ()
      | Result.Error (Unix.Unix_error (Unix.EEXIST, _, _)) ->
        ()
      | Result.Error exn -> raise exn
    in
    let implementations =
      let hash h =
        match Digest.from_hex h with
        | None -> failwith "invalid hash"
        | Some h -> h
      in
      let block_get =
        let f t h = Blocks.get ~f:Core.In_channel.read_all t (hash h) in
        Async_rpc.implement Rpc.block_get f
      and block_has =
        let f t h = Blocks.head t (hash h) in
        Async_rpc.implement Rpc.block_has f
      and block_put =
        let f t (h, executable, contents) =
          Blocks.put ~f:Core.Out_channel.output_string t (hash h) executable
            contents
        in
        Async_rpc.implement Rpc.block_put f
      and index_get =
        let f t (path, h) =
          Blocks.get ~path ~f:Core.In_channel.read_lines t (hash h) >>| function
          | Some (v, _) -> Some v
          | None -> None
        in
        Async_rpc.implement Rpc.index_get f
      and index_put =
        let f t (path, h, contents) =
          Blocks.put ~f:Core.Out_channel.output_lines ~path t (hash h) false
            contents
        in
        Async_rpc.implement Rpc.index_put f
      in
      match
        Async.Rpc.Implementations.create
          ~implementations:
            [ block_get; block_has; block_put; index_get; index_put ]
          ~on_unknown_rpc:`Raise
      with
      | Result.Ok impls -> impls
      | Result.Error (`Duplicate_implementations _) -> failwith "duplicate RPC"
    in
    let t = { root; ranges } in
    let* server =
      Async.Tcp.Server.create
        ~on_handler_error:
          (`Call
            (fun _ exn -> Async.Log.Global.sexp [%sexp (exn : Base.Exn.t)]))
        (Async.Tcp.Where_to_listen.bind_to
           (Async.Tcp.Bind_to_address.Address host)
           (Async.Tcp.Bind_to_port.On_port port))
        (fun _addr r w ->
          Async.Rpc.Connection.server_with_close r w
            ~connection_state:(fun _ -> t)
            ~on_handshake_error:
              (`Call
                (fun exn ->
                  Async.Log.Global.sexp [%sexp (exn : Base.Exn.t)];
                  Async.return ()))
            ~implementations)
    in
    let stop = Async.Tcp.Server.close_finished server in
    let () =
      Async.Clock.every' ~stop (Core.Time.Span.of_sec trim_period) (fun () ->
          trim t ~goal:trim_size)
    and () =
      let f _ = Async.don't_wait_for @@ Async.Tcp.Server.close server in
      Async.Signal.handle ~f [ Async.Signal.int; Async.Signal.quit ]
    in
    stop
  in
  Async.Thread_safe.block_on_async main
