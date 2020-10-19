open Stdune

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

let bad_request = Format.ksprintf (fun reason -> raise (Bad_request reason))

module Blocks = struct
  let check_range ranges address =
    if not (Config.ranges_include ranges address) then
      raise (Misdirected (address, ranges))

  let handle_errors hash path reqd f =
    Async.try_with ~extract_exn:true f >>= function
    | Result.Ok v -> Async.return v
    | Result.Error (Unix.Unix_error (Unix.ENOENT, _, _)) ->
      error reqd `No_content "block %S not found locally"
        (Digest.to_string hash)
    | Result.Error (Unix.Unix_error (Unix.EISDIR, _, _)) ->
      let* () =
        Logs_async.err (fun m ->
            m "block file is a directory: %S" (Path.to_string path))
      in
      error reqd `Internal_server_error "unable to read block %S"
        (Digest.to_string hash)
    | Result.Error (Unix.Unix_error (Unix.EACCES, _, _)) ->
      let* () =
        Logs_async.err (fun m ->
            m "permission denied on block: %S" (Path.to_string path))
      in
      error reqd `Internal_server_error "unable to read block %S"
        (Digest.to_string hash)
    | Result.Error exn ->
      let exn = Base.Exn.to_string exn in
      let* () = Logs_async.err (fun m -> m "unexpected exception: %s" exn) in
      error reqd `Internal_server_error "unexpected exception: %s" exn

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

  let read ?path { root; ranges } reqd hash f =
    let () = check_range ranges hash in
    let* path = file_path ?path root hash in
    let read () =
      let stats = Path.stat path in
      let headers =
        [ ( "X-executable"
          , if stats.st_perm land 0o100 <> 0 then
              "1"
            else
              "0" )
        ; ("X-dune-hash", Digest.to_string hash)
        ]
      in
      f path stats headers
    in
    handle_errors hash path reqd read

  let head t reqd hash =
    let f _ _ headers =
      let response =
        let headers =
          Httpaf.Headers.of_list @@ (("Content-length", "0") :: headers)
        in
        Httpaf.Response.create ~headers `OK
      in
      let* () = Logs_async.info (fun m -> m "> %s" (status_to_string `OK)) in
      Async.return @@ Httpaf.Reqd.respond_with_string reqd response ""
    in
    read t reqd hash f

  let get ?path t reqd hash =
    let f path stats headers =
      let f stats input =
        let input = Async.Reader.of_in_channel input Async.Fd.Kind.File
        and file_size = stats.Core.Unix.Native_file.st_size in
        let response =
          let headers =
            Httpaf.Headers.of_list
            @@ ("Content-length", string_of_int file_size)
               :: ("Content-type", "application/octet-stream")
               :: headers
          in
          Httpaf.Response.create ~headers `OK
        in
        let body = Httpaf.Reqd.respond_with_streaming reqd response in
        let size = 16 * 1024 in
        let buffer = Bytes.make size '\x00'
        and bigstring = Bigstringaf.create size in
        let rec loop () =
          Async.Reader.read input buffer >>= function
          | `Eof -> Async.return ()
          | `Ok read ->
            let () =
              Bigstringaf.unsafe_blit_from_bytes buffer ~src_off:0 bigstring
                ~dst_off:0 ~len:read
            in
            let* () =
              let () =
                Httpaf.Body.schedule_bigstring body
                  (Bigstringaf.sub bigstring ~off:0 ~len:read)
              and wakeup = Async.Ivar.create () in
              let () = Httpaf.Body.flush body (Async.Ivar.fill wakeup) in
              Async.Ivar.read wakeup
            in
            loop ()
        in
        let* () = loop () in
        let* () =
          Logs_async.info (fun m ->
              m "> %s [%i bytes]" (status_to_string `OK) file_size)
        in
        Async.return @@ Httpaf.Body.close_writer body
      in
      Core.In_channel.with_file ~binary:true (Path.to_string path) ~f:(f stats)
    in
    read ?path t reqd hash f

  type disk_buffer =
    { buffer : Bytes.t
    ; pos : int
    ; size : int
    }

  let put ?path { root; ranges } reqd hash executable =
    let () = check_range ranges hash in
    let* path = file_path ?path root hash in
    let put () =
      let f output =
        let write b =
          if b.pos = 0 then
            Async.return b
          else
            let rec loop from =
              if from = b.pos then
                Async.return { b with pos = 0 }
              else
                let* written =
                  let len = b.pos - from in
                  let () =
                    Async.Writer.write_bytes output ~pos:from ~len b.buffer
                  in
                  let+ () = Async.Writer.flushed output in
                  len
                in
                loop (from + written)
            in
            loop 0
        in
        let wait = Async.Ivar.create () in
        let request_body = Httpaf.Reqd.request_body reqd in
        let rec on_read b bs ~off ~len =
          let rec blit b blit_from blit_end =
            if blit_from = blit_end then
              Async.return
              @@ Httpaf.Body.schedule_read request_body ~on_eof:(on_eof b)
                   ~on_read:(on_read b)
            else
              let len = min (blit_end - blit_from) (b.size - b.pos) in
              let () =
                Bigstringaf.unsafe_blit_to_bytes bs ~src_off:blit_from b.buffer
                  ~dst_off:b.pos ~len
              in
              let b = { b with pos = b.pos + len }
              and blit_from = blit_from + len in
              if b.pos = b.size then
                let* b = write b in
                blit b blit_from blit_end
              else
                blit b blit_from blit_end
          in
          Async.don't_wait_for (blit b off (off + len))
        and on_eof b () =
          let finalize =
            let* _ = write b in
            let* () =
              Logs_async.info (fun m -> m "> %s" (status_to_string `Created))
            in
            let* () = response reqd `Created in
            Async.return @@ Async.Ivar.fill wait ()
          in
          Async.don't_wait_for finalize
        in
        let () =
          let size = 16 * 1024 * 1024 in
          let buffer = { buffer = Bytes.make size '\x00'; pos = 0; size } in
          Httpaf.Body.schedule_read request_body ~on_eof:(on_eof buffer)
            ~on_read:(on_read buffer)
        in
        Async.Ivar.read wait
      in
      let perm =
        if executable then
          0o700
        else
          0o600
      in
      Async.Writer.with_file ~perm ~f (Path.to_string path)
    in
    handle_errors hash path reqd put
end

let string_of_sockaddr = function
  | `Unix p -> p
  | `Inet (addr, port) ->
    Format.sprintf "%s:%i" (Unix.Inet_addr.to_string addr) port

let request_handler t sockaddr reqd =
  let { Httpaf.Request.meth; target; headers; _ } = Httpaf.Reqd.request reqd in
  let name =
    Fmt.str "%s: %s %s"
      (string_of_sockaddr sockaddr)
      (Httpaf.Method.to_string meth)
      target
  in
  let respond =
    let f () =
      let* () = Logs_async.info (fun m -> m "%s" name) in
      let meth =
        match meth with
        | `HEAD -> `HEAD
        | `GET -> `GET
        | `PUT -> `PUT
        | _ -> raise (Method_not_allowed meth)
      in
      match String.split ~on:'/' target with
      | [ ""; "blocks"; hash ] -> (
        let hash =
          match Digest.from_hex hash with
          | Some hash -> hash
          | None -> bad_request "invalid hash: %S" hash
        in
        match meth with
        | `HEAD -> Blocks.head t reqd hash
        | `GET -> Blocks.get t reqd hash
        | `PUT ->
          let executable =
            Httpaf.Headers.get headers "X-executable" = Some "1"
          in
          Blocks.put t reqd hash executable )
      | [ ""; "index"; path; hash ] -> (
        match Digest.from_hex hash with
        | None -> error reqd `Bad_request "invalid hash: %S" hash
        | Some hash -> (
          match meth with
          | `HEAD -> raise (Method_not_allowed meth)
          | `GET -> Blocks.get ~path t reqd hash
          | `PUT -> Blocks.put ~path t reqd hash false ) )
      | path ->
        error reqd `Bad_request "no such endpoint: %S"
          (String.concat ~sep:"/" path)
    in
    try
      Async.try_with ~name ~extract_exn:true f >>= function
      | Result.Ok () -> Async.return ()
      | Result.Error (Bad_request reason) -> error reqd `Bad_request "%s" reason
      | Result.Error (Method_not_allowed _) ->
        error reqd `Method_not_allowed "method not allowed"
      | Result.Error (Misdirected (addr, _)) ->
        error reqd (`Code 421) "address %s not in range" (Digest.to_string addr)
      | Result.Error exn ->
        error reqd `Internal_server_error "unexpected exception: %s"
          (Core.Exn.to_string exn)
    with e ->
      Caml.print_endline "FUCK";
      raise e
  in

  Async.don't_wait_for respond

let error_handler _ ?request:_ error start_response =
  let response_body = start_response Httpaf.Headers.empty in
  ( match error with
  | `Exn exn ->
    Httpaf.Body.write_string response_body (Printexc.to_string exn);
    Httpaf.Body.write_string response_body "\n"
  | #Httpaf.Status.standard as error ->
    Httpaf.Body.write_string response_body
      (Httpaf.Status.default_reason_phrase error) );
  Httpaf.Body.close_writer response_body

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
        let self = Uri.make ~host:(Unix.gethostname ()) ~port () in
        let f { Config.hostname; _ } =
          (* FIXME: also lookup ourselves by IP *)
          let hostname = Uri.with_scheme hostname None in
          Uri.equal hostname self
        in
        match List.find ~f c.nodes with
        | Some { space; _ } -> space
        | None ->
          User_error.raise
            [ Pp.textf "unable to find self (%s) in configuration file"
                (Uri.to_string self)
            ] )
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
    let t = { root; ranges } in
    let request_handler = request_handler t in
    let handler =
      Httpaf_async.Server.create_connection_handler ~request_handler
        ~error_handler
    in
    let* server =
      Async.Tcp.Server.create_sock ~on_handler_error:`Raise
        (Async.Tcp.Where_to_listen.bind_to
           (Async.Tcp.Bind_to_address.Address host)
           (Async.Tcp.Bind_to_port.On_port port))
        handler
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

(* try%lwt
 *   let* _server =
 *     Lwt_io.establish_server_with_client_socket listen_address handler
 *   in
 *   let rec loop () =
 *     let* () = Lwt_unix.sleep trim_period in
 *     let* () = trim t ~goal:trim_size in
 *     loop ()
 *   in
 *   loop ()
 * with Unix.Unix_error (Unix.EACCES, "bind", _) ->
 *   Logs_async.err (fun m -> m "unable to bind to port %i" port) *)
