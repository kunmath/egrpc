"""HTTP/2-over-TLS test server (hyper-h2) for egrpc M2 keepalive tests.

Dependencies: Python stdlib + the ``h2`` package only. The server speaks just
enough HTTP/2 to exercise a client's keepalive machinery: it completes the
connection preface / SETTINGS exchange, auto-acks client PINGs, and can be told
to misbehave in ways that a keepalive implementation must survive.

Usable as a context manager, mirroring ``tls_test_server.TlsTestServer``: bind
an ephemeral port, run the accept loop in a daemon thread, and handle one
connection at a time inline (the keepalive tests are sequential).

Modes:
  - "normal": full SETTINGS exchange, auto-ack every PING.
  - "blackhole": behave normally until the client's SETTINGS has been received
    and acked (the first flush after RemoteSettingsChanged), then go silent --
    never send or read again -- while holding the TCP connection open. Simulates
    a dead peer so the client's unacked-PING timeout must fire.
  - "goaway_too_many_pings": behave normally, but on the FIRST PING send GOAWAY
    with error code ENHANCE_YOUR_CALM (0xB) and debug data b"too_many_pings",
    then keep the socket open briefly so the client can read the GOAWAY.
  - "stall_streams": accept requests and never send response headers or data.
  - "send_compressed_then_ok": on stream 1 send response headers + a compressed
    gRPC DATA frame (flag=1, len=4, b"abcd") without END_STREAM; on stream > 1
    respond cleanly with an empty gRPC message and grpc-status: 0.
  - "send_oversize_then_ok": on stream 1 send response headers + a gRPC DATA
    frame declaring length 1000000 with 16 payload bytes and no END_STREAM; on
    stream > 1 respond cleanly with an empty gRPC message and grpc-status: 0.
  - "drop_on_request": immediately close the TLS socket after receiving the
    first RequestReceived event.
  - "stream_then_ok": on stream 1 send response headers + 1 empty gRPC message
    without END_STREAM; on stream > 1 respond cleanly with an empty gRPC
    message and grpc-status: 0.
  - "502_large_body_then_ok": on the 1st request stream send :status 502 +
    content-type text/html + 32,768 bytes of HTML body respecting flow control
    (waiting for WindowUpdated as needed) with END_STREAM; on the 2nd request
    stream send :status 200 + application/grpc + one gRPC-framed Feature
    ("ok-after-502") + trailers grpc-status: 0.
  - "burst_then_close": on stream 1 send :status 200 + application/grpc + 30
    gRPC-framed Feature messages ("msg-0".."msg-29") + trailers grpc-status: 0,
    flush, and immediately close the TLS/TCP socket.
  - "no_settings": complete TLS ALPN h2 handshake, send NO HTTP/2 SETTINGS
    frame, and hold the connection open reading/discarding incoming bytes.
  - "stream_frames": on each request stream send :status 200 + application/grpc
    + stream_frames_count gRPC-framed Feature messages of
    stream_frame_payload_bytes each (respecting HTTP/2 flow control), followed
    by trailers grpc-status: 0.

In "normal" mode, specific test paths also trigger targeted wire behaviors:
  - "/test.Stream/MalformedThenOk": on stream 1 send a raw HPACK-encoded
    HEADERS frame missing :status (triggering client nghttp2 RST_STREAM during
    FlushOutput); on stream >= 2 reply normally with b"second-ok" and
    grpc-status: 0.
  - "/test.Stream/ConnProtocolErrorThenPing": send initial :status 200 headers
    (no END_STREAM), then a raw invalid SETTINGS ACK frame with 6-byte payload
    on stream 0, then keep the socket open for 3 s sending raw PINGs every
    200 ms.
  - "/test.Stream/TrailersOnlyDenied": immediately on RequestReceived (before
    request DATA) send trailers-only HEADERS with grpc-status: 7,
    grpc-message: denied, end_stream=True.
"""

import json
import socket
import ssl
import struct
import threading
import time

import h2.config
import h2.connection
import h2.events
import h2.exceptions
import h2.settings
from h2.errors import ErrorCodes

# How long to hold a goaway'd connection open so the client can read the frame.
_GOAWAY_GRACE_S = 2.0


def _encode_varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _encode_feature(name, latitude=0, longitude=0):
    """Minimal protobuf wire encoding for routeguide.Feature."""
    out = bytearray()
    if name:
        raw = name.encode("utf-8")
        out.append(0x0A)  # field 1 (string name), wire type 2
        out.extend(_encode_varint(len(raw)))
        out.extend(raw)
    loc = bytearray()
    if latitude:
        loc.append(0x08)  # field 1 (int32 latitude), varint
        loc.extend(_encode_varint(latitude & 0xFFFFFFFF if latitude < 0 else latitude))
    if longitude:
        loc.append(0x10)  # field 2 (int32 longitude), varint
        loc.extend(
            _encode_varint(longitude & 0xFFFFFFFF if longitude < 0 else longitude)
        )
    if loc:
        out.append(0x12)  # field 2 (Point location), wire type 2
        out.extend(_encode_varint(len(loc)))
        out.extend(loc)
    return bytes(out)


def _grpc_frame(payload):
    return struct.pack("!BI", 0, len(payload)) + payload


def _raw_h2_frame(frame_type, flags, stream_id, payload):
    return (
        struct.pack("!I", len(payload))[1:]
        + struct.pack("!BBI", frame_type, flags, stream_id & 0x7FFFFFFF)
        + payload
    )


def _flush_pending_sends(h2conn, pending_sends):
    """Send flow-controlled DATA chunks and trailers for queued streams.

    Returns True if any completed stream requested closing the connection
    immediately after flushing.
    """
    close_after = False
    for sid in list(pending_sends.keys()):
        entry = pending_sends[sid]
        buf = entry["buf"]
        try:
            while entry["offset"] < len(buf):
                win = h2conn.local_flow_control_window(sid)
                chunk_size = min(
                    win, h2conn.max_outbound_frame_size, len(buf) - entry["offset"]
                )
                if chunk_size <= 0:
                    break
                end_now = (
                    entry["offset"] + chunk_size == len(buf)
                    and entry["end_stream_on_data"]
                )
                h2conn.send_data(
                    sid,
                    buf[entry["offset"] : entry["offset"] + chunk_size],
                    end_stream=end_now,
                )
                entry["offset"] += chunk_size

            if entry["offset"] == len(buf):
                if len(buf) == 0 and entry["end_stream_on_data"]:
                    h2conn.send_data(sid, b"", end_stream=True)
                if entry["trailers"] is not None:
                    h2conn.send_headers(sid, entry["trailers"], end_stream=True)
                if entry["close_conn_after"]:
                    close_after = True
                del pending_sends[sid]
        except (h2.exceptions.StreamClosedError, h2.exceptions.FlowControlError):
            pending_sends.pop(sid, None)
    return close_after


class H2TestServer:
    """A minimal HTTP/2-over-TLS server usable as a context manager.

    Args:
        certfile: path to the server certificate (PEM).
        keyfile: path to the server private key (PEM).
        mode: one of "normal", "blackhole", "goaway_too_many_pings".
        report_path: optional path; on stop() a JSON report of observed
            connection events is written there.
    """

    def __init__(
        self,
        certfile,
        keyfile,
        mode="normal",
        report_path=None,
        max_concurrent_streams=None,
        initial_window_size=None,
        stream_frames_count=40,
        stream_frame_payload_bytes=1024,
    ):
        if mode not in (
            "normal",
            "blackhole",
            "goaway_too_many_pings",
            "stall_streams",
            "send_compressed_then_ok",
            "send_oversize_then_ok",
            "drop_on_request",
            "stream_then_ok",
            "502_large_body_then_ok",
            "burst_then_close",
            "no_settings",
            "stream_frames",
        ):
            raise ValueError("unknown mode: {!r}".format(mode))
        self._certfile = certfile
        self._keyfile = keyfile
        self._mode = mode
        self._report_path = report_path
        self._max_concurrent_streams = max_concurrent_streams
        self._initial_window_size = initial_window_size
        self._stream_frames_count = stream_frames_count
        self._stream_frame_payload_bytes = stream_frame_payload_bytes
        self._listener = None
        self._thread = None
        self._stop = threading.Event()
        self.port = None
        self._lock = threading.Lock()
        self._reset_cond = threading.Condition(self._lock)
        self._req_cond = threading.Condition(self._lock)

        # Event timestamps / observations. Guarded by _lock for live reads.
        self._events = {
            "connected": None,
            "remote_settings": None,
            "pings": [],
            "requests": [],
            "resets": [],
            "connection_lost": None,
        }

    def wait_for_requests(self, count=1, timeout=5.0):
        deadline = time.monotonic() + timeout
        with self._req_cond:
            while len(self._events["requests"]) < count:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return list(self._events["requests"])
                self._req_cond.wait(rem)
            return list(self._events["requests"])

    def wait_for_resets(self, count=1, timeout=5.0):
        deadline = time.monotonic() + timeout
        with self._reset_cond:
            while len(self._events["resets"]) < count:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return list(self._events["resets"])
                self._reset_cond.wait(rem)
            return list(self._events["resets"])

    @property
    def requests(self):
        with self._lock:
            return list(self._events["requests"])

    @property
    def resets(self):
        with self._lock:
            return list(self._events["resets"])

    def _build_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile)
        ctx.set_alpn_protocols(["h2"])
        return ctx

    def start(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self._listener.settimeout(0.25)
        self.port = self._listener.getsockname()[1]

        ctx = self._build_context()
        self._thread = threading.Thread(
            target=self._accept_loop, args=(ctx,), daemon=True
        )
        self._thread.start()
        return self

    def _accept_loop(self, ctx):
        while not self._stop.is_set():
            try:
                raw, _ = self._listener.accept()
            except socket.timeout:
                continue
            except OSError:
                # Listener closed during stop().
                break
            try:
                self._handle(ctx, raw)
            except (ssl.SSLError, OSError):
                # Per-connection failures must not kill the accept loop.
                pass

    def _handle(self, ctx, raw):
        conn = None
        try:
            conn = ctx.wrap_socket(raw, server_side=True)
            self._events["connected"] = time.monotonic()
            self._serve(conn)
        except (ssl.SSLError, OSError):
            pass
        finally:
            self._events["connection_lost"] = time.monotonic()
            try:
                if conn is not None:
                    conn.close()
                else:
                    raw.close()
            except OSError:
                pass

    def _serve(self, conn):
        if self._mode == "no_settings":
            conn.settimeout(0.25)
            while not self._stop.is_set():
                try:
                    data = conn.recv(65535)
                except socket.timeout:
                    continue
                except (ssl.SSLError, OSError):
                    break
                if not data:
                    break
            return

        config = h2.config.H2Configuration(client_side=False)
        h2conn = h2.connection.H2Connection(config=config)
        h2conn.initiate_connection()
        settings_to_update = {}
        if self._max_concurrent_streams is not None:
            settings_to_update[h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS] = int(
                self._max_concurrent_streams
            )
        if self._initial_window_size is not None:
            settings_to_update[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE] = int(
                self._initial_window_size
            )
        if settings_to_update:
            h2conn.update_settings(settings_to_update)
        conn.sendall(h2conn.data_to_send())

        conn.settimeout(0.25)
        blackholed = False
        goaway_sent = False
        goaway_deadline = None
        stream_count = 0
        malformed_count = 0
        stream_paths = {}
        pending_sends = {}

        while not self._stop.is_set():
            # Once blackholed we go fully silent: no reads, no writes. Just hold
            # the socket open until the server is stopped.
            if blackholed:
                if self._stop.wait(0.25):
                    break
                continue

            # After sending GOAWAY, linger briefly so the client can read it.
            if goaway_sent:
                if goaway_deadline is not None and time.monotonic() >= goaway_deadline:
                    break
                if self._stop.wait(0.1):
                    break
                continue

            try:
                data = conn.recv(65535)
            except socket.timeout:
                continue
            except (ssl.SSLError, OSError):
                break
            if not data:
                break

            try:
                events = h2conn.receive_data(data)
            except h2.exceptions.ProtocolError:
                # Send whatever h2 queued (e.g. a GOAWAY) then bail.
                try:
                    conn.sendall(h2conn.data_to_send())
                except (ssl.SSLError, OSError):
                    pass
                break

            saw_remote_settings = False
            saw_first_ping = False
            new_streams = []
            ended_streams = []
            for event in events:
                if isinstance(event, h2.events.RemoteSettingsChanged):
                    if self._events["remote_settings"] is None:
                        self._events["remote_settings"] = time.monotonic()
                    saw_remote_settings = True
                elif isinstance(event, h2.events.PingReceived):
                    self._events["pings"].append(time.monotonic())
                    if not goaway_sent:
                        saw_first_ping = True
                elif isinstance(event, h2.events.RequestReceived):
                    req_path = ""
                    for k, v in event.headers:
                        k_str = k.decode("ascii") if isinstance(k, bytes) else k
                        if k_str == ":path":
                            req_path = v.decode("ascii") if isinstance(v, bytes) else v
                            break
                    stream_paths[event.stream_id] = req_path
                    with self._req_cond:
                        self._events["requests"].append(event.stream_id)
                        self._req_cond.notify_all()
                    new_streams.append(event.stream_id)
                elif isinstance(event, h2.events.StreamEnded):
                    ended_streams.append(event.stream_id)
                elif isinstance(event, h2.events.StreamReset):
                    pending_sends.pop(event.stream_id, None)
                    with self._reset_cond:
                        self._events["resets"].append(
                            (event.stream_id, int(event.error_code))
                        )
                        self._reset_cond.notify_all()

            # drop_on_request: immediately drop the TLS socket on the first
            # RequestReceived to exercise client TearDown with active calls.
            if self._mode == "drop_on_request" and new_streams:
                return

            if self._mode == "normal":
                for sid in new_streams:
                    path = stream_paths.get(sid, "")
                    if path == "/test.Stream/ConnProtocolErrorThenPing":
                        try:
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "200"),
                                    ("content-type", "application/grpc"),
                                ],
                            )
                            conn.sendall(h2conn.data_to_send())
                            # SETTINGS ACK with non-zero 6-byte payload on stream 0
                            # triggers connection-level FRAME_SIZE_ERROR / PROTOCOL_ERROR.
                            bad_settings = _raw_h2_frame(0x04, 0x01, 0, b"\x00" * 6)
                            conn.sendall(bad_settings)
                        except (h2.exceptions.StreamClosedError, ssl.SSLError, OSError):
                            return
                        deadline = time.monotonic() + 3.0
                        raw_ping = _raw_h2_frame(0x06, 0x00, 0, b"\x00" * 8)
                        while not self._stop.is_set() and time.monotonic() < deadline:
                            if self._stop.wait(0.2):
                                break
                            try:
                                conn.sendall(raw_ping)
                            except (ssl.SSLError, OSError):
                                break
                        return
                    elif path == "/test.Stream/TrailersOnlyDenied":
                        try:
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "200"),
                                    ("content-type", "application/grpc"),
                                    ("grpc-status", "7"),
                                    ("grpc-message", "denied"),
                                ],
                                end_stream=True,
                            )
                        except h2.exceptions.StreamClosedError:
                            pass

                for sid in ended_streams:
                    if stream_paths.get(sid, "") == "/test.Stream/MalformedThenOk":
                        malformed_count += 1
                        if malformed_count == 1:
                            try:
                                pending = h2conn.data_to_send()
                                if pending:
                                    conn.sendall(pending)
                                # Raw HPACK literal header without indexing:
                                # content-type: application/grpc (no :status).
                                raw_headers = _raw_h2_frame(
                                    0x01,
                                    0x05,
                                    sid,
                                    b"\x00\x0ccontent-type\x10application/grpc",
                                )
                                conn.sendall(raw_headers)
                            except (ssl.SSLError, OSError):
                                return
                        else:
                            try:
                                h2conn.send_headers(
                                    sid,
                                    [
                                        (":status", "200"),
                                        ("content-type", "application/grpc"),
                                    ],
                                )
                                pending_sends[sid] = {
                                    "buf": _grpc_frame(b"second-ok"),
                                    "offset": 0,
                                    "end_stream_on_data": False,
                                    "trailers": [("grpc-status", "0")],
                                    "close_conn_after": False,
                                }
                            except h2.exceptions.StreamClosedError:
                                pass

            if self._mode in (
                "send_compressed_then_ok",
                "send_oversize_then_ok",
                "stream_then_ok",
            ):
                for sid in new_streams:
                    try:
                        h2conn.send_headers(
                            sid,
                            [
                                (":status", "200"),
                                ("content-type", "application/grpc"),
                            ],
                        )
                        if sid == 1:
                            if self._mode == "send_compressed_then_ok":
                                frame = struct.pack("!BI", 1, 4) + b"abcd"
                            elif self._mode == "send_oversize_then_ok":
                                frame = struct.pack("!BI", 0, 1000000) + (
                                    b"x" * 16
                                )
                            else:  # stream_then_ok
                                frame = b"\x00\x00\x00\x00\x00"
                            h2conn.send_data(sid, frame, end_stream=False)
                        else:
                            h2conn.send_data(
                                sid, b"\x00\x00\x00\x00\x00", end_stream=False
                            )
                            h2conn.send_headers(
                                sid, [("grpc-status", "0")], end_stream=True
                            )
                    except h2.exceptions.StreamClosedError:
                        pass

            if self._mode == "502_large_body_then_ok":
                for sid in new_streams:
                    stream_count += 1
                    try:
                        if stream_count == 1:
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "502"),
                                    ("content-type", "text/html"),
                                ],
                            )
                            chunk = b"<html><body>502 Bad Gateway</body></html>\n"
                            body = (chunk * (32768 // len(chunk) + 1))[:32768]
                            pending_sends[sid] = {
                                "buf": body,
                                "offset": 0,
                                "end_stream_on_data": True,
                                "trailers": None,
                                "close_conn_after": False,
                            }
                        elif stream_count == 2:
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "200"),
                                    ("content-type", "application/grpc"),
                                ],
                            )
                            frame = _grpc_frame(_encode_feature("ok-after-502"))
                            pending_sends[sid] = {
                                "buf": frame,
                                "offset": 0,
                                "end_stream_on_data": False,
                                "trailers": [("grpc-status", "0")],
                                "close_conn_after": False,
                            }
                    except h2.exceptions.StreamClosedError:
                        pass

            if self._mode == "burst_then_close":
                for sid in ended_streams:
                    if sid == 1:
                        try:
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "200"),
                                    ("content-type", "application/grpc"),
                                ],
                            )
                            payload = b"".join(
                                _grpc_frame(
                                    _encode_feature("msg-{}".format(i))
                                )
                                for i in range(30)
                            )
                            pending_sends[sid] = {
                                "buf": payload,
                                "offset": 0,
                                "end_stream_on_data": False,
                                "trailers": [("grpc-status", "0")],
                                "close_conn_after": True,
                            }
                        except h2.exceptions.StreamClosedError:
                            pass

            if self._mode == "stream_frames":
                for sid in new_streams:
                    try:
                        h2conn.send_headers(
                            sid,
                            [
                                (":status", "200"),
                                ("content-type", "application/grpc"),
                            ],
                        )
                        name = "p" * self._stream_frame_payload_bytes
                        payload = b"".join(
                            _grpc_frame(
                                _encode_feature(
                                    name,
                                    latitude=i,
                                    longitude=self._stream_frame_payload_bytes,
                                )
                            )
                            for i in range(self._stream_frames_count)
                        )
                        pending_sends[sid] = {
                            "buf": payload,
                            "offset": 0,
                            "end_stream_on_data": False,
                            "trailers": [("grpc-status", "0")],
                            "close_conn_after": False,
                        }
                    except h2.exceptions.StreamClosedError:
                        pass

            # goaway_too_many_pings: on the first PING, queue a GOAWAY with
            # ENHANCE_YOUR_CALM and hold the socket open for a grace period.
            if (
                self._mode == "goaway_too_many_pings"
                and saw_first_ping
                and not goaway_sent
            ):
                h2conn.close_connection(
                    error_code=ErrorCodes.ENHANCE_YOUR_CALM,
                    additional_data=b"too_many_pings",
                    last_stream_id=0,
                )
                try:
                    conn.sendall(h2conn.data_to_send())
                except (ssl.SSLError, OSError):
                    break
                goaway_sent = True
                goaway_deadline = time.monotonic() + _GOAWAY_GRACE_S
                continue

            close_now = _flush_pending_sends(h2conn, pending_sends)

            # Flush anything h2 auto-queued (SETTINGS acks, PING acks, DATA).
            try:
                conn.sendall(h2conn.data_to_send())
            except (ssl.SSLError, OSError):
                break

            if close_now:
                return

            # blackhole: the flush above delivered the SETTINGS ack; now go
            # silent for the rest of the connection's life.
            if self._mode == "blackhole" and saw_remote_settings:
                blackholed = True

    def stop(self):
        self._stop.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)
            if self._thread.is_alive():
                # Serve thread wedged: reading events now would race the
                # writer. Skip the report; the test will fail loudly on it.
                return
        self._write_report()

    def _write_report(self):
        if not self._report_path:
            return
        report = {
            "connected": self._events["connected"],
            "remote_settings": self._events["remote_settings"],
            "pings": list(self._events["pings"]),
        }
        with open(self._report_path, "w") as fh:
            json.dump(report, fh)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False
