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
    FlushOutput on libnghttp2 < 1.67, or GOAWAY(PROTOCOL_ERROR) on
    libnghttp2 >= 1.67 / CVE-2025-8671); on stream >= 2 reply normally with
    b"second-ok" and grpc-status: 0.
  - "/test.Stream/ConnProtocolErrorThenPing": send initial :status 200 headers
    (no END_STREAM), then a raw invalid SETTINGS ACK frame with 6-byte payload
    on stream 0, then keep the socket open for 3 s sending raw PINGs every
    200 ms.
  - "/test.Stream/TrailersOnlyDenied": immediately on RequestReceived (before
    request DATA) send trailers-only HEADERS with grpc-status: 7,
    grpc-message: denied, end_stream=True.

M6 multi-connection modes (resilience: reconnect, keepalive, GOAWAY, shutdown).
Every connection in these modes is served on its own thread, so a stalled or
blackholed connection N never blocks the accept of connection N+1. Connections
are numbered from 1 in accept order. "OK service" below means: when a request
stream ends, reply :status 200 + application/grpc + one gRPC-framed
routeguide.Feature named "ok-conn-<N>" + trailers grpc-status: 0, and let h2
auto-ack PINGs.
  - "multi_conn_ok": OK service on every connection.
  - "drop_first_then_ok": connection 1 closes the socket as soon as a request
    arrives (RequestReceived); connections 2+ get OK service.
  - "blackhole_after_first_rpc": connection 1 answers its first RPC with OK and
    then never sends another byte (PINGs are never acked). It keeps reading and
    discarding inbound bytes only to record the client's unanswered PINGs and
    the client's close. Connections 2+ get OK service.
  - "goaway_too_many_pings_then_ok": connection 1 gives OK service until the
    first client PING, then sends GOAWAY(ENHANCE_YOUR_CALM,
    last_stream_id=<highest request stream>, debug=b"too_many_pings") and
    discards input until the client closes (or a 2 s grace). Connections 2+ get
    OK service (PING timestamps recorded per connection).
  - "goaway_split_drain": connection 1 waits until two request streams have
    ended, then sends a raw GOAWAY(NO_ERROR, last_stream_id=<first stream>)
    (no RST_STREAM for the second stream), and drain_delay_ms later completes
    the first stream with OK. The second stream is never answered. Connections
    2+ get OK service.
  - "goaway_stall_drain": connection 1 sends a raw GOAWAY(NO_ERROR,
    last_stream_id=<first stream>) as soon as the first request arrives and
    never answers it. Connections 2+ get OK service.
  - "flap_ready_drop": every connection gets OK service but is closed hold_ms
    after the server's SETTINGS were sent.
  - "server_cancel": path ".../GetFeature" -> trailers-only grpc-status: 1
    (CANCELLED); path "/test.Stream/RstCancel" -> RST_STREAM(CANCEL);
    ".../ListFeatures" and "/test.Stream/Stall" -> initial headers, then stall.
  - "shutdown_blackhole": on the first request send initial :status 200
    headers, then never read or write again (dead peer) until stop().
  - "shutdown_blackhole_large_window": as "shutdown_blackhole", but the
    server's initial SETTINGS advertise INITIAL_WINDOW_SIZE = 4 MiB and are
    followed by a connection-level WINDOW_UPDATE of +4 MiB, so the client
    holds megabytes of open stream + connection send window when the peer
    goes dead (pending output is then bounded by socket buffers, not by
    HTTP/2 flow control).
  - "stall_after_headers": when a request stream ends (StreamEnded, after the
    request was recorded in ``requests``), send initial :status 200 +
    content-type application/grpc HEADERS (no END_STREAM), flush, and leave
    the stream open forever (no DATA, no trailers). Keeps reading (PINGs are
    auto-acked). Honors ``max_concurrent_streams``. Unlike the legacy
    "stall_streams", a client observing initial metadata knows the server
    has received the whole request.

Per-connection records (``connections`` / ``wait_for_connections``) carry:
index, accepted, tls_done, settings_sent, remote_settings, closed (monotonic
seconds), pings (timestamps of client PINGs, incl. unanswered ones in
blackhole), requests [(stream_id, path)], resets [(stream_id, code)],
goaways [(last_stream_id, code)] received from the client, goaway_sent.
Legacy single-connection modes record only index/accepted/tls_done/closed.
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

# M6 modes served thread-per-connection by _serve_multi().
_MULTI_CONN_MODES = (
    "multi_conn_ok",
    "drop_first_then_ok",
    "blackhole_after_first_rpc",
    "goaway_too_many_pings_then_ok",
    "goaway_split_drain",
    "goaway_stall_drain",
    "flap_ready_drop",
    "server_cancel",
    "shutdown_blackhole",
    "shutdown_blackhole_large_window",
    "stall_after_headers",
)

# "shutdown_blackhole_large_window": advertised stream (SETTINGS) and
# connection (WINDOW_UPDATE increment) flow-control window.
_LARGE_WINDOW_BYTES = 4 * 1024 * 1024

_H2_FRAME_GOAWAY = 0x7
_H2_FRAME_PING = 0x6


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
        port=0,
        hold_ms=0,
        drain_delay_ms=150,
    ):
        if mode not in _MULTI_CONN_MODES and mode not in (
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
        self._bind_port = int(port)
        self._hold_s = hold_ms / 1000.0
        self._drain_delay_s = drain_delay_ms / 1000.0
        self._listener = None
        self._thread = None
        self._stop = threading.Event()
        self.port = None
        self._lock = threading.Lock()
        self._reset_cond = threading.Condition(self._lock)
        self._req_cond = threading.Condition(self._lock)
        self._goaway_cond = threading.Condition(self._lock)
        self._conn_cond = threading.Condition(self._lock)
        # Per-connection records (see module docstring); guarded by _lock.
        self._conns = []
        self._conn_threads = []

        # Event timestamps / observations. Guarded by _lock for live reads.
        self._events = {
            "connected": None,
            "remote_settings": None,
            "pings": [],
            "requests": [],
            "resets": [],
            "goaways": [],
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

    def wait_for_goaways(self, count=1, timeout=5.0):
        deadline = time.monotonic() + timeout
        with self._goaway_cond:
            while len(self._events["goaways"]) < count:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return list(self._events["goaways"])
                self._goaway_cond.wait(rem)
            return list(self._events["goaways"])

    @property
    def requests(self):
        with self._lock:
            return list(self._events["requests"])

    @property
    def resets(self):
        with self._lock:
            return list(self._events["resets"])

    @property
    def goaways(self):
        with self._lock:
            return list(self._events["goaways"])

    @staticmethod
    def _copy_conn(cs):
        out = dict(cs)
        for key in ("pings", "requests", "resets", "goaways"):
            out[key] = list(cs[key])
        return out

    @property
    def connections(self):
        """Snapshot (deep-copied) of the per-connection records."""
        with self._lock:
            return [self._copy_conn(cs) for cs in self._conns]

    def wait_for_connections(self, count=1, timeout=5.0):
        """Wait until at least `count` connections were accepted; snapshot."""
        deadline = time.monotonic() + timeout
        with self._conn_cond:
            while len(self._conns) < count:
                rem = deadline - time.monotonic()
                if rem <= 0:
                    break
                self._conn_cond.wait(rem)
            return [self._copy_conn(cs) for cs in self._conns]

    def wait_for_conn_closed(self, index, timeout=5.0):
        """Wait until connection `index` (1-based) is closed; snapshot or None."""
        deadline = time.monotonic() + timeout
        with self._conn_cond:
            while True:
                if len(self._conns) >= index and self._conns[index - 1]["closed"] is not None:
                    return self._copy_conn(self._conns[index - 1])
                rem = deadline - time.monotonic()
                if rem <= 0:
                    return None
                self._conn_cond.wait(rem)

    def _new_conn_record(self):
        with self._conn_cond:
            cs = {
                "index": len(self._conns) + 1,
                "accepted": time.monotonic(),
                "tls_done": None,
                "settings_sent": None,
                "remote_settings": None,
                "closed": None,
                "pings": [],
                "requests": [],
                "resets": [],
                "goaways": [],
                "goaway_sent": None,
            }
            self._conns.append(cs)
            self._conn_cond.notify_all()
            return cs

    def _set_conn(self, cs, key, value):
        with self._conn_cond:
            cs[key] = value
            self._conn_cond.notify_all()

    def _build_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile)
        ctx.set_alpn_protocols(["h2"])
        return ctx

    def start(self):
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", self._bind_port))
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
            if self._mode in _MULTI_CONN_MODES:
                cs = self._new_conn_record()
                t = threading.Thread(
                    target=self._handle_multi, args=(ctx, raw, cs), daemon=True
                )
                self._conn_threads.append(t)
                t.start()
                continue
            try:
                self._handle(ctx, raw)
            except (ssl.SSLError, OSError):
                # Per-connection failures must not kill the accept loop.
                pass

    def _handle(self, ctx, raw):
        conn = None
        cs = self._new_conn_record()
        try:
            conn = ctx.wrap_socket(raw, server_side=True)
            self._events["connected"] = time.monotonic()
            self._set_conn(cs, "tls_done", self._events["connected"])
            self._serve(conn)
        except (ssl.SSLError, OSError):
            pass
        finally:
            self._events["connection_lost"] = time.monotonic()
            self._set_conn(cs, "closed", self._events["connection_lost"])
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
                elif isinstance(event, h2.events.ConnectionTerminated):
                    with self._goaway_cond:
                        self._events["goaways"].append(
                            (event.last_stream_id, int(event.error_code))
                        )
                        self._goaway_cond.notify_all()

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

    # --- M6 multi-connection modes ------------------------------------------

    def _conn_behavior(self, index):
        mode = self._mode
        if mode == "multi_conn_ok":
            return "ok"
        if mode == "flap_ready_drop":
            return "flap"
        if mode == "server_cancel":
            return "server_cancel"
        if mode in ("shutdown_blackhole", "shutdown_blackhole_large_window"):
            return "headers_then_blackhole"
        if mode == "stall_after_headers":
            return "headers_on_end_then_stall"
        if index > 1:
            return "ok"
        return {
            "drop_first_then_ok": "drop_on_request",
            "blackhole_after_first_rpc": "ok_then_blackhole",
            "goaway_too_many_pings_then_ok": "goaway_on_ping",
            "goaway_split_drain": "split_drain",
            "goaway_stall_drain": "stall_drain",
        }[mode]

    def _handle_multi(self, ctx, raw, cs):
        conn = None
        try:
            raw.settimeout(5.0)  # bound a stuck TLS handshake
            conn = ctx.wrap_socket(raw, server_side=True)
            self._set_conn(cs, "tls_done", time.monotonic())
            self._serve_multi(conn, cs)
        except (ssl.SSLError, OSError, h2.exceptions.H2Error):
            pass
        finally:
            self._set_conn(cs, "closed", time.monotonic())
            try:
                if conn is not None:
                    conn.close()
                else:
                    raw.close()
            except OSError:
                pass

    @staticmethod
    def _send_ok(h2conn, sid, conn_index):
        try:
            h2conn.send_headers(
                sid, [(":status", "200"), ("content-type", "application/grpc")]
            )
            h2conn.send_data(
                sid, _grpc_frame(_encode_feature("ok-conn-{}".format(conn_index)))
            )
            h2conn.send_headers(sid, [("grpc-status", "0")], end_stream=True)
        except h2.exceptions.H2Error:
            pass

    def _record_ping(self, cs):
        t = time.monotonic()
        with self._lock:
            cs["pings"].append(t)
            self._events["pings"].append(t)

    def _serve_multi(self, conn, cs):
        idx = cs["index"]
        behavior = self._conn_behavior(idx)

        h2conn = h2.connection.H2Connection(
            config=h2.config.H2Configuration(client_side=False)
        )
        h2conn.initiate_connection()
        settings_to_update = {}
        if self._max_concurrent_streams is not None:
            settings_to_update[h2.settings.SettingCodes.MAX_CONCURRENT_STREAMS] = int(
                self._max_concurrent_streams
            )
        large_window = self._mode == "shutdown_blackhole_large_window"
        if self._initial_window_size is not None:
            settings_to_update[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE] = int(
                self._initial_window_size
            )
        elif large_window:
            settings_to_update[h2.settings.SettingCodes.INITIAL_WINDOW_SIZE] = (
                _LARGE_WINDOW_BYTES
            )
        if settings_to_update:
            h2conn.update_settings(settings_to_update)
        if large_window:
            # Connection-level WINDOW_UPDATE (stream 0), sent with the initial
            # SETTINGS -- i.e. well before the first stream's headers and the
            # blackhole -- so the client's connection send window is ~4 MiB.
            h2conn.increment_flow_control_window(_LARGE_WINDOW_BYTES)
        conn.sendall(h2conn.data_to_send())
        settings_sent = time.monotonic()
        self._set_conn(cs, "settings_sent", settings_sent)

        def flush():
            out = h2conn.data_to_send()
            if out:
                conn.sendall(out)

        def send_raw_goaway(last_stream_id, error_code, debug=b""):
            flush()  # keep h2's queued frames ahead of the raw frame
            payload = struct.pack("!II", last_stream_id & 0x7FFFFFFF, error_code) + debug
            conn.sendall(_raw_h2_frame(_H2_FRAME_GOAWAY, 0, 0, payload))
            self._set_conn(cs, "goaway_sent", time.monotonic())

        close_at = settings_sent + self._hold_s if behavior == "flap" else None
        # phase: "h2" (normal framing), "silent_read" (never write; parse raw
        # frames only to record PINGs), "discard" (read and drop until EOF or
        # discard_deadline), "dead" (no reads, no writes).
        phase = "h2"
        discard_deadline = None
        timers = []  # [(due_monotonic, fn)]
        stream_paths = {}
        request_sids = []
        ended_order = []
        answered = 0
        goaway_sent = False
        raw_buf = bytearray()

        while not self._stop.is_set():
            now = time.monotonic()
            if close_at is not None and now >= close_at:
                return
            if phase == "dead":
                self._stop.wait(0.1)
                continue
            if phase == "discard" and now >= discard_deadline:
                return

            if timers:
                due = [t for t in timers if t[0] <= now]
                if due:
                    timers = [t for t in timers if t[0] > now]
                    for _, fn in due:
                        fn()
                    flush()

            wake = [0.05]
            if close_at is not None:
                wake.append(close_at - now)
            if timers:
                wake.append(min(t[0] for t in timers) - now)
            conn.settimeout(max(0.005, min(wake)))
            try:
                data = conn.recv(65535)
            except socket.timeout:
                continue
            except (ssl.SSLError, OSError):
                return
            if not data:
                return

            if phase == "discard":
                continue
            if phase == "silent_read":
                raw_buf.extend(data)
                while len(raw_buf) >= 9:
                    length = int.from_bytes(raw_buf[0:3], "big")
                    if len(raw_buf) < 9 + length:
                        break
                    if raw_buf[3] == _H2_FRAME_PING and not (raw_buf[4] & 0x1):
                        self._record_ping(cs)
                    del raw_buf[: 9 + length]
                continue

            try:
                events = h2conn.receive_data(data)
            except h2.exceptions.ProtocolError:
                try:
                    flush()
                except (ssl.SSLError, OSError):
                    pass
                return

            new_streams = []
            ended_streams = []
            saw_ping = False
            client_goaway = False
            for event in events:
                if isinstance(event, h2.events.RemoteSettingsChanged):
                    if cs["remote_settings"] is None:
                        self._set_conn(cs, "remote_settings", time.monotonic())
                elif isinstance(event, h2.events.PingReceived):
                    self._record_ping(cs)
                    saw_ping = True
                elif isinstance(event, h2.events.RequestReceived):
                    req_path = ""
                    for k, v in event.headers:
                        k_str = k.decode("ascii") if isinstance(k, bytes) else k
                        if k_str == ":path":
                            req_path = v.decode("ascii") if isinstance(v, bytes) else v
                            break
                    stream_paths[event.stream_id] = req_path
                    request_sids.append(event.stream_id)
                    with self._req_cond:
                        cs["requests"].append((event.stream_id, req_path))
                        self._events["requests"].append(event.stream_id)
                        self._req_cond.notify_all()
                    new_streams.append(event.stream_id)
                elif isinstance(event, h2.events.StreamEnded):
                    ended_streams.append(event.stream_id)
                elif isinstance(event, h2.events.StreamReset):
                    with self._reset_cond:
                        entry = (event.stream_id, int(event.error_code))
                        cs["resets"].append(entry)
                        self._events["resets"].append(entry)
                        self._reset_cond.notify_all()
                elif isinstance(event, h2.events.ConnectionTerminated):
                    with self._goaway_cond:
                        entry = (event.last_stream_id, int(event.error_code))
                        cs["goaways"].append(entry)
                        self._events["goaways"].append(entry)
                        self._goaway_cond.notify_all()
                    client_goaway = True

            if behavior == "drop_on_request" and new_streams:
                return

            if behavior in ("ok", "flap"):
                for sid in ended_streams:
                    self._send_ok(h2conn, sid, idx)

            elif behavior == "ok_then_blackhole":
                for sid in ended_streams:
                    if answered == 0:
                        self._send_ok(h2conn, sid, idx)
                        answered += 1

            elif behavior == "goaway_on_ping":
                if not goaway_sent:
                    for sid in ended_streams:
                        self._send_ok(h2conn, sid, idx)
                    if saw_ping:
                        h2conn.close_connection(
                            error_code=ErrorCodes.ENHANCE_YOUR_CALM,
                            additional_data=b"too_many_pings",
                            last_stream_id=max(request_sids) if request_sids else 0,
                        )
                        flush()
                        self._set_conn(cs, "goaway_sent", time.monotonic())
                        goaway_sent = True
                        phase = "discard"
                        discard_deadline = time.monotonic() + _GOAWAY_GRACE_S
                        continue

            elif behavior == "split_drain":
                if not goaway_sent:
                    ended_order.extend(ended_streams)
                    if len(ended_order) >= 2:
                        first = min(ended_order)
                        send_raw_goaway(first, 0)
                        goaway_sent = True
                        timers.append(
                            (
                                time.monotonic() + self._drain_delay_s,
                                lambda sid=first: self._send_ok(h2conn, sid, idx),
                            )
                        )

            elif behavior == "stall_drain":
                if not goaway_sent and ended_streams:
                    send_raw_goaway(min(ended_streams), 0)
                    goaway_sent = True

            elif behavior == "server_cancel":
                for sid in new_streams:
                    path = stream_paths.get(sid, "")
                    if path.endswith("/ListFeatures") or path.endswith("/Stall"):
                        try:
                            h2conn.send_headers(
                                sid,
                                [(":status", "200"), ("content-type", "application/grpc")],
                            )
                        except h2.exceptions.H2Error:
                            pass
                for sid in ended_streams:
                    path = stream_paths.get(sid, "")
                    try:
                        if path.endswith("/RstCancel"):
                            h2conn.reset_stream(sid, error_code=ErrorCodes.CANCEL)
                        elif path.endswith("/GetFeature"):
                            h2conn.send_headers(
                                sid,
                                [
                                    (":status", "200"),
                                    ("content-type", "application/grpc"),
                                    ("grpc-status", "1"),
                                    ("grpc-message", "server-cancelled"),
                                ],
                                end_stream=True,
                            )
                    except h2.exceptions.H2Error:
                        pass

            elif behavior == "headers_on_end_then_stall":
                # The request is fully received (and already recorded above):
                # answer with initial headers only and hold the stream open.
                for sid in ended_streams:
                    try:
                        h2conn.send_headers(
                            sid,
                            [(":status", "200"), ("content-type", "application/grpc")],
                            end_stream=False,
                        )
                    except h2.exceptions.H2Error:
                        pass

            elif behavior == "headers_then_blackhole":
                for sid in new_streams:
                    try:
                        h2conn.send_headers(
                            sid,
                            [(":status", "200"), ("content-type", "application/grpc")],
                        )
                    except h2.exceptions.H2Error:
                        pass

            try:
                flush()
            except h2.exceptions.H2Error:
                return

            if client_goaway:
                # Client is closing (e.g. graceful Shutdown): just drain input.
                phase = "discard"
                discard_deadline = time.monotonic() + _GOAWAY_GRACE_S
            elif behavior == "ok_then_blackhole" and answered > 0:
                phase = "silent_read"
            elif behavior == "headers_then_blackhole" and new_streams:
                phase = "dead"

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
        for t in self._conn_threads:
            t.join(timeout=5.0)
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
