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
"""

import json
import socket
import ssl
import threading
import time

import h2.config
import h2.connection
import h2.events
import h2.exceptions
from h2.errors import ErrorCodes

# How long to hold a goaway'd connection open so the client can read the frame.
_GOAWAY_GRACE_S = 2.0


class H2TestServer:
    """A minimal HTTP/2-over-TLS server usable as a context manager.

    Args:
        certfile: path to the server certificate (PEM).
        keyfile: path to the server private key (PEM).
        mode: one of "normal", "blackhole", "goaway_too_many_pings".
        report_path: optional path; on stop() a JSON report of observed
            connection events is written there.
    """

    def __init__(self, certfile, keyfile, mode="normal", report_path=None):
        if mode not in ("normal", "blackhole", "goaway_too_many_pings"):
            raise ValueError("unknown mode: {!r}".format(mode))
        self._certfile = certfile
        self._keyfile = keyfile
        self._mode = mode
        self._report_path = report_path
        self._listener = None
        self._thread = None
        self._stop = threading.Event()
        self.port = None

        # Event timestamps (seconds, time.monotonic()). Written only by the
        # accept-loop thread; read by stop() strictly after that thread joins,
        # so no lock is needed.
        self._events = {
            "connected": None,
            "remote_settings": None,
            "pings": [],
            "connection_lost": None,
        }

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
        config = h2.config.H2Configuration(client_side=False)
        h2conn = h2.connection.H2Connection(config=config)
        h2conn.initiate_connection()
        conn.sendall(h2conn.data_to_send())

        conn.settimeout(0.25)
        blackholed = False
        goaway_sent = False
        goaway_deadline = None

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
            for event in events:
                if isinstance(event, h2.events.RemoteSettingsChanged):
                    if self._events["remote_settings"] is None:
                        self._events["remote_settings"] = time.monotonic()
                    saw_remote_settings = True
                elif isinstance(event, h2.events.PingReceived):
                    self._events["pings"].append(time.monotonic())
                    if not goaway_sent:
                        saw_first_ping = True

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

            # Flush anything h2 auto-queued (SETTINGS acks, PING acks).
            try:
                conn.sendall(h2conn.data_to_send())
            except (ssl.SSLError, OSError):
                break

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
