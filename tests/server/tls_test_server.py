"""Python-stdlib-only threaded TLS test server for egrpc M1 integration tests.

No third-party dependencies: only ssl, socket, threading. The server accepts TLS
connections, performs the server-side handshake (optionally advertising ALPN),
and holds each successful connection open until the peer disconnects or the
server is stopped. Individual handshake failures (for example, an ALPN-refusal
alert or a client aborting mid-handshake) are expected test outcomes and are
swallowed without killing the accept loop.
"""

import socket
import ssl
import threading


class TlsTestServer:
    """A minimal TLS server usable as a context manager.

    Args:
        certfile: path to the server certificate (PEM).
        keyfile: path to the server private key (PEM).
        alpn_protocols: list of ALPN protocol strings to advertise, or None to
            leave ALPN entirely unconfigured on the SSL context.
    """

    def __init__(self, certfile, keyfile, alpn_protocols):
        self._certfile = certfile
        self._keyfile = keyfile
        self._alpn_protocols = alpn_protocols
        self._listener = None
        self._thread = None
        self._stop = threading.Event()
        self.port = None

    def _build_context(self):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=self._certfile, keyfile=self._keyfile)
        if self._alpn_protocols is not None:
            ctx.set_alpn_protocols(self._alpn_protocols)
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
            self._handle(ctx, raw)

    def _handle(self, ctx, raw):
        conn = None
        try:
            conn = ctx.wrap_socket(raw, server_side=True)
            # Handshake succeeded: hold the connection open until the peer goes
            # away or the server is stopped.
            conn.settimeout(0.25)
            while not self._stop.is_set():
                try:
                    data = conn.recv(4096)
                except socket.timeout:
                    continue
                except (ssl.SSLError, OSError):
                    break
                if not data:
                    break
        except (ssl.SSLError, OSError):
            # Expected: ALPN-refusal alerts, cert-verification aborts by the
            # client, or the client closing mid-handshake.
            pass
        finally:
            try:
                if conn is not None:
                    conn.close()
                else:
                    raw.close()
            except OSError:
                pass

    def stop(self):
        self._stop.set()
        if self._listener is not None:
            try:
                self._listener.close()
            except OSError:
                pass
        if self._thread is not None:
            self._thread.join(timeout=5.0)

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False
