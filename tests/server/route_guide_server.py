"""Upstream-grpcio TLS RouteGuide server for egrpc M3 integration tests.

This is the compatibility oracle (design doc §7, Ring 2/3): a *real* gRPC server
built from unmodified upstream ``grpcio``. The client under test must interoperate
with it byte-for-byte on the wire, so nothing here is hand-rolled HTTP/2.

Python gencode ignores the proto's ``optimize_for = LITE_RUNTIME`` option, so the
stubs are generated on the fly at server start with ``grpc_tools.protoc`` into a
throwaway directory that is prepended to ``sys.path``.

Usable as a context manager, mirroring ``tls_test_server.TlsTestServer``: bind
127.0.0.1 on an ephemeral port, expose ``.port``, and shut down on ``stop()``.
"""

import importlib
import os
import sys
import tempfile
import threading
import time
from concurrent import futures

import grpc
from grpc_tools import protoc

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_THIS_DIR))
_PROTO_DIR = os.path.join(_REPO_ROOT, "examples", "route_guide")
_PROTO_FILE = os.path.join(_PROTO_DIR, "route_guide.proto")


_STUBS_CACHE = None
_STUBS_LOCK = threading.Lock()


def _generate_stubs():
    """Generate route_guide_pb2 / _pb2_grpc into a tempdir and import them.

    Returns a tuple ``(pb2, pb2_grpc)``. Cached per process so fast server
    restarts do not re-run grpc_tools.protoc.
    """
    global _STUBS_CACHE
    with _STUBS_LOCK:
        if _STUBS_CACHE is None:
            _STUBS_CACHE = _generate_stubs_uncached()
        return _STUBS_CACHE


def _generate_stubs_uncached():
    out_dir = tempfile.mkdtemp(prefix="egrpc_rg_stubs_")

    # grpc_tools bundles the well-known-type .proto files; include their path so
    # imports like google/protobuf resolve during generation.
    well_known = os.path.join(os.path.dirname(protoc.__file__), "_proto")
    args = [
        "grpc_tools.protoc",
        "--proto_path={}".format(_PROTO_DIR),
        "--proto_path={}".format(well_known),
        "--python_out={}".format(out_dir),
        "--grpc_python_out={}".format(out_dir),
        _PROTO_FILE,
    ]
    rc = protoc.main(args)
    if rc != 0:
        raise RuntimeError("grpc_tools.protoc failed with exit code {}".format(rc))

    if out_dir not in sys.path:
        sys.path.insert(0, out_dir)
    pb2 = importlib.import_module("route_guide_pb2")
    pb2_grpc = importlib.import_module("route_guide_pb2_grpc")
    return pb2, pb2_grpc


class RouteGuideTestServer:
    """A real grpcio TLS RouteGuide server usable as a context manager.

    Args:
        certfile: path to the server certificate (PEM).
        keyfile: path to the server private key (PEM).
        port: port to bind on 127.0.0.1 (0 = ephemeral). Binding retries
            briefly so a restart can reuse the previous instance's port.
        root_ca_file: when set, require client certificates signed by this
            CA (mTLS).
        server_generation: instance number reported by ListFeatures mode 7
            (initial metadata "egrpc-gen" and Feature.location.longitude).
    """

    def __init__(
        self,
        certfile,
        keyfile,
        max_concurrent_streams=None,
        port=0,
        root_ca_file=None,
        server_generation=0,
    ):
        self._certfile = certfile
        self._keyfile = keyfile
        self._max_concurrent_streams = max_concurrent_streams
        self._requested_port = port
        self._root_ca_file = root_ca_file
        self.server_generation = server_generation
        self._attempt_lock = threading.Lock()
        self._attempts = {}
        self._server = None
        self.port = None
        self._pb2 = None
        self._pb2_grpc = None
        self._cancel_lock = threading.Lock()
        self._cancelled_calls = 0
        self._cancel_event = threading.Event()

    @property
    def cancelled_calls(self):
        with self._cancel_lock:
            return self._cancelled_calls

    def reset_cancellations(self):
        with self._cancel_lock:
            self._cancelled_calls = 0
            self._cancel_event.clear()

    def wait_for_cancellation(self, timeout=5.0):
        return self._cancel_event.wait(timeout=timeout)

    def _next_attempt(self, mode):
        """Returns the 1-based attempt number of a ListFeatures call in mode."""
        with self._attempt_lock:
            n = self._attempts.get(mode, 0) + 1
            self._attempts[mode] = n
            return n

    def _record_cancellation(self):
        with self._cancel_lock:
            self._cancelled_calls += 1
            self._cancel_event.set()

    def _make_servicer(self):
        pb2 = self._pb2
        pb2_grpc = self._pb2_grpc
        server_obj = self

        class _RouteGuideServicer(pb2_grpc.RouteGuideServicer):
            def GetFeature(self, request, context):
                lat = request.latitude
                lon = request.longitude

                # Error path: emit a Trailers-Only response. We deliberately do
                # NOT call send_initial_metadata first, so grpcio collapses the
                # response into a single HEADERS frame carrying grpc-status.
                if lat == -1:
                    context.set_trailing_metadata(
                        (
                            ("egrpc-test-trailer", "tv1"),
                            ("egrpc-bin-bin", b"\x00\x01"),
                        )
                    )
                    context.abort(
                        grpc.StatusCode.PERMISSION_DENIED, "denied: magic point"
                    )
                    # abort() raises; unreachable, but keeps the type checker happy.
                    return pb2.Feature()

                # Large payload: force multi-DATA-frame reassembly on the client.
                if lat == -3:
                    return pb2.Feature(name="x" * 200000, location=request)

                # Deadline echo: report whether the client's grpc-timeout
                # header reached the server, and roughly how much remained.
                # Lets tests assert timeout propagation end-to-end.
                if lat == -4:
                    remaining = context.time_remaining()
                    # grpcio reports "no deadline" as None or as a huge
                    # sentinel (int64-max-ish seconds), depending on version.
                    if remaining is None or remaining > 10**6:
                        name = "tr-none"
                    else:
                        name = "tr-{}".format(int(round(remaining)))
                    return pb2.Feature(name=name, location=request)

                # Slow point: sleep so that concurrent calls overlap on the
                # server, letting the client-side concurrent-stream cap be
                # observed (the server's thread pool must not serialize them).
                if lat == -5:
                    time.sleep(0.5)
                    return pb2.Feature(name="slow", location=request)

                # Cancellable slow unary: sleeps up to lon ms (default 5000ms),
                # recording server-side cancellation if the client cancels or
                # times out.
                if lat == -6:
                    cancelled = threading.Event()

                    def _on_done():
                        if not context.is_active():
                            cancelled.set()
                            server_obj._record_cancellation()

                    context.add_callback(_on_done)
                    wait_s = (lon / 1000.0) if lon > 0 else 5.0
                    deadline = time.monotonic() + wait_s
                    while time.monotonic() < deadline and context.is_active():
                        if cancelled.wait(0.02):
                            break
                    if not context.is_active():
                        return pb2.Feature()
                    return pb2.Feature(name="slow-unary-done", location=request)

                # Known point: send initial metadata first, then the feature.
                if lat == 1000 and lon == 2000:
                    context.send_initial_metadata((("egrpc-initial", "iv1"),))
                    return pb2.Feature(name="egrpc-test-feature", location=request)

                # Unknown point (route_guide semantics: empty name).
                return pb2.Feature(name="", location=request)

            def ListFeatures(self, request, context):
                mode = request.lo.latitude

                # Mode 0: normal multi-feature stream with initial & trailing
                # metadata (including -bin keys).
                if mode == 0:
                    context.send_initial_metadata(
                        (
                            ("egrpc-stream-init", "siv1"),
                            ("egrpc-stream-init-bin", b"\xde\xad"),
                        )
                    )
                    for i in range(5):
                        yield pb2.Feature(
                            name="feat-{}".format(i),
                            location=pb2.Point(latitude=100 + i, longitude=200 + i),
                        )
                    context.set_trailing_metadata(
                        (
                            ("egrpc-stream-trail", "stv1"),
                            ("egrpc-stream-trail-bin", b"\xbe\xef"),
                        )
                    )
                    return

                # Mode 1: empty stream (0 messages, OK).
                if mode == 1:
                    context.send_initial_metadata((("egrpc-stream-init", "empty"),))
                    context.set_trailing_metadata((("egrpc-stream-trail", "empty-ok"),))
                    return

                # Mode 2: error mid-stream after 3 messages.
                if mode == 2:
                    context.send_initial_metadata((("egrpc-stream-init", "mid-err"),))
                    for i in range(3):
                        yield pb2.Feature(
                            name="ok-{}".format(i),
                            location=pb2.Point(latitude=i, longitude=i),
                        )
                    context.set_trailing_metadata((("egrpc-err-trail", "et1"),))
                    context.abort(
                        grpc.StatusCode.RESOURCE_EXHAUSTED, "mid-stream boom"
                    )
                    return

                # Mode 3: trailers-only error (no initial metadata, no DATA).
                if mode == 3:
                    context.set_trailing_metadata((("egrpc-to-trail", "to1"),))
                    context.abort(
                        grpc.StatusCode.PERMISSION_DENIED, "stream denied"
                    )
                    return

                # Mode 4: burst of N small messages (default 40) to exercise
                # client-side kDraining when > max_queued_messages arrive
                # before the caller reads them all.
                if mode == 4:
                    count = request.hi.latitude if request.hi.latitude > 0 else 40
                    for i in range(count):
                        yield pb2.Feature(
                            name="drain-{}".format(i),
                            location=pb2.Point(latitude=i, longitude=0),
                        )
                    return

                # Mode 5: fast producer emitting count x payload_bytes messages
                # for Read-driven HTTP/2 flow-control backpressure tests.
                if mode == 5:
                    cancelled = threading.Event()

                    def _on_done():
                        if not context.is_active():
                            cancelled.set()
                            server_obj._record_cancellation()

                    context.add_callback(_on_done)
                    count = request.hi.latitude if request.hi.latitude > 0 else 60
                    size = request.hi.longitude if request.hi.longitude > 0 else 4096
                    payload = "p" * size
                    for i in range(count):
                        if not context.is_active():
                            return
                        yield pb2.Feature(
                            name=payload,
                            location=pb2.Point(latitude=i, longitude=size),
                        )
                    return

                # Mode 6: slow/blocking stream for TryCancel and deadline tests.
                # Emits lo.longitude initial messages, then holds the stream
                # open up to hi.latitude ms (default 10000ms) watching for cancel.
                if mode == 6:
                    cancelled = threading.Event()

                    def _on_done():
                        if not context.is_active():
                            cancelled.set()
                            server_obj._record_cancellation()

                    context.add_callback(_on_done)
                    context.send_initial_metadata((("egrpc-stream-init", "slow"),))
                    initial = max(0, request.lo.longitude)
                    for i in range(initial):
                        if not context.is_active():
                            return
                        yield pb2.Feature(
                            name="pre-{}".format(i),
                            location=pb2.Point(latitude=i, longitude=0),
                        )
                    wait_s = (
                        (request.hi.latitude / 1000.0)
                        if request.hi.latitude > 0
                        else 10.0
                    )
                    deadline = time.monotonic() + wait_s
                    while time.monotonic() < deadline and context.is_active():
                        if cancelled.wait(0.02):
                            break
                    return

                # Mode 7: sequenced subscribe stream resuming after cursor
                # lo.longitude, up to hi.latitude (default 10000), one message
                # every hi.longitude ms (default 25).
                if mode == 7:
                    cancelled = threading.Event()

                    def _on_done():
                        if not context.is_active():
                            cancelled.set()

                    context.add_callback(_on_done)
                    gen = server_obj.server_generation
                    cursor = max(0, request.lo.longitude)
                    max_seq = request.hi.latitude if request.hi.latitude > 0 else 10000
                    delay_ms = request.hi.longitude if request.hi.longitude > 0 else 25
                    context.send_initial_metadata((("egrpc-gen", str(gen)),))
                    for seq in range(cursor + 1, max_seq + 1):
                        if not context.is_active():
                            return
                        yield pb2.Feature(
                            name="notif-{}".format(seq),
                            location=pb2.Point(latitude=seq, longitude=gen),
                        )
                        if cancelled.wait(delay_ms / 1000.0):
                            return
                    return

                # Mode 8: the first lo.longitude attempts abort with a
                # server-originated CANCELLED (no initial metadata); later
                # attempts stream hi.latitude (default 3) sequenced events.
                if mode == 8:
                    attempt = server_obj._next_attempt(8)
                    if attempt <= request.lo.longitude:
                        context.abort(grpc.StatusCode.CANCELLED, "server-bounced")
                        return
                    context.send_initial_metadata((("egrpc-attempt", str(attempt)),))
                    count = request.hi.latitude if request.hi.latitude > 0 else 3
                    for seq in range(1, count + 1):
                        yield pb2.Feature(
                            name="notif-{}".format(seq),
                            location=pb2.Point(latitude=seq, longitude=0),
                        )
                    return

                # Mode 9: attempt 1 stalls without initial metadata until the
                # client cancels / times out; later attempts stream
                # hi.latitude (default 3) sequenced events.
                if mode == 9:
                    attempt = server_obj._next_attempt(9)
                    if attempt == 1:
                        cancelled = threading.Event()

                        def _on_done():
                            if not context.is_active():
                                cancelled.set()

                        context.add_callback(_on_done)
                        deadline = time.monotonic() + 30.0
                        while time.monotonic() < deadline and context.is_active():
                            if cancelled.wait(0.02):
                                break
                        return
                    context.send_initial_metadata((("egrpc-attempt", str(attempt)),))
                    count = request.hi.latitude if request.hi.latitude > 0 else 3
                    for seq in range(1, count + 1):
                        yield pb2.Feature(
                            name="notif-{}".format(seq),
                            location=pb2.Point(latitude=seq, longitude=0),
                        )
                    return

                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "unknown ListFeatures mode {}".format(mode),
                )

        return _RouteGuideServicer()

    def start(self):
        self._pb2, self._pb2_grpc = _generate_stubs()

        options = []
        if self._max_concurrent_streams is not None:
            options.append(
                ("grpc.max_concurrent_streams", int(self._max_concurrent_streams))
            )
        self._server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=16), options=options
        )
        self._pb2_grpc.add_RouteGuideServicer_to_server(
            self._make_servicer(), self._server
        )

        with open(self._keyfile, "rb") as fh:
            key = fh.read()
        with open(self._certfile, "rb") as fh:
            cert = fh.read()
        if self._root_ca_file is not None:
            with open(self._root_ca_file, "rb") as fh:
                root_ca = fh.read()
            creds = grpc.ssl_server_credentials(
                ((key, cert),), root_certificates=root_ca, require_client_auth=True
            )
        else:
            creds = grpc.ssl_server_credentials(((key, cert),))
        addr = "127.0.0.1:{}".format(self._requested_port)
        # A fixed port may still be held briefly by a just-stopped instance.
        deadline = time.monotonic() + 10.0
        while True:
            try:
                self.port = self._server.add_secure_port(addr, creds)
            except RuntimeError:
                self.port = 0
            if self.port != 0 or self._requested_port == 0:
                break
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
        if self.port == 0:
            raise RuntimeError("add_secure_port failed to bind {}".format(addr))
        self._server.start()
        return self

    def stop(self, grace=1.0):
        if self._server is not None:
            self._server.stop(grace=grace).wait(timeout=5.0)
            self._server = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False
