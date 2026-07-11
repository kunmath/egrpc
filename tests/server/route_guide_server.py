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
from concurrent import futures

import grpc
from grpc_tools import protoc

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_THIS_DIR))
_PROTO_DIR = os.path.join(_REPO_ROOT, "examples", "route_guide")
_PROTO_FILE = os.path.join(_PROTO_DIR, "route_guide.proto")


def _generate_stubs():
    """Generate route_guide_pb2 / _pb2_grpc into a tempdir and import them.

    Returns a tuple ``(pb2, pb2_grpc)`` of the freshly imported modules.
    """
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
    """

    def __init__(self, certfile, keyfile):
        self._certfile = certfile
        self._keyfile = keyfile
        self._server = None
        self.port = None
        self._pb2 = None
        self._pb2_grpc = None

    def _make_servicer(self):
        pb2 = self._pb2
        pb2_grpc = self._pb2_grpc

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

                # Known point: send initial metadata first, then the feature.
                if lat == 1000 and lon == 2000:
                    context.send_initial_metadata((("egrpc-initial", "iv1"),))
                    return pb2.Feature(name="egrpc-test-feature", location=request)

                # Unknown point (route_guide semantics: empty name).
                return pb2.Feature(name="", location=request)

        return _RouteGuideServicer()

    def start(self):
        self._pb2, self._pb2_grpc = _generate_stubs()

        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        self._pb2_grpc.add_RouteGuideServicer_to_server(
            self._make_servicer(), self._server
        )

        with open(self._keyfile, "rb") as fh:
            key = fh.read()
        with open(self._certfile, "rb") as fh:
            cert = fh.read()
        creds = grpc.ssl_server_credentials(((key, cert),))
        self.port = self._server.add_secure_port("127.0.0.1:0", creds)
        if self.port == 0:
            raise RuntimeError("add_secure_port failed to bind 127.0.0.1:0")
        self._server.start()
        return self

    def stop(self):
        if self._server is not None:
            self._server.stop(grace=1.0).wait(timeout=5.0)
            self._server = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False
