"""Upstream-grpcio TLS interop TestService server for egrpc M8.

Compatibility oracle (design §7, Ring 3): serves the UNMODIFIED gRPC v1.82.0
``grpc.testing.TestService`` from the vendored protos in ``tests/interop/proto``
with a servicer that mirrors upstream
``src/python/grpcio_tests/tests/interop/service.py`` line for line.

Python stubs are generated on the fly with ``grpc_tools.protoc`` into a
process-cached tempdir (removed at exit) so that the upstream import layout
``src.proto.grpc.testing.*_pb2`` resolves exactly as in the upstream tree.
"""

import atexit
import importlib
import os
import shutil
import sys
import tempfile
import threading
import time
from concurrent import futures

import grpc
from grpc_tools import protoc

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(os.path.dirname(_THIS_DIR))
_PROTO_ROOT = os.path.join(_REPO_ROOT, "tests", "interop", "proto")
_PROTO_REL = "src/proto/grpc/testing"
_PROTO_FILES = ("empty.proto", "messages.proto", "test.proto")

_STUBS_CACHE = None
_STUBS_LOCK = threading.Lock()


def _generate_stubs():
    """Returns ``(empty_pb2, messages_pb2, test_pb2_grpc)``, cached per process."""
    global _STUBS_CACHE
    with _STUBS_LOCK:
        if _STUBS_CACHE is None:
            _STUBS_CACHE = _generate_stubs_uncached()
        return _STUBS_CACHE


def _generate_stubs_uncached():
    out_dir = tempfile.mkdtemp(prefix="egrpc_interop_stubs_")
    atexit.register(shutil.rmtree, out_dir, True)

    well_known = os.path.join(os.path.dirname(protoc.__file__), "_proto")
    args = [
        "grpc_tools.protoc",
        "--proto_path={}".format(_PROTO_ROOT),
        "--proto_path={}".format(well_known),
        "--python_out={}".format(out_dir),
        "--grpc_python_out={}".format(out_dir),
    ] + [os.path.join(_PROTO_ROOT, _PROTO_REL, f) for f in _PROTO_FILES]
    rc = protoc.main(args)
    if rc != 0:
        raise RuntimeError("grpc_tools.protoc failed with exit code {}".format(rc))

    # Make src/, src/proto/, ... importable as regular packages.
    pkg = out_dir
    for part in _PROTO_REL.split("/"):
        pkg = os.path.join(pkg, part)
        open(os.path.join(pkg, "__init__.py"), "a").close()

    if out_dir not in sys.path:
        sys.path.insert(0, out_dir)
    empty_pb2 = importlib.import_module("src.proto.grpc.testing.empty_pb2")
    messages_pb2 = importlib.import_module("src.proto.grpc.testing.messages_pb2")
    test_pb2_grpc = importlib.import_module("src.proto.grpc.testing.test_pb2_grpc")
    return empty_pb2, messages_pb2, test_pb2_grpc


# --- Mirror of upstream grpc-1.82.0 tests/interop/service.py ---------------

_INITIAL_METADATA_KEY = "x-grpc-test-echo-initial"
_TRAILING_METADATA_KEY = "x-grpc-test-echo-trailing-bin"
_US_IN_A_SECOND = 1000 * 1000


def _maybe_echo_metadata(servicer_context):
    """Copies metadata from request to response if it is present."""
    invocation_metadata = dict(servicer_context.invocation_metadata())
    if _INITIAL_METADATA_KEY in invocation_metadata:
        initial_metadatum = (
            _INITIAL_METADATA_KEY,
            invocation_metadata[_INITIAL_METADATA_KEY],
        )
        servicer_context.send_initial_metadata((initial_metadatum,))
    if _TRAILING_METADATA_KEY in invocation_metadata:
        trailing_metadatum = (
            _TRAILING_METADATA_KEY,
            invocation_metadata[_TRAILING_METADATA_KEY],
        )
        servicer_context.set_trailing_metadata((trailing_metadatum,))


def _maybe_echo_status_and_message(request, servicer_context):
    """Sets the response context code and details if the request asks for them"""
    if request.HasField("response_status"):
        servicer_context.set_code(request.response_status.code)
        servicer_context.set_details(request.response_status.message)


def _make_test_service(empty_pb2, messages_pb2, test_pb2_grpc):
    class TestService(test_pb2_grpc.TestServiceServicer):
        def EmptyCall(self, request, context):
            _maybe_echo_metadata(context)
            return empty_pb2.Empty()

        def UnaryCall(self, request, context):
            _maybe_echo_metadata(context)
            _maybe_echo_status_and_message(request, context)
            return messages_pb2.SimpleResponse(
                payload=messages_pb2.Payload(
                    type=messages_pb2.COMPRESSABLE,
                    body=b"\x00" * request.response_size,
                )
            )

        def StreamingOutputCall(self, request, context):
            _maybe_echo_status_and_message(request, context)
            for response_parameters in request.response_parameters:
                if response_parameters.interval_us != 0:
                    time.sleep(response_parameters.interval_us / _US_IN_A_SECOND)
                yield messages_pb2.StreamingOutputCallResponse(
                    payload=messages_pb2.Payload(
                        type=request.response_type,
                        body=b"\x00" * response_parameters.size,
                    )
                )

        def StreamingInputCall(self, request_iterator, context):
            aggregate_size = 0
            for request in request_iterator:
                if request.payload is not None and request.payload.body:
                    aggregate_size += len(request.payload.body)
            return messages_pb2.StreamingInputCallResponse(
                aggregated_payload_size=aggregate_size
            )

        def FullDuplexCall(self, request_iterator, context):
            _maybe_echo_metadata(context)
            for request in request_iterator:
                _maybe_echo_status_and_message(request, context)
                for response_parameters in request.response_parameters:
                    if response_parameters.interval_us != 0:
                        time.sleep(
                            response_parameters.interval_us / _US_IN_A_SECOND
                        )
                    yield messages_pb2.StreamingOutputCallResponse(
                        payload=messages_pb2.Payload(
                            type=request.payload.type,
                            body=b"\x00" * response_parameters.size,
                        )
                    )

        def HalfDuplexCall(self, request_iterator, context):
            return self.FullDuplexCall(request_iterator, context)

        # UnimplementedCall is deliberately inherited from TestServiceServicer,
        # which aborts with UNIMPLEMENTED — exactly as upstream.

    return TestService


class InteropTestServer:
    """A real grpcio TLS interop TestService server usable as a context manager.

    Args:
        certfile: path to the server certificate (PEM).
        keyfile: path to the server private key (PEM).
        port: port to bind on 127.0.0.1 (0 = ephemeral).
    """

    def __init__(self, certfile, keyfile, port=0):
        self._certfile = certfile
        self._keyfile = keyfile
        self._requested_port = port
        self._server = None
        self.port = None

    def start(self):
        empty_pb2, messages_pb2, test_pb2_grpc = _generate_stubs()
        with open(self._certfile, "rb") as f:
            cert = f.read()
        with open(self._keyfile, "rb") as f:
            key = f.read()
        creds = grpc.ssl_server_credentials(((key, cert),))

        # Sleeping handlers (timeout/cancel cases) each hold a worker for up to
        # 10 s, so size the pool generously.
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
        test_pb2_grpc.add_TestServiceServicer_to_server(
            _make_test_service(empty_pb2, messages_pb2, test_pb2_grpc)(), server
        )
        port = server.add_secure_port(
            "127.0.0.1:{}".format(self._requested_port), creds
        )
        if port == 0:
            raise RuntimeError(
                "failed to bind 127.0.0.1:{}".format(self._requested_port)
            )
        server.start()
        self._server = server
        self.port = port
        return self

    def stop(self):
        if self._server is not None:
            self._server.stop(grace=None).wait()
            self._server = None

    def __enter__(self):
        return self.start()

    def __exit__(self, exc_type, exc, tb):
        self.stop()
        return False
