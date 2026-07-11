"""M4 acceptance (design §8): unary calls through UNMODIFIED generated stubs.

Ring-2/3 interop: the vendored route_guide example client — built from
grpc_cpp_plugin v1.82.0 output compiled against egrpc's grpcpp shim — runs
GetFeature against a real upstream-grpcio TLS server. This exercises the
whole shim funnel (CreateChannel → NewStub → BlockingUnaryCall →
SerializationTraits) rather than the M3 byte-level probe.
"""

import os
import subprocess

import pytest

from conftest import require_test_binary
from server.route_guide_server import RouteGuideTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_TIMEOUT_S = 30


@pytest.fixture(scope="module")
def shim_bin():
    """Absolute path to the compiled route_guide example client (M4)."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(
        build_dir, "examples", "route_guide", "egrpc_route_guide_client"
    )
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


@pytest.fixture(scope="module")
def route_guide_server(certs):
    """A real grpcio RouteGuide TLS server, started once per module."""
    with RouteGuideTestServer(certs.cert, certs.key) as srv:
        yield srv


def _run_client(shim_bin, certs, port, *extra_args):
    return subprocess.run(
        [
            shim_bin,
            "--target", "localhost:{}".format(port),
            "--ca-file", certs.cert,
            *extra_args,
        ],
        capture_output=True,
        text=True,
        timeout=_CLIENT_TIMEOUT_S,
    )


def _fail_msg(result):
    return "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        result.stdout, result.stderr
    )


def test_known_point_returns_feature(shim_bin, certs, route_guide_server):
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert (
        "GETFEATURE ok name=egrpc-test-feature latitude=1000 longitude=2000"
        in result.stdout.splitlines()
    ), _fail_msg(result)


def test_known_point_with_deadline(shim_bin, certs, route_guide_server):
    """ClientContext deadline flows through the shim as grpc-timeout."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000",
        "--deadline-ms", "5000",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert (
        "GETFEATURE ok name=egrpc-test-feature latitude=1000 longitude=2000"
        in result.stdout.splitlines()
    ), _fail_msg(result)


def test_unknown_point_returns_empty_feature(shim_bin, certs, route_guide_server):
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "5", "--longitude", "6",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert (
        "GETFEATURE ok name= latitude=5 longitude=6" in result.stdout.splitlines()
    ), _fail_msg(result)


def test_error_status_surfaces_as_grpc_status(shim_bin, certs, route_guide_server):
    """Trailers-Only PERMISSION_DENIED becomes grpc::Status(7, message)."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "-1",
    )
    assert result.returncode == 1, _fail_msg(result)
    assert (
        "GETFEATURE error code=7 message=denied: magic point"
        in result.stdout.splitlines()
    ), _fail_msg(result)


def test_large_response_reassembled(shim_bin, certs, route_guide_server):
    """A 200 kB feature name spans DATA frames; SerializationTraits gets it whole."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "-3", "--longitude", "0",
    )
    assert result.returncode == 0, _fail_msg(result)
    expected = "GETFEATURE ok name={} latitude=-3 longitude=0".format("x" * 200000)
    assert expected in result.stdout.splitlines(), _fail_msg(result)


def test_deadline_reaches_server_as_grpc_timeout(shim_bin, certs, route_guide_server):
    """The server echoes context.time_remaining(): 5 s deadline must arrive."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "-4", "--longitude", "0",
        "--deadline-ms", "5000",
    )
    assert result.returncode == 0, _fail_msg(result)
    line = next(
        ln for ln in result.stdout.splitlines() if ln.startswith("GETFEATURE ok")
    )
    remaining = int(line.split("name=tr-")[1].split(" ")[0])
    assert 1 <= remaining <= 5, _fail_msg(result)


def test_no_deadline_means_no_grpc_timeout(shim_bin, certs, route_guide_server):
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "-4", "--longitude", "0",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert (
        "GETFEATURE ok name=tr-none latitude=-4 longitude=0"
        in result.stdout.splitlines()
    ), _fail_msg(result)


def test_initial_metadata_exposed_via_client_context(
    shim_bin, certs, route_guide_server
):
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert "INITIAL egrpc-initial=iv1" in result.stdout.splitlines(), _fail_msg(result)


def test_trailing_metadata_exposed_with_bin_decoding(
    shim_bin, certs, route_guide_server
):
    """Trailers-Only error path: text and -bin (base64→bytes) trailing metadata."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--latitude", "-1",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "TRAILING egrpc-test-trailer=tv1" in lines, _fail_msg(result)
    # b"\x00\x01", base64 on the wire, decoded by the shim, hex-escaped by
    # the example client for line-oriented output.
    assert "TRAILING egrpc-bin-bin=\\x00\\x01" in lines, _fail_msg(result)


def test_server_streaming_stub_reports_unimplemented(
    shim_bin, certs, route_guide_server
):
    """M4 contract for the M5-pending ClientReader: no messages, UNIMPLEMENTED, no hang."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert "LISTFEATURES code=12 messages=0" in result.stdout.splitlines(), _fail_msg(
        result
    )


def test_concurrent_unary_calls(shim_bin, certs, route_guide_server):
    """8 caller threads on one channel (design §3); all must succeed."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "concurrent", "--threads", "8",
        "--latitude", "1000", "--longitude", "2000",
        "--deadline-ms", "15000",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert "CONCURRENT ok=8 total=8" in result.stdout.splitlines(), _fail_msg(result)
