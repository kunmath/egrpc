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
    if not os.path.isfile(path):
        pytest.skip(
            "example client not found at {}; build the project first: "
            "cmake -S . -B build && cmake --build build -j".format(path)
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
