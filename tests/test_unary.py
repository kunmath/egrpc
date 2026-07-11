"""egrpc M3 acceptance: hand-rolled unary call end-to-end (design doc §8, M3).

Drives the compiled ``egrpc_unary_call`` probe against a real upstream-grpcio
RouteGuide TLS server (the compatibility oracle). Each test runs the probe once
and asserts on its stdout protocol (see tests/integration/unary_call_main.cc):

  STATUS code=<int> message=<grpc-message, percent-decoded>
  FEATURE name=<name> latitude=<lat> longitude=<lon>   (only when code=0)
  INITIAL <key>=<value>       (per initial-metadata entry)
  TRAILER <key>=<value>       (per trailing-metadata entry)

Exit 0 = the call completed with *some* grpc-status; the status itself is
asserted from the STATUS line.
"""

import os
import subprocess

import pytest

from conftest import require_test_binary

from server.route_guide_server import RouteGuideTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Generous ceiling: TLS handshake + protoc stub-gen + up to a few calls.
_PROBE_TIMEOUT_S = 30


@pytest.fixture(scope="session")
def unary_bin():
    """Absolute path to the compiled egrpc_unary_call probe binary."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(
        _REPO_ROOT, "build"
    )
    path = os.path.join(build_dir, "tests", "integration", "egrpc_unary_call")
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


def _run_probe(unary_bin, certs, port, *extra_args):
    return subprocess.run(
        [
            unary_bin,
            "--host", "localhost",
            "--port", str(port),
            "--ca", certs.cert,
            *extra_args,
        ],
        capture_output=True,
        text=True,
        timeout=_PROBE_TIMEOUT_S,
    )


def _fail_msg(result):
    return "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        result.stdout, result.stderr
    )


def test_known_point_returns_feature_with_initial_metadata(
    unary_bin, certs, route_guide_server
):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "STATUS code=0 message=" in lines, _fail_msg(result)
    assert (
        "FEATURE name=egrpc-test-feature latitude=1000 longitude=2000" in lines
    ), _fail_msg(result)
    assert "INITIAL egrpc-initial=iv1" in lines, _fail_msg(result)


def test_unknown_point_returns_empty_feature_no_initial(
    unary_bin, certs, route_guide_server
):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "5", "--longitude", "6",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "STATUS code=0 message=" in lines, _fail_msg(result)
    assert "FEATURE name= latitude=5 longitude=6" in lines, _fail_msg(result)
    assert not any(
        ln.startswith("INITIAL egrpc-initial=") for ln in lines
    ), _fail_msg(result)


def test_trailers_only_error_path(unary_bin, certs, route_guide_server):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "-1",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()

    # grpc-status 7 = PERMISSION_DENIED, with the abort() message.
    assert "STATUS code=7 message=denied: magic point" in lines, _fail_msg(result)

    # Custom trailing metadata must surface.
    assert "TRAILER egrpc-test-trailer=tv1" in lines, _fail_msg(result)

    # -bin trailer: 0x0001 base64-encoded, padded ("AAE=") or unpadded ("AAE").
    bin_lines = [
        ln for ln in lines if ln.startswith("TRAILER egrpc-bin-bin=")
    ]
    assert len(bin_lines) == 1, _fail_msg(result)
    bin_value = bin_lines[0].split("=", 1)[1]
    assert bin_value in ("AAE", "AAE="), _fail_msg(result)

    # Trailers-only response ⇒ no initial metadata at all.
    assert not any(ln.startswith("INITIAL ") for ln in lines), _fail_msg(result)


def test_large_payload_multi_data_frame_reassembly(
    unary_bin, certs, route_guide_server
):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "-3",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "STATUS code=0 message=" in lines, _fail_msg(result)

    feature_lines = [ln for ln in lines if ln.startswith("FEATURE name=")]
    assert len(feature_lines) == 1, _fail_msg(result)
    # "FEATURE name=" + 200000 'x' + " latitude=-3 longitude=0"
    expected = "FEATURE name={} latitude=-3 longitude=0".format("x" * 200000)
    assert feature_lines[0] == expected, (
        "large-payload feature name mismatch (len={})".format(
            len(feature_lines[0])
        )
        + _fail_msg(result)
    )


def test_connection_reuse_three_calls(unary_bin, certs, route_guide_server):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000", "--calls", "3",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    status_ok = [ln for ln in lines if ln == "STATUS code=0 message="]
    feature_lines = [ln for ln in lines if ln.startswith("FEATURE name=")]
    assert len(status_ok) == 3, _fail_msg(result)
    assert len(feature_lines) == 3, _fail_msg(result)


def test_deadline_expired_before_submit_fails_locally(
    unary_bin, certs, route_guide_server
):
    """grpc-timeout is computed when HEADERS are built on the EventThread,
    not at call entry. --timeout-ms 0 sets a deadline that has always
    expired by the time the connect completes and the call reaches
    submission, so it must fail locally with DEADLINE_EXCEEDED (4) and
    never produce a response."""
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000", "--timeout-ms", "0",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "STATUS code=4 message=Deadline Exceeded" in lines, _fail_msg(result)
    assert not any(
        ln.startswith("FEATURE ") for ln in lines
    ), _fail_msg(result)


def test_deadline_header_does_not_break_server(
    unary_bin, certs, route_guide_server
):
    result = _run_probe(
        unary_bin, certs, route_guide_server.port,
        "--latitude", "1000", "--longitude", "2000", "--timeout-ms", "5000",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "STATUS code=0 message=" in lines, _fail_msg(result)
    assert (
        "FEATURE name=egrpc-test-feature latitude=1000 longitude=2000" in lines
    ), _fail_msg(result)
