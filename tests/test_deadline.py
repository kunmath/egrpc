"""M5 acceptance (design §5.6, §5.7, §8/M5): local deadline enforcement
(unary, streaming, unresponsive peer ignoring grpc-timeout, and queued calls
behind MAX_CONCURRENT_STREAMS) and TryCancel() (pre-start, mid-unary,
mid-stream, with server-side cancellation verification).
"""

import os
import subprocess

import pytest

from conftest import require_test_binary
from server.h2_test_server import H2TestServer
from server.route_guide_server import RouteGuideTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_TIMEOUT_S = 30
_H2_CANCEL = 0x8


@pytest.fixture(scope="module")
def shim_bin():
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


def _elapsed_ms(stdout):
    line = next(ln for ln in stdout.splitlines() if ln.startswith("ELAPSED_MS "))
    return int(line.split()[1])


def test_unary_deadline_fires_against_unresponsive_h2_server(shim_bin, certs):
    """Local EventThread deadline timer must fire DEADLINE_EXCEEDED and send
    RST_STREAM(CANCEL) even when the peer completely ignores grpc-timeout.
    """
    with H2TestServer(certs.cert, certs.key, mode="stall_streams") as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "getfeature",
            "--latitude", "1000", "--longitude", "2000",
            "--deadline-ms", "300",
        )
        assert result.returncode == 1, _fail_msg(result)
        lines = result.stdout.splitlines()
        assert any(
            ln.startswith("GETFEATURE error code=4 ") for ln in lines
        ), _fail_msg(result)
        elapsed = _elapsed_ms(result.stdout)
        assert 200 <= elapsed <= 2500, _fail_msg(result)

        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) >= 1, "expected RST_STREAM from client on deadline"
        assert resets[0][1] == _H2_CANCEL


def test_stream_deadline_fires_against_unresponsive_h2_server(shim_bin, certs):
    """Local deadline timer unblocks ClientReader::Read() with DEADLINE_EXCEEDED
    and sends RST_STREAM(CANCEL) when the server stalls.
    """
    with H2TestServer(certs.cert, certs.key, mode="stall_streams") as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "listfeatures",
            "--lo-lat", "0",
            "--deadline-ms", "300",
        )
        assert result.returncode == 1, _fail_msg(result)
        lines = result.stdout.splitlines()
        assert any(
            ln.startswith("LISTFEATURES code=4 messages=0 ") for ln in lines
        ), _fail_msg(result)
        elapsed = _elapsed_ms(result.stdout)
        assert 200 <= elapsed <= 2500, _fail_msg(result)

        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) >= 1
        assert resets[0][1] == _H2_CANCEL


def test_stream_deadline_mid_stream_against_grpcio_server(
    shim_bin, certs, route_guide_server
):
    """Slow grpcio stream delivers 2 messages, then stalls; deadline fires
    DEADLINE_EXCEEDED within tolerance and server sees cancellation.
    """
    route_guide_server.reset_cancellations()
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "6",
        "--lo-lon", "2",
        "--hi-lat", "10000",
        "--deadline-ms", "400",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert any(
        ln.startswith("LISTFEATURES code=4 messages=2 ") for ln in lines
    ), _fail_msg(result)
    elapsed = _elapsed_ms(result.stdout)
    assert 300 <= elapsed <= 2500, _fail_msg(result)
    assert route_guide_server.wait_for_cancellation(timeout=3.0)


def test_queued_call_deadline_expires_while_waiting_for_stream_slot(
    shim_bin, certs
):
    """§5.6: a call queued in pending_calls_ behind MAX_CONCURRENT_STREAMS=1
    must fail with DEADLINE_EXCEEDED on its own timer without waiting for the
    active call to finish.
    """
    with RouteGuideTestServer(
        certs.cert, certs.key, max_concurrent_streams=1
    ) as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "queued_deadline",
            "--deadline-ms", "250",
        )
        assert result.returncode == 0, _fail_msg(result)
        line = next(
            ln for ln in result.stdout.splitlines() if ln.startswith("QUEUED ")
        )
        fields = dict(tok.split("=", 1) for tok in line.split()[1:])
        assert int(fields["first_code"]) == 0, _fail_msg(result)
        assert int(fields["second_code"]) == 4, _fail_msg(result)  # DEADLINE_EXCEEDED
        # First call takes 1500ms; queued call had a 250ms deadline and must
        # expire well before the first call completes.
        assert int(fields["second_ms"]) < 1000, _fail_msg(result)


def test_queued_call_deadline_never_hits_wire_against_h2_server(shim_bin, certs):
    """§5.6: a call queued behind MAX_CONCURRENT_STREAMS=1 whose deadline fires
    before a slot opens must fail locally and never send HEADERS on the wire.
    """
    with H2TestServer(
        certs.cert, certs.key, mode="stall_streams", max_concurrent_streams=1
    ) as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "queued_deadline",
            "--deadline-ms", "250",
        )
        assert result.returncode == 0, _fail_msg(result)
        line = next(
            ln for ln in result.stdout.splitlines() if ln.startswith("QUEUED ")
        )
        fields = dict(tok.split("=", 1) for tok in line.split()[1:])
        assert int(fields["first_code"]) == 4, _fail_msg(result)  # DEADLINE_EXCEEDED
        assert int(fields["second_code"]) == 4, _fail_msg(result)  # DEADLINE_EXCEEDED
        assert int(fields["second_ms"]) < 1000, _fail_msg(result)
        assert len(srv.requests) == 1, (
            "expected only 1 request on wire, got {}".format(srv.requests)
        )
        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) == 1, "expected exactly 1 RST_STREAM for holder, got {}".format(
            resets
        )
        assert resets[0] == (srv.requests[0], _H2_CANCEL), _fail_msg(result)


def test_try_cancel_prestart_unary_and_stream(shim_bin, certs, route_guide_server):
    """Calling TryCancel() before starting an RPC returns CANCELLED immediately."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "cancel_prestart",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert (
        "PRESTART unary_code=1 stream_code=1 stream_read=0"
        in result.stdout.splitlines()
    ), _fail_msg(result)


def test_try_cancel_unblocks_slow_unary_and_notifies_server(
    shim_bin, certs, route_guide_server
):
    """TryCancel() from another thread unblocks GetFeature with CANCELLED (1)
    and the grpcio server observes stream cancellation.
    """
    route_guide_server.reset_cancellations()
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "getfeature",
        "--latitude", "-6", "--longitude", "8000",
        "--cancel-after-ms", "200",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert any(
        ln.startswith("GETFEATURE error code=1 ") for ln in lines
    ), _fail_msg(result)
    elapsed = _elapsed_ms(result.stdout)
    assert elapsed < 2500, _fail_msg(result)
    assert route_guide_server.wait_for_cancellation(timeout=3.0)


def test_try_cancel_unblocks_blocked_stream_read_and_notifies_server(
    shim_bin, certs, route_guide_server
):
    """M5 acceptance: TryCancel() unblocks a blocked ClientReader::Read() after
    reading 2 initial messages, Finish() returns CANCELLED (1), and the server
    observes cancellation.
    """
    route_guide_server.reset_cancellations()
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "6",
        "--lo-lon", "2",
        "--hi-lat", "10000",
        "--cancel-after-ms", "250",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert any(
        ln.startswith("LISTFEATURES code=1 messages=2 ") for ln in lines
    ), _fail_msg(result)
    elapsed = _elapsed_ms(result.stdout)
    assert elapsed < 2500, _fail_msg(result)
    assert route_guide_server.wait_for_cancellation(timeout=3.0)
