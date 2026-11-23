"""egrpc M6 acceptance (design §5.7, §8/M6): bounded clean shutdown.

Shutdown fails every active and queued call with UNAVAILABLE, never blocks
past shutdown_timeout (against a blackholed peer or a hung DNS lookup), lets
a ClientReader safely outlive its Channel, and says GOAWAY(NO_ERROR) to a
healthy peer before closing.

Drives ``egrpc_resilience_probe`` (tests/integration/resilience_probe_main.cc).
Bounds are wide (shutdown_timeout=1000 ms, asserted < 500 ms) so the suite is
stable under ASan/UBSan/TSan builds (EGRPC_BUILD_DIR=build-asan / build-tsan).
"""

import os
import socket
import subprocess

import pytest

from conftest import require_test_binary
from server.h2_test_server import H2TestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PROBE_TIMEOUT_S = 30

_OK = 0
_CANCELLED = 1
_UNAVAILABLE = 14
_H2_NO_ERROR = 0x0


# Sanitizer builds (EGRPC_BUILD_DIR=build-asan/-tsan): UBSan recovers by
# default (exit 0), so any report on stderr fails the test explicitly.
_SANITIZER_MARKERS = (
    "ERROR: AddressSanitizer",
    "ERROR: LeakSanitizer",
    "WARNING: ThreadSanitizer",
    "runtime error:",
)


def _assert_no_sanitizer_reports(stderr):
    hits = [m for m in _SANITIZER_MARKERS if m in stderr]
    assert not hits, "sanitizer report(s) {}:\n{}".format(hits, stderr)


@pytest.fixture(scope="module")
def probe_bin():
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(build_dir, "tests", "integration", "egrpc_resilience_probe")
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


def _run_probe(probe_bin, certs, port, mode, *extra_args):
    return subprocess.run(
        [
            probe_bin,
            "--host", "localhost",
            "--port", str(port),
            "--ca", certs.cert,
            "--mode", mode,
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


def _checked(result):
    assert result.returncode == 0, _fail_msg(result)
    _assert_no_sanitizer_reports(result.stderr)
    kv = {}
    for ln in result.stdout.splitlines():
        if "=" in ln:
            k, v = ln.split("=", 1)
            kv[k] = v
    assert kv.get("DONE") == "1", _fail_msg(result)
    return kv


def test_shutdown_unblocks_active_calls_and_outliving_reader(certs, probe_bin):
    # stall_after_headers answers every fully-received request with initial
    # HEADERS only, so the probe synchronizes on initial metadata: proof that
    # the server holds each request before the channel is dropped / shut down.
    # The server also advertises MAX_CONCURRENT_STREAMS=2, matching Part B's
    # local cap, so a third stream would be a protocol violation.
    with H2TestServer(
        certs.cert, certs.key, mode="stall_after_headers", max_concurrent_streams=2
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shutdown_active_and_outliving_reader",
            "--shutdown-timeout-ms", "1000",
        )
        requests = srv.requests

    kv = _checked(result)
    # Both parts synchronized deterministically (no fixed sleeps): the calls
    # were genuinely in flight when the channel was dropped / shut down.
    assert kv["PART_A_SYNC"] == "1", _fail_msg(result)
    assert kv["PART_B_SYNC"] == "1", _fail_msg(result)
    # Part A (public API): dropping the last Channel reference while another
    # thread is blocked in ClientReader::Read() unblocks it promptly; the
    # reader outlives the channel and reports a local UNAVAILABLE.
    assert kv["READER_READ_OK"] == "0", _fail_msg(result)
    assert kv["READER_ENDED_AFTER_DROP"] == "1", _fail_msg(result)
    assert int(kv["READER_UNBLOCK_MS"]) < 1000, _fail_msg(result)
    assert int(kv["CHANNEL_DROP_MS"]) < 1000, _fail_msg(result)
    assert kv["READER_READ_AGAIN_OK"] == "0", _fail_msg(result)
    assert kv["READER_FINISH_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["READER_FINISH_LOCAL"] == "1", _fail_msg(result)

    # Part B (core channel): active stream + active unary + a wait-for-ready
    # call queued behind the 2-stream cap all fail UNAVAILABLE on Shutdown().
    assert kv["STATE_BEFORE_SHUTDOWN"] == "READY", _fail_msg(result)
    assert kv["WFR_DONE_BEFORE_SHUTDOWN"] == "0", _fail_msg(result)
    # High-water mark read after Shutdown(): the queued call never got a slot.
    assert kv["MAX_ACTIVE_CALLS"] == "2", _fail_msg(result)
    for prefix in ("STREAM", "UNARY", "WFR"):
        assert kv[prefix + "_STATUS"] == str(_UNAVAILABLE), (prefix, _fail_msg(result))
        assert kv[prefix + "_ORIGIN"] == "local", (prefix, _fail_msg(result))
    assert kv["STREAM_POP_OK"] == "0", _fail_msg(result)
    assert int(kv["SHUTDOWN_MS"]) < 500, _fail_msg(result)
    assert int(kv["UNBLOCK_MAX_MS"]) < 1000, _fail_msg(result)
    # A call after Shutdown() is rejected, not queued.
    assert kv["POST_SHUTDOWN_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)

    # Server saw exactly: Part A's ListFeatures + Part B's stream and unary.
    # The wait-for-ready call never reached the wire (it was still queued).
    assert len(requests) == 3, requests


def test_shutdown_bounded_against_blackhole_peer(certs, probe_bin):
    with H2TestServer(certs.cert, certs.key, mode="shutdown_blackhole") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shutdown_blackhole",
            "--shutdown-timeout-ms", "1000",
        )
    kv = _checked(result)
    assert kv["INITIAL_METADATA"] == "1", _fail_msg(result)
    assert kv["SYNC"] == "1", _fail_msg(result)  # READY with the stream active
    assert kv["CALL_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL_ORIGIN"] == "local", _fail_msg(result)
    # The peer never reads: shutdown must not wait for it.
    assert int(kv["SHUTDOWN_MS"]) < 500, _fail_msg(result)
    assert int(kv["CALL_UNBLOCK_MS"]) < 1000, _fail_msg(result)


def test_shutdown_bounded_with_large_pending_outbound(certs, probe_bin):
    """Fix #2 regression: a 2 MiB unary request is pending output on a
    connection whose peer stopped reading; Shutdown() right after submission
    still returns within the shutdown_timeout budget."""
    with H2TestServer(certs.cert, certs.key, mode="shutdown_blackhole") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shutdown_large_outbound",
            "--shutdown-timeout-ms", "1000",
            "--payload-bytes", str(2 * 1024 * 1024),
        )
    kv = _checked(result)
    assert kv["INITIAL_METADATA"] == "1", _fail_msg(result)
    assert int(kv["REQUEST_BYTES"]) > 2 * 1024 * 1024, _fail_msg(result)
    assert kv["SYNC"] == "1", _fail_msg(result)  # both calls submitted
    # Bounded by shutdown_timeout (1000 ms) + slack, never by the backlog.
    assert int(kv["SHUTDOWN_MS"]) < 1500, _fail_msg(result)
    for prefix in ("STREAM", "UNARY"):
        assert kv[prefix + "_STATUS"] == str(_UNAVAILABLE), (prefix, _fail_msg(result))
        assert kv[prefix + "_ORIGIN"] == "local", (prefix, _fail_msg(result))
    assert int(kv["UNBLOCK_MAX_MS"]) < 2000, _fail_msg(result)


def test_shutdown_bounded_with_large_window_pending_outbound(certs, probe_bin):
    """Finding #1 regression: the peer advertised 4 MiB of stream
    (SETTINGS_INITIAL_WINDOW_SIZE) and connection (WINDOW_UPDATE) send window
    before going dead, so nghttp2 is not flow-control-limited and megabytes
    of the 4 MiB unary request are eligible for output when Shutdown() fires
    -- only the socket buffers hold it back. The shutdown flush must still
    stay bounded by shutdown_timeout."""
    payload = 4 * 1024 * 1024
    with H2TestServer(
        certs.cert, certs.key, mode="shutdown_blackhole_large_window"
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shutdown_large_outbound",
            "--shutdown-timeout-ms", "1000",
            "--payload-bytes", str(payload),
        )
    kv = _checked(result)
    assert kv["INITIAL_METADATA"] == "1", _fail_msg(result)
    assert int(kv["REQUEST_BYTES"]) > payload, _fail_msg(result)
    assert kv["SYNC"] == "1", _fail_msg(result)  # both calls submitted
    # Bounded by shutdown_timeout (1000 ms) + slack, never by the backlog.
    assert int(kv["SHUTDOWN_MS"]) < 1500, _fail_msg(result)
    for prefix in ("STREAM", "UNARY"):
        assert kv[prefix + "_STATUS"] == str(_UNAVAILABLE), (prefix, _fail_msg(result))
        assert kv[prefix + "_ORIGIN"] == "local", (prefix, _fail_msg(result))
    assert int(kv["UNBLOCK_MAX_MS"]) < 2000, _fail_msg(result)


def test_precancelled_calls_fail_cancelled_with_local_origin(certs, probe_bin):
    """Fix #3: TryCancel() before the call is issued fails unary and
    server-streaming calls (unmodified generated stubs) with CANCELLED and
    egrpc_status_is_local() == true, without ever touching the transport."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()  # nothing listens: any connect attempt would be visible
    result = _run_probe(probe_bin, certs, port, "precancel_local_origin")
    kv = _checked(result)
    assert kv["UNARY_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["UNARY_LOCAL"] == "1", _fail_msg(result)
    assert kv["UNARY_RESPONSE"] == "untouched", _fail_msg(result)
    assert kv["STREAM_READ_OK"] == "0", _fail_msg(result)
    assert kv["STREAM_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["STREAM_LOCAL"] == "1", _fail_msg(result)
    # Never submitted: the channel never even started connecting.
    assert kv["CONNECT_ATTEMPTS"] == "0", _fail_msg(result)
    assert kv["STATE"] == "IDLE", _fail_msg(result)


def test_shutdown_bounded_during_hung_dns(certs, probe_bin):
    # No server: the injected resolver hangs before any connect happens.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    result = _run_probe(
        probe_bin, certs, port, "shutdown_hung_dns",
        "--shutdown-timeout-ms", "1000",
    )
    kv = _checked(result)
    assert kv["RESOLVER_ENTERED"] == "1", _fail_msg(result)
    assert int(kv["SHUTDOWN_MS"]) < 500, _fail_msg(result)
    assert kv["CALL_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL_ORIGIN"] == "local", _fail_msg(result)
    assert int(kv["CALL_UNBLOCK_MS"]) < 1000, _fail_msg(result)
    # The detached resolver worker exited once released (no leak at exit).
    assert kv["WORKER_EXITED"] == "1", _fail_msg(result)


def test_shutdown_sends_graceful_goaway_no_error(certs, probe_bin):
    with H2TestServer(certs.cert, certs.key, mode="multi_conn_ok") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shutdown_graceful_goaway",
            "--shutdown-timeout-ms", "1000",
        )
        goaways = srv.wait_for_goaways(1, timeout=3.0)
        closed = srv.wait_for_conn_closed(1, timeout=3.0)

    kv = _checked(result)
    assert kv["CALL1_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL1_NAME"] == "ok-conn-1", _fail_msg(result)
    assert int(kv["SHUTDOWN_MS"]) < 1000, _fail_msg(result)
    assert len(goaways) == 1, goaways
    _last_stream_id, error_code = goaways[0]
    assert error_code == _H2_NO_ERROR, goaways
    assert closed is not None and closed["goaways"] == goaways, closed
