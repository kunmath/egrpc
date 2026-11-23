"""egrpc M2 acceptance: HTTP/2 keepalive PING scheduling + enforcement.

Drives the compiled probe binary ``egrpc_h2_keepalive`` against the hyper-h2
test server. The probe binary may not exist yet (M2 is in progress), so these
tests skip cleanly when it is missing.

Probe CLI contract (FROZEN):
  egrpc_h2_keepalive --host localhost --port P [--ca FILE] [--insecure]
      [--keepalive-time-ms N] [--keepalive-timeout-ms N] [--run-ms N]
      [--connect-timeout-ms N]
Stdout (line-buffered): "CONNECTED alpn=h2", "REMOTE_SETTINGS",
  "SETTINGS_ACKED", "PING_ACK n=<k>" per ack, "GOAWAY code=<c> too_many_pings=
  <0|1>" if GOAWAY, then on normal completion "DONE pings_acked=<k>", exit 0.
Keepalive death: prints "DEAD" (last line), exit 3.
Failure: stderr "FAILED: <detail>", exit 1.
"""

import json
import os
import subprocess
import time

import pytest

from conftest import require_test_binary

from server.h2_test_server import H2TestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def keepalive_probe_bin():
    """Absolute path to the compiled egrpc_h2_keepalive probe binary."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(
        _REPO_ROOT, "build"
    )
    path = os.path.join(build_dir, "tests", "integration", "egrpc_h2_keepalive")
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


def _run_probe(probe_bin, host, port, *extra_args, timeout):
    return subprocess.run(
        [
            probe_bin,
            "--host", host,
            "--port", str(port),
            *extra_args,
        ],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_settings_exchange_and_ping_schedule(certs, keepalive_probe_bin, tmp_path):
    report = tmp_path / "normal_report.json"
    with H2TestServer(
        certs.cert, certs.key, mode="normal", report_path=str(report)
    ) as srv:
        result = _run_probe(
            keepalive_probe_bin,
            "localhost",
            srv.port,
            "--ca", certs.cert,
            "--keepalive-time-ms", "1000",
            "--keepalive-timeout-ms", "2000",
            "--run-ms", "3600",
            timeout=15,
        )

    assert result.returncode == 0, result.stderr
    assert "CONNECTED alpn=h2" in result.stdout
    assert "REMOTE_SETTINGS" in result.stdout
    assert "SETTINGS_ACKED" in result.stdout

    done_lines = [
        ln for ln in result.stdout.splitlines() if ln.startswith("DONE pings_acked=")
    ]
    assert done_lines, result.stdout
    acked = int(done_lines[-1].split("=", 1)[1])
    assert acked >= 2, result.stdout

    with open(str(report)) as fh:
        rep = json.load(fh)
    assert rep["remote_settings"] is not None
    pings = rep["pings"]
    assert len(pings) >= 2, rep

    # First ping fires ~keepalive-time (1.0s) after the SETTINGS exchange.
    first_gap = pings[0] - rep["remote_settings"]
    assert 0.6 <= first_gap <= 1.9, first_gap

    # Subsequent pings are spaced by ~keepalive-time as well.
    for prev, cur in zip(pings, pings[1:]):
        gap = cur - prev
        assert 0.6 <= gap <= 1.9, gap


def test_blackhole_detected_within_keepalive_timeout(certs, keepalive_probe_bin):
    with H2TestServer(certs.cert, certs.key, mode="blackhole") as srv:
        start = time.monotonic()
        result = _run_probe(
            keepalive_probe_bin,
            "localhost",
            srv.port,
            "--ca", certs.cert,
            "--keepalive-time-ms", "1000",
            "--keepalive-timeout-ms", "2000",
            "--run-ms", "20000",
            timeout=15,
        )
        elapsed = time.monotonic() - start

    assert result.returncode == 3, result.stdout + result.stderr
    assert "DEAD" in result.stdout
    # Expected ~3s: 1s idle before the first ping + 2s unacked-ping window.
    assert elapsed < 8.0, elapsed


def test_goaway_too_many_pings_reported(certs, keepalive_probe_bin):
    with H2TestServer(
        certs.cert, certs.key, mode="goaway_too_many_pings"
    ) as srv:
        result = _run_probe(
            keepalive_probe_bin,
            "localhost",
            srv.port,
            "--ca", certs.cert,
            "--keepalive-time-ms", "500",
            "--keepalive-timeout-ms", "2000",
            "--run-ms", "4000",
            timeout=15,
        )

    # Exit code is intentionally not asserted: after GOAWAY the probe may finish
    # normally (0) or treat the closed connection as death (3).
    assert "GOAWAY code=11 too_many_pings=1" in result.stdout, (
        result.stdout + result.stderr
    )


# --- M6: channel-level keepalive (design §5.2) ---------------------------------
#
# Drive the full ChannelImpl (reconnect + backoff) through the M6
# ``egrpc_resilience_probe`` against multi-connection server modes.

_OK = 0


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


@pytest.fixture(scope="session")
def resilience_probe_bin():
    """Absolute path to the compiled egrpc_resilience_probe binary."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(
        _REPO_ROOT, "build"
    )
    path = os.path.join(build_dir, "tests", "integration", "egrpc_resilience_probe")
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


def _run_resilience(probe_bin, certs, port, mode, *extra_args):
    result = subprocess.run(
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
        timeout=30,
    )
    msg = "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        result.stdout, result.stderr
    )
    assert result.returncode == 0, msg
    _assert_no_sanitizer_reports(result.stderr)
    kv = {}
    for ln in result.stdout.splitlines():
        if "=" in ln:
            k, v = ln.split("=", 1)
            kv[k] = v
    assert kv.get("DONE") == "1", msg
    return result, kv, msg


def test_channel_keepalive_idle_blackhole_reconnects(certs, resilience_probe_bin):
    """Zero calls in flight, the peer blackholes: keepalive (200 ms + 400 ms
    timeout) declares the connection dead and the channel reconnects to
    connection 2 on its own, so the next call succeeds there."""
    with H2TestServer(
        certs.cert, certs.key, mode="blackhole_after_first_rpc"
    ) as srv:
        result, kv, msg = _run_resilience(
            resilience_probe_bin, certs, srv.port, "keepalive_idle_reconnect",
            "--keepalive-time-ms", "200",
            "--keepalive-timeout-ms", "400",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "500",
            "--wait-ms", "2500",
            "--deadline-ms", "5000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)
        conn1 = srv.wait_for_conn_closed(1, timeout=2.0)

    assert kv["CALL1_STATUS"] == str(_OK), msg
    assert kv["CALL1_NAME"] == "ok-conn-1", msg
    # Reconnect happened in the background, before call 2 was issued.
    assert int(kv["CONNECT_ATTEMPTS_BEFORE_CALL2"]) >= 2, msg
    assert kv["STATE_BEFORE_CALL2"] == "READY", msg
    assert kv["CALL2_STATUS"] == str(_OK), msg
    assert kv["CALL2_NAME"] == "ok-conn-2", msg
    assert kv["CONNECT_ATTEMPTS"] == "2", msg

    assert len(conns) == 2, conns
    # The idle connection was probed (unanswered PING) and then torn down by
    # the client, not left hanging.
    assert conn1 is not None, conns
    assert len(conn1["pings"]) >= 1, conn1
    assert conn1["pings"][0] < conns[1]["accepted"], conns
    # Call 2 went over connection 2 only.
    assert len(conn1["requests"]) == 1, conn1
    assert len(conns[1]["requests"]) == 1, conns


def test_channel_goaway_too_many_pings_doubles_keepalive_and_reconnects(
    certs, resilience_probe_bin
):
    """GOAWAY(ENHANCE_YOUR_CALM, "too_many_pings"): the channel doubles its
    effective keepalive_time (250 -> 500 ms), warns on stderr, reconnects, and
    connection 2 sees PINGs at the doubled spacing."""
    with H2TestServer(
        certs.cert, certs.key, mode="goaway_too_many_pings_then_ok"
    ) as srv:
        result, kv, msg = _run_resilience(
            resilience_probe_bin, certs, srv.port, "keepalive_too_many_pings",
            "--keepalive-time-ms", "250",
            "--keepalive-timeout-ms", "600",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "500",
            "--wait-ms", "3000",
            "--deadline-ms", "5000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)

    assert kv["CALL1_STATUS"] == str(_OK), msg
    assert kv["INITIAL_KEEPALIVE_MS"] == "250", msg
    assert kv["EFFECTIVE_KEEPALIVE_MS"] == "500", msg
    assert "too_many_pings" in result.stderr, msg
    assert "doubling keepalive_time from 250 ms to 500 ms" in result.stderr, msg
    assert kv["CALL2_STATUS"] == str(_OK), msg
    assert kv["CALL2_NAME"] == "ok-conn-2", msg
    # Exactly one replacement: no further GOAWAY storm on connection 2.
    assert kv["CONNECT_ATTEMPTS"] == "2", msg

    assert len(conns) == 2, conns
    c1, c2 = conns
    assert c1["goaway_sent"] is not None and len(c1["pings"]) >= 1, c1
    pings = c2["pings"]
    assert len(pings) >= 2, c2
    gaps_ms = [(b - a) * 1000.0 for a, b in zip(pings, pings[1:])]
    # Doubled interval: >= 500 ms nominal (old 250 ms spacing would fail),
    # upper bound generous for sanitizer builds.
    assert all(450 <= g <= 1500 for g in gaps_ms), gaps_ms
    first_after_ready_ms = (pings[0] - c2["remote_settings"]) * 1000.0
    assert first_after_ready_ms >= 450, (first_after_ready_ms, c2)
