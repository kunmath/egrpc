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

from server.h2_test_server import H2TestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture(scope="session")
def keepalive_probe_bin():
    """Absolute path to the compiled egrpc_h2_keepalive probe binary."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(
        _REPO_ROOT, "build"
    )
    path = os.path.join(build_dir, "tests", "integration", "egrpc_h2_keepalive")
    if not os.path.isfile(path):
        pytest.skip(
            "probe binary not found at {}; build the project first: "
            "cmake -S . -B build && cmake --build build -j".format(path)
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
