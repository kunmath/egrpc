"""egrpc M6 acceptance (design §4.2, §4.4, §5.3, §4.6 prerequisite, §8/M6):
reconnect with backoff, wait-for-ready admission, anti-flap backoff vs
READY-stability reset, GOAWAY drain & replace, and call failure origin.

Drives ``egrpc_resilience_probe`` (tests/integration/resilience_probe_main.cc)
against the multi-connection modes of the hyper-h2 test server
(tests/server/h2_test_server.py). All channel timings are test-sized
(150 ms initial backoff, 400 ms GOAWAY drain, ...) and every timing bound
leaves wide headroom for ASan/UBSan/TSan builds (EGRPC_BUILD_DIR=build-asan /
build-tsan), where execution is several times slower.
"""

import codecs
import os
import select
import socket
import subprocess
import threading
import time

import pytest

from conftest import require_test_binary
from server.h2_test_server import H2TestServer
from server.tls_test_server import TlsTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PROBE_TIMEOUT_S = 30

_OK = 0
_CANCELLED = 1
_DEADLINE_EXCEEDED = 4
_UNAVAILABLE = 14
_H2_CANCEL = 0x8


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


def _probe_cmd(probe_bin, certs, port, mode, *extra_args):
    return [
        probe_bin,
        "--host", "localhost",
        "--port", str(port),
        "--ca", certs.cert,
        "--mode", mode,
        *extra_args,
    ]


def _run_probe(probe_bin, certs, port, mode, *extra_args):
    return subprocess.run(
        _probe_cmd(probe_bin, certs, port, mode, *extra_args),
        capture_output=True,
        text=True,
        timeout=_PROBE_TIMEOUT_S,
    )


def _kv(stdout):
    kv = {}
    for ln in stdout.splitlines():
        if "=" in ln and " " not in ln.split("=", 1)[0]:
            k, v = ln.split("=", 1)
            kv[k] = v
    return kv


def _fail_msg(result):
    return "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        result.stdout, result.stderr
    )


def _checked(result):
    """Asserts a clean run to completion and returns the KEY=VALUE map."""
    assert result.returncode == 0, _fail_msg(result)
    _assert_no_sanitizer_reports(result.stderr)
    kv = _kv(result.stdout)
    assert kv.get("DONE") == "1", _fail_msg(result)
    return kv


def _read_lines_until(proc, predicate, timeout_s, acc=None):
    """Reads ``proc``'s stdout until a complete line satisfies ``predicate``.

    Never blocks past ``timeout_s``: waits with select() on the raw pipe fd and
    reads whatever bytes are available with os.read(), so a probe that wedges
    without printing (or without a trailing newline) cannot hang the suite.
    ``acc`` is an optional list of already-read text chunks to continue from.

    Returns all stdout text read so far (it may extend past the matching
    line; callers must prepend it to anything read later, e.g. by
    ``proc.communicate()``). On timeout or EOF without a match, kills and
    reaps ``proc`` and raises AssertionError carrying the captured stdout and
    stderr. Only use on a Popen whose stdout has not been read through its
    buffered file object.
    """
    acc = [] if acc is None else acc
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    fd = proc.stdout.fileno()
    deadline = time.monotonic() + timeout_s
    scanned = 0  # chars of "".join(acc) already split into complete lines
    eof = False
    while True:
        text = "".join(acc)
        while True:
            nl = text.find("\n", scanned)
            if nl < 0:
                break
            line = text[scanned:nl + 1]
            scanned = nl + 1
            if predicate(line):
                return text
        remaining = deadline - time.monotonic()
        if eof or remaining <= 0:
            break
        ready, _, _ = select.select([fd], [], [], remaining)
        if not ready:
            continue  # loop re-checks the deadline
        chunk = os.read(fd, 65536)
        if not chunk:
            eof = True
            acc.append(decoder.decode(b"", final=True))
            continue
        acc.append(decoder.decode(chunk))

    reason = "EOF" if eof else "timeout after {:.1f}s".format(timeout_s)
    if proc.poll() is None:
        proc.kill()
    try:
        rest, err = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        rest, err = "", "<stderr unavailable: probe did not exit after kill>"
    raise AssertionError(
        "probe output never matched ({})\n--- stdout ---\n{}{}\n--- stderr ---\n{}".format(
            reason, "".join(acc), rest or "", err or ""
        )
    )


def _reserve_port():
    """A free 127.0.0.1 port with nothing listening on it (bound then closed)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# --- Reconnect ----------------------------------------------------------------


def test_reconnect_after_server_drop(certs, probe_bin):
    """Connection dropped mid-call: the in-flight call fails UNAVAILABLE with a
    local origin, the channel reconnects, and a wait-for-ready call succeeds
    on connection 2."""
    with H2TestServer(certs.cert, certs.key, mode="drop_first_then_ok") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "reconnect_after_drop",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "500",
            "--deadline-ms", "5000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)

    kv = _checked(result)
    assert kv["CALL1_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL1_ORIGIN"] == "local", _fail_msg(result)
    assert kv["CALL2_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL2_ORIGIN"] == "server", _fail_msg(result)
    # Served by the replacement connection, not the dropped one.
    assert kv["CALL2_NAME"] == "ok-conn-2", _fail_msg(result)
    assert kv["CONNECT_ATTEMPTS"] == "2", _fail_msg(result)
    assert len(conns) == 2, conns
    assert len(conns[0]["requests"]) == 1 and len(conns[1]["requests"]) == 1, conns


def test_reconnect_after_server_restart(certs, probe_bin):
    """The server process goes away entirely and comes back on the same port:
    the channel keeps retrying with backoff and recovers on its own."""
    port = _reserve_port()
    srv = H2TestServer(certs.cert, certs.key, mode="multi_conn_ok", port=port).start()
    proc = subprocess.Popen(
        _probe_cmd(
            probe_bin, certs, port, "reconnect_after_restart",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "400",
            "--deadline-ms", "10000",
        ),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    srv2 = None
    first_out = ""
    try:
        first_out = _read_lines_until(
            proc, lambda ln: ln.startswith("READY_FOR_RESTART="), _PROBE_TIMEOUT_S
        )
        first = _kv(first_out)
        assert first.get("READY_FOR_RESTART") == "1", first_out
        assert first["CALL1_STATUS"] == str(_OK), first_out
        assert first["CALL1_NAME"] == "ok-conn-1", first_out

        # Kill server 1 (closes the live connection), stay down long enough
        # for at least one refused reconnect, then restart on the same port.
        srv.stop()
        time.sleep(0.6)
        srv2 = H2TestServer(
            certs.cert, certs.key, mode="multi_conn_ok", port=port
        ).start()
        out, err = proc.communicate("go\n", timeout=_PROBE_TIMEOUT_S)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
        srv.stop()
        if srv2 is not None:
            srv2.stop()

    full_out = first_out + out
    msg = "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(full_out, err)
    assert proc.returncode == 0, msg
    _assert_no_sanitizer_reports(err)
    kv = _kv(full_out)
    assert kv.get("DONE") == "1", msg
    assert kv["CALL2_STATUS"] == str(_OK), msg
    # First connection on the restarted server.
    assert kv["CALL2_NAME"] == "ok-conn-1", msg
    # Initial connect + at least one refused attempt while down + the
    # successful one against the restarted server.
    assert int(kv["CONNECT_ATTEMPTS"]) >= 3, msg
    assert len(srv2.connections) == 1, srv2.connections


# --- Wait-for-ready admission (§4.4) ------------------------------------------


def test_wait_for_ready_admission_matrix(certs, probe_bin):
    """In TRANSIENT_FAILURE: fail-fast fails immediately; wait-for-ready
    queues and honors its deadline and TryCancel (both local origin)."""
    port = _reserve_port()  # nothing listens: every connect is refused
    result = _run_probe(
        probe_bin, certs, port, "wait_for_ready_matrix",
        # Long backoff keeps the channel in TRANSIENT_FAILURE throughout.
        "--initial-backoff-ms", "5000",
        "--max-backoff-ms", "5000",
        "--deadline-ms", "400",
        "--cancel-after-ms", "150",
    )
    kv = _checked(result)

    assert kv["CALL1_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL1_ORIGIN"] == "local", _fail_msg(result)
    assert kv["STATE_AFTER_CALL1"] == "TRANSIENT_FAILURE", _fail_msg(result)

    assert kv["FF_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["FF_ORIGIN"] == "local", _fail_msg(result)
    # Immediate: no queueing behind the 5 s backoff.
    assert int(kv["FF_ELAPSED_MS"]) < 250, _fail_msg(result)

    assert kv["WFR_DEADLINE_STATUS"] == str(_DEADLINE_EXCEEDED), _fail_msg(result)
    assert kv["WFR_DEADLINE_ORIGIN"] == "local", _fail_msg(result)
    wd = int(kv["WFR_DEADLINE_ELAPSED_MS"])
    assert 380 <= wd <= 2000, _fail_msg(result)

    assert kv["WFR_CANCEL_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["WFR_CANCEL_ORIGIN"] == "local", _fail_msg(result)
    wc = int(kv["WFR_CANCEL_ELAPSED_MS"])
    assert 140 <= wc <= 1500, _fail_msg(result)

    # Still backing off: only the initial attempt was made.
    assert kv["STATE_AT_END"] == "TRANSIENT_FAILURE", _fail_msg(result)
    assert kv["CONNECT_ATTEMPTS"] == "1", _fail_msg(result)


def test_wait_for_ready_waits_for_late_server(certs, probe_bin):
    """A wait-for-ready call issued before the server binds rides through
    refused connects + backoff and succeeds once the server comes up."""
    port = _reserve_port()
    proc = subprocess.Popen(
        _probe_cmd(
            probe_bin, certs, port, "wait_for_ready_late_server",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "300",
            "--deadline-ms", "10000",
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    srv = None
    head = ""
    try:
        # Deterministic: the probe reports once a connect has been attempted
        # and refused (channel in TRANSIENT_FAILURE) — only then bind.
        head = _read_lines_until(
            proc, lambda ln: ln.startswith("CONNECT_ATTEMPTED="), _PROBE_TIMEOUT_S
        )
        assert _kv(head).get("CONNECT_ATTEMPTED") == "1", head
        srv = H2TestServer(certs.cert, certs.key, mode="multi_conn_ok", port=port).start()
        out, err = proc.communicate(timeout=_PROBE_TIMEOUT_S)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
        if srv is not None:
            srv.stop()

    out = head + out
    msg = "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(out, err)
    assert proc.returncode == 0, msg
    _assert_no_sanitizer_reports(err)
    kv = _kv(out)
    assert kv.get("DONE") == "1", msg
    assert kv["STATUS"] == str(_OK), msg
    assert kv["ORIGIN"] == "server", msg
    assert kv["NAME"] == "ok-conn-1", msg
    # >= 2 attempts: at least one refused connect before the server bound,
    # i.e. the call genuinely waited through TRANSIENT_FAILURE.
    assert int(kv["CONNECT_ATTEMPTS"]) >= 2, msg


def test_shim_wait_for_ready_and_channel_args_reconnect(certs, probe_bin):
    """End to end through the unmodified generated stub: GRPC_ARG_*_RECONNECT_
    BACKOFF_MS channel args + ClientContext::set_wait_for_ready(true)."""
    with H2TestServer(certs.cert, certs.key, mode="drop_first_then_ok") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "shim_wait_for_ready_reconnect",
            # Long enough (even with the shim's default ±20 % jitter) that the
            # fail-fast control call lands inside the backoff window.
            "--initial-backoff-ms", "1000",
            "--max-backoff-ms", "1000",
            "--connect-timeout-ms", "5000",
            "--deadline-ms", "10000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)

    kv = _checked(result)
    assert kv["CALL1_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL1_LOCAL"] == "1", _fail_msg(result)
    # Fail-fast while in TRANSIENT_FAILURE: immediate local UNAVAILABLE.
    assert kv["CALLFF_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALLFF_LOCAL"] == "1", _fail_msg(result)
    assert int(kv["CALLFF_ELAPSED_MS"]) < 500, _fail_msg(result)
    # set_wait_for_ready(true) waits out the backoff and succeeds.
    assert kv["CALL2_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL2_LOCAL"] == "0", _fail_msg(result)
    assert kv["CALL2_NAME"] == "ok-conn-2", _fail_msg(result)
    assert len(conns) == 2, conns


# --- Backoff: anti-flap vs READY-stability reset (§5.3) ------------------------


def _reconnect_gaps_ms(conns):
    """Gap between connection i closing and connection i+1 being accepted."""
    gaps = []
    for prev, cur in zip(conns, conns[1:]):
        assert prev["closed"] is not None, conns
        gaps.append((cur["accepted"] - prev["closed"]) * 1000.0)
    return gaps


def test_backoff_anti_flap_growth_vs_ready_stability_reset(certs, probe_bin):
    # Part A — anti-flap: each connection reaches READY but drops after ~50 ms,
    # far below ready_stability_threshold, so backoff must NOT reset:
    # delays 150, 240, 384 ms (multiplier 1.6, jitter 0).
    with H2TestServer(
        certs.cert, certs.key, mode="flap_ready_drop", hold_ms=50
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "backoff_observe",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "10000",
            "--multiplier", "1.6",
            "--jitter", "0",
            "--ready-stability-ms", "5000",
            "--wait-ms", "2500",
            "--deadline-ms", "2000",
        )
        conns_a = srv.connections
    kv = _checked(result)
    assert len(conns_a) >= 4, (conns_a, _fail_msg(result))
    gaps_a = _reconnect_gaps_ms(conns_a[:4])
    msg_a = "anti-flap gaps (ms): {}{}".format(gaps_a, _fail_msg(result))
    # Timers never fire early: each gap is at least the scheduled backoff.
    assert gaps_a[0] >= 140, msg_a
    assert gaps_a[1] >= 228, msg_a
    assert gaps_a[2] >= 365, msg_a
    # Strict growth (expected increments 90 ms and 144 ms).
    assert gaps_a[1] > gaps_a[0] + 45, msg_a
    assert gaps_a[2] > gaps_a[1] + 70, msg_a
    assert int(kv["CONNECT_ATTEMPTS"]) >= 4, msg_a

    # Part B — stability reset: each connection holds READY ~350 ms, above the
    # 250 ms threshold, so every failure restarts from initial_backoff.
    with H2TestServer(
        certs.cert, certs.key, mode="flap_ready_drop", hold_ms=350
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "backoff_observe",
            "--initial-backoff-ms", "150",
            "--max-backoff-ms", "10000",
            "--multiplier", "1.6",
            "--jitter", "0",
            "--ready-stability-ms", "250",
            "--wait-ms", "3000",
            "--deadline-ms", "2000",
        )
        conns_b = srv.connections
    kv = _checked(result)
    assert len(conns_b) >= 4, (conns_b, _fail_msg(result))
    gaps_b = _reconnect_gaps_ms(conns_b[:4])
    msg_b = "stability-reset gaps (ms): {}{}".format(gaps_b, _fail_msg(result))
    for g in gaps_b:
        assert g >= 140, msg_b
        # Without the reset the 3rd gap would be >= 384 ms.
        assert g < 330, msg_b
    # Flat, not growing like Part A.
    assert gaps_b[2] - gaps_b[0] < 100, msg_b


# --- GOAWAY drain & replace (§4.2) ---------------------------------------------


def test_goaway_split_drain_and_replace(certs, probe_bin):
    """GOAWAY(NO_ERROR, last_stream_id=1) with streams 1 and 3 open: stream 1
    drains to OK, stream 3 fails UNAVAILABLE immediately, and the connection is
    replaced for subsequent calls."""
    drain_delay_ms = 300
    with H2TestServer(
        certs.cert, certs.key, mode="goaway_split_drain", drain_delay_ms=drain_delay_ms
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "goaway_split_drain",
            "--initial-backoff-ms", "150",
            "--goaway-drain-ms", "5000",
            "--deadline-ms", "5000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)

    kv = _checked(result)
    assert kv["CALL1_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL1_NAME"] == "ok-conn-1", _fail_msg(result)
    assert kv["CALL2_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    c1 = int(kv["CALL1_ELAPSED_MS"])
    c2 = int(kv["CALL2_ELAPSED_MS"])
    # Stream 3 failed on GOAWAY receipt, well before stream 1's delayed reply.
    assert c2 + drain_delay_ms // 2 < c1, _fail_msg(result)
    # ...and stream 1 was not cut short by the (5 s) drain timeout either.
    assert c1 < 4000, _fail_msg(result)
    assert kv["CALL3_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL3_NAME"] == "ok-conn-2", _fail_msg(result)
    assert kv["CONNECT_ATTEMPTS"] == "2", _fail_msg(result)

    assert len(conns) == 2, conns
    assert [sid for sid, _ in conns[0]["requests"]] == [1, 3], conns
    assert conns[0]["goaway_sent"] is not None, conns
    # No new stream was opened on the draining connection.
    assert len(conns[1]["requests"]) == 1, conns


def test_goaway_drain_timeout_forces_replace(certs, probe_bin):
    """A stream <= last_stream_id that never completes is failed UNAVAILABLE
    when goaway_drain_timeout (400 ms) expires; the connection is replaced."""
    with H2TestServer(certs.cert, certs.key, mode="goaway_stall_drain") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "goaway_drain_timeout",
            "--initial-backoff-ms", "150",
            "--goaway-drain-ms", "400",
            "--deadline-ms", "5000",
        )
        conns = srv.wait_for_connections(2, timeout=2.0)
        closed1 = srv.wait_for_conn_closed(1, timeout=2.0)

    kv = _checked(result)
    assert kv["CALL1_STATUS"] == str(_UNAVAILABLE), _fail_msg(result)
    assert kv["CALL1_ORIGIN"] == "local", _fail_msg(result)
    c1 = int(kv["CALL1_ELAPSED_MS"])
    # Drain timer (400 ms) — far below the call's own 10 s deadline.
    assert 380 <= c1 <= 3000, _fail_msg(result)
    assert kv["CALL2_STATUS"] == str(_OK), _fail_msg(result)
    assert kv["CALL2_NAME"] == "ok-conn-2", _fail_msg(result)
    assert kv["CONNECT_ATTEMPTS"] == "2", _fail_msg(result)
    assert len(conns) == 2, conns
    assert closed1 is not None, "client never closed the drained connection"


# --- Failure origin -------------------------------------------------------------


def test_cancel_failure_origin_local_vs_server(certs, probe_bin):
    """CANCELLED from a local TryCancel is tagged local; CANCELLED from the
    server (grpc-status trailers or RST_STREAM(CANCEL)) is tagged server —
    both on the core channel and through the public shim."""
    with H2TestServer(certs.cert, certs.key, mode="server_cancel") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "cancel_origin_check",
            "--deadline-ms", "5000",
            "--cancel-after-ms", "150",
        )
        resets = srv.wait_for_resets(1, timeout=2.0)

    kv = _checked(result)
    assert kv["LOCAL_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["LOCAL_ORIGIN"] == "local", _fail_msg(result)
    assert kv["SERVER_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["SERVER_ORIGIN"] == "server", _fail_msg(result)
    assert kv["SERVER_MESSAGE"] == "server-cancelled", _fail_msg(result)
    assert kv["SERVER_RST_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["SERVER_RST_ORIGIN"] == "server", _fail_msg(result)

    assert kv["SHIM_SERVER_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["SHIM_SERVER_LOCAL"] == "0", _fail_msg(result)
    assert kv["SHIM_LOCAL_READ_OK"] == "0", _fail_msg(result)
    assert kv["SHIM_LOCAL_STATUS"] == str(_CANCELLED), _fail_msg(result)
    assert kv["SHIM_LOCAL_LOCAL"] == "1", _fail_msg(result)

    # The local cancel reached the wire as RST_STREAM(CANCEL) on stream 1.
    assert (1, _H2_CANCEL) in resets, resets


# --- Multi-address TLS fallback (§4.1, Fix #5) ----------------------------------


class _PlainTcpRejecter:
    """Plain-TCP listener on 127.0.0.1 that answers every connection with an
    HTTP/1.1 400 and closes: the TCP connect succeeds, the TLS handshake is
    guaranteed to fail. Counts accepted connections."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.1)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._accepted = 0
        self._thread = threading.Thread(target=self._loop, daemon=True)

    @property
    def accepted(self):
        with self._lock:
            return self._accepted

    def _loop(self):
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with self._lock:
                self._accepted += 1
            try:
                conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            except OSError:
                pass
            finally:
                conn.close()

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._thread.join(timeout=5.0)
        self._sock.close()


def _run_tls_fallback(probe_bin, certs, ports):
    return _checked(
        _run_probe(
            probe_bin, certs, ports[-1], "tls_addr_fallback",
            "--addr-ports", ",".join(str(p) for p in ports),
            "--deadline-ms", "10000",
        )
    )


def test_tls_fallback_past_plain_tcp_listener(certs, probe_bin):
    """Address 1 accepts TCP but speaks HTTP/1.1 (TLS handshake fails):
    TlsSocket must fall back to address 2 (h2 TLS server) and connect."""
    with _PlainTcpRejecter() as bad, H2TestServer(
        certs.cert, certs.key, mode="multi_conn_ok"
    ) as good:
        kv = _run_tls_fallback(probe_bin, certs, [bad.port, good.port])
        bad_accepted = bad.accepted
        conns = good.wait_for_connections(1, timeout=2.0)

    assert kv["CONNECTED"] == "1", kv
    assert kv["TLS_STATE"] == "CONNECTED", kv
    assert kv["ALPN"] == "h2", kv
    assert kv["PEER_PORT"] == str(good.port), kv
    # Address 1 was genuinely tried first (not skipped).
    assert bad_accepted == 1, bad_accepted
    assert len(conns) == 1, conns


def test_tls_fallback_past_http11_alpn_server(certs, probe_bin):
    """Address 1 completes a TLS handshake but negotiates no h2 (http/1.1-only
    ALPN): an ALPN failure must also fall back to address 2."""
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=["http/1.1"]) as bad, \
            H2TestServer(certs.cert, certs.key, mode="multi_conn_ok") as good:
        kv = _run_tls_fallback(probe_bin, certs, [bad.port, good.port])
        conns = good.wait_for_connections(1, timeout=2.0)

    assert kv["CONNECTED"] == "1", kv
    assert kv["ALPN"] == "h2", kv
    assert kv["PEER_PORT"] == str(good.port), kv
    assert len(conns) == 1, conns


def test_tls_fallback_all_fail_reports_tls_error(certs, probe_bin):
    """Every address fails: an earlier address's ALPN failure is reported in
    preference to the last address's plain TCP connect() refusal."""
    refused = _reserve_port()
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=["http/1.1"]) as bad:
        kv = _run_tls_fallback(probe_bin, certs, [bad.port, refused])

    assert kv["CONNECTED"] == "0", kv
    assert kv["TLS_STATE"] == "FAILED", kv
    assert kv["ALPN"] == "-", kv
    assert "did not negotiate HTTP/2 via ALPN" in kv["ERROR"], kv
    assert "connect() failed" not in kv["ERROR"], kv
