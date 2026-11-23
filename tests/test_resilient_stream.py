"""egrpc M7 acceptance (design §4.6, §8/M7): egrpc::resilient_stream with
reconcile-on-reconnect, the native ChannelConfig API (mTLS, cellular profile)
and the LogSink API.

Drives ``egrpc_resilient_subscribe`` (examples/resilient_subscribe/main.cc)
against the upstream-grpcio RouteGuide server
(tests/server/route_guide_server.py, ListFeatures modes 3/7/8/9) and the
hyper-h2 test server. Timings are test-sized with wide headroom for ASan/TSan
builds (EGRPC_BUILD_DIR=build-asan / build-tsan).
"""

import codecs
import os
import select
import subprocess
import tempfile
import time

import pytest

from conftest import require_test_binary
from server.h2_test_server import H2TestServer
from server.route_guide_server import RouteGuideTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_RUN_TIMEOUT_S = 60

_OK = 0
_CANCELLED = 1
_PERMISSION_DENIED = 7

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
def sub_bin():
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(
        build_dir, "examples", "resilient_subscribe", "egrpc_resilient_subscribe"
    )
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


def _cmd(sub_bin, port, ca, *extra):
    return [sub_bin, "--host", "localhost", "--port", str(port), "--ca", ca, *extra]


def _kv(stdout):
    kv = {}
    for ln in stdout.splitlines():
        if "=" in ln and " " not in ln.split("=", 1)[0]:
            k, v = ln.split("=", 1)
            kv[k] = v
    return kv


def _msg(stdout, stderr):
    return "\n--- stdout ---\n{}\n--- stderr ---\n{}".format(stdout, stderr)


def _run(sub_bin, port, ca, *extra, timeout=_RUN_TIMEOUT_S):
    r = subprocess.run(
        _cmd(sub_bin, port, ca, *extra),
        capture_output=True,
        text=True,
        timeout=timeout,
        stdin=subprocess.DEVNULL,
    )
    msg = _msg(r.stdout, r.stderr)
    assert r.returncode == 0, msg
    _assert_no_sanitizer_reports(r.stderr)
    kv = _kv(r.stdout)
    assert kv.get("DONE") == "1", msg
    return r, kv, msg


class _LineReader:
    """Incremental, deadline-bounded line reader over a Popen's stdout pipe."""

    def __init__(self, proc, stderr_file):
        self._proc = proc
        self._stderr_file = stderr_file
        self._fd = proc.stdout.fileno()
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._buf = ""
        self.text = ""
        self.eof = False

    def stderr(self):
        self._stderr_file.flush()
        with open(self._stderr_file.name, "r", errors="replace") as fh:
            return fh.read()

    def fail(self, what):
        if self._proc.poll() is None:
            self._proc.kill()
        self._proc.wait()
        self._drain()
        raise AssertionError("timed out waiting for {}{}".format(
            what, _msg(self.text, self.stderr())))

    def _drain(self):
        while not self.eof:
            ready, _, _ = select.select([self._fd], [], [], 1.0)
            if not ready:
                return
            self._feed()

    def _feed(self):
        chunk = os.read(self._fd, 65536)
        if not chunk:
            self.eof = True
            s = self._decoder.decode(b"", final=True)
        else:
            s = self._decoder.decode(chunk)
        self._buf += s
        self.text += s

    def wait_for(self, predicate, timeout_s, what):
        deadline = time.monotonic() + timeout_s
        while True:
            while "\n" in self._buf:
                line, self._buf = self._buf.split("\n", 1)
                if predicate(line):
                    return line
            remaining = deadline - time.monotonic()
            if self.eof or remaining <= 0:
                self.fail(what)
            ready, _, _ = select.select([self._fd], [], [], remaining)
            if ready:
                self._feed()

    def finish(self, timeout_s):
        try:
            self._proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            self.fail("process exit")
        self._drain()
        return self.text, self.stderr()


def _event_gen(line):
    if not line.startswith("EVENT "):
        return None
    for tok in line.split():
        if tok.startswith("gen="):
            return int(tok[4:])
    return None


def _start_streaming(sub_bin, port, ca, *extra):
    stderr_file = tempfile.NamedTemporaryFile(prefix="egrpc_sub_", suffix=".err")
    proc = subprocess.Popen(
        _cmd(sub_bin, port, ca, "--stop-on-stdin", *extra),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr_file,
    )
    return proc, _LineReader(proc, stderr_file)


def _stop_and_collect(proc, reader, timeout_s=20):
    try:
        proc.stdin.write(b"stop\n")
        proc.stdin.flush()
        proc.stdin.close()
    except BrokenPipeError:
        pass
    stdout, stderr = reader.finish(timeout_s)
    msg = _msg(stdout, stderr)
    assert proc.returncode == 0, msg
    _assert_no_sanitizer_reports(stderr)
    kv = _kv(stdout)
    assert kv.get("DONE") == "1", msg
    return kv, msg


_FAST_RETRY = (
    "--initial-backoff-ms", "50",
    "--max-backoff-ms", "200",
    "--initial-retry-delay-ms", "20",
    "--max-retry-delay-ms", "100",
    "--delay-ms", "20",
)


def _restart_cycle(sub_bin, certs, restarts, *extra):
    srv = RouteGuideTestServer(certs.cert, certs.key, server_generation=0).start()
    port = srv.port
    proc, reader = _start_streaming(
        sub_bin, port, certs.cert, "--list-mode", "7", *_FAST_RETRY, *extra
    )
    overall = time.monotonic() + 120.0
    try:
        reader.wait_for(lambda ln: _event_gen(ln) == 0, 20.0, "EVENT gen=0")
        for g in range(restarts):
            assert time.monotonic() < overall, "restart loop exceeded 120 s"
            srv.stop(grace=0)
            srv = RouteGuideTestServer(
                certs.cert, certs.key, port=port, server_generation=g + 1
            ).start()
            n = g + 1 + (1 if "--fire-on-first-connect" in extra else 0)
            reader.wait_for(
                lambda ln, n=n: ln.startswith("RECONNECTED n={} ".format(n)),
                20.0,
                "RECONNECTED n={}".format(n),
            )
            reader.wait_for(
                lambda ln, g=g: _event_gen(ln) == g + 1,
                20.0,
                "EVENT gen={}".format(g + 1),
            )
        return _stop_and_collect(proc, reader)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        srv.stop(grace=0)


def test_resilient_stream_survives_20_server_restarts(sub_bin, certs):
    kv, msg = _restart_cycle(sub_bin, certs, 20, "--reconcile-unary")
    assert kv["RECONNECTS"] == "20", msg
    assert kv["RECONCILE_UNARY_OK"] == "20", msg
    assert kv["MISSED"] == "0", msg
    assert kv["DUPLICATES"] == "0", msg
    assert kv["GENERATIONS"] == "21", msg
    assert kv["FINAL_STATUS"] == str(_CANCELLED), msg


def test_resilient_stream_fire_on_first_connect(sub_bin, certs):
    kv, msg = _restart_cycle(sub_bin, certs, 1, "--fire-on-first-connect")
    assert kv["RECONNECTS"] == "2", msg
    assert kv["MISSED"] == "0", msg
    assert kv["DUPLICATES"] == "0", msg
    assert kv["GENERATIONS"] == "2", msg
    assert kv["FINAL_STATUS"] == str(_CANCELLED), msg


def test_resilient_stream_terminal_error_does_not_retry(sub_bin, certs):
    with RouteGuideTestServer(certs.cert, certs.key) as srv:
        _, kv, msg = _run(sub_bin, srv.port, certs.cert, "--list-mode", "3",
                          *_FAST_RETRY)
    assert kv["ATTEMPTS"] == "1", msg
    assert kv["RECONNECTS"] == "0", msg
    assert kv["FINAL_STATUS"] == str(_PERMISSION_DENIED), msg


def test_resilient_stream_retries_server_cancelled(sub_bin, certs):
    with RouteGuideTestServer(certs.cert, certs.key) as srv:
        _, kv, msg = _run(sub_bin, srv.port, certs.cert, "--list-mode", "8",
                          "--fail-count", "1", "--target-events", "3",
                          *_FAST_RETRY)
    assert kv["ATTEMPTS"] == "2", msg
    # Attempt 1 never sent initial metadata, so it never counted as connected.
    assert kv["RECONNECTS"] == "0", msg
    assert kv["RECEIVED"] == "3", msg
    assert kv["MISSED"] == "0", msg
    assert kv["FINAL_STATUS"] == str(_OK), msg


def test_resilient_stream_per_attempt_deadline_retries(sub_bin, certs):
    with RouteGuideTestServer(certs.cert, certs.key) as srv:
        _, kv, msg = _run(sub_bin, srv.port, certs.cert, "--list-mode", "9",
                          "--target-events", "3", "--per-attempt-deadline-ms",
                          "400", *_FAST_RETRY)
    assert kv["ATTEMPTS"] == "2", msg
    assert kv["RECEIVED"] == "3", msg
    assert kv["FINAL_STATUS"] == str(_OK), msg


def test_resilient_stream_stop_unblocks_mid_stream(sub_bin, certs):
    with RouteGuideTestServer(certs.cert, certs.key) as srv:
        start = time.monotonic()
        # 1 s between messages: stop() must interrupt a blocked Read().
        _, kv, msg = _run(sub_bin, srv.port, certs.cert, "--list-mode", "7",
                          "--delay-ms", "1000", "--stop-after-events", "2",
                          timeout=30)
        elapsed = time.monotonic() - start
    assert kv["RECEIVED"] in ("2", "3"), msg
    assert kv["FINAL_STATUS"] == str(_CANCELLED), msg
    assert elapsed < 15.0, (elapsed, msg)


def _openssl(*args):
    subprocess.run(["openssl", *args], check=True, capture_output=True)


@pytest.fixture(scope="module")
def mtls_certs(tmp_path_factory):
    d = tmp_path_factory.mktemp("mtls")
    p = {k: str(d / k) for k in (
        "ca.pem", "ca.key", "srv.key", "srv.csr", "srv.pem", "srv.ext",
        "cli.key", "cli.csr", "cli.pem")}
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
             "-keyout", p["ca.key"], "-out", p["ca.pem"], "-subj", "/CN=egrpc-test-ca")
    with open(p["srv.ext"], "w") as fh:
        fh.write("subjectAltName=DNS:localhost,IP:127.0.0.1\n")
    for who, cn in (("srv", "localhost"), ("cli", "egrpc-test-client")):
        _openssl("req", "-newkey", "rsa:2048", "-nodes",
                 "-keyout", p[who + ".key"], "-out", p[who + ".csr"],
                 "-subj", "/CN=" + cn)
        extra = ["-extfile", p["srv.ext"]] if who == "srv" else []
        _openssl("x509", "-req", "-in", p[who + ".csr"], "-CA", p["ca.pem"],
                 "-CAkey", p["ca.key"], "-CAcreateserial", "-days", "2",
                 "-out", p[who + ".pem"], *extra)
    return p


def test_channel_config_mtls_and_cellular_profile(sub_bin, mtls_certs):
    p = mtls_certs
    with RouteGuideTestServer(p["srv.pem"], p["srv.key"], root_ca_file=p["ca.pem"]) as srv:
        _, kv, msg = _run(sub_bin, srv.port, p["ca.pem"], "--cellular",
                          "--client-cert", p["cli.pem"], "--client-key", p["cli.key"],
                          "--reconcile-unary", "--fire-on-first-connect",
                          "--list-mode", "8", "--target-events", "3")
        assert kv["RECONNECTS"] == "1", msg
        assert kv["RECONCILE_UNARY_OK"] == "1", msg
        assert kv["RECEIVED"] == "3", msg
        assert kv["FINAL_STATUS"] == str(_OK), msg

        _, kv, msg = _run(sub_bin, srv.port, p["ca.pem"], "--cellular",
                          "--client-cert", p["cli.pem"], "--client-key", p["cli.key"],
                          "--mode", "unary")
        assert kv["UNARY_STATUS"] == str(_OK), msg

        # Without a client certificate the server rejects the handshake.
        _, kv, msg = _run(sub_bin, srv.port, p["ca.pem"], "--cellular",
                          "--mode", "unary")
        assert kv["UNARY_STATUS"] != str(_OK), msg


def test_log_sink_captures_enhance_your_calm(sub_bin, certs):
    with H2TestServer(certs.cert, certs.key, mode="goaway_too_many_pings_then_ok") as srv:
        r, kv, msg = _run(sub_bin, srv.port, certs.cert, "--mode", "keepalive_log",
                          "--capture-logs", "--keepalive-time-ms", "250",
                          "--keepalive-timeout-ms", "600",
                          "--initial-backoff-ms", "150", "--max-backoff-ms", "500",
                          "--wait-ms", "3000")
    assert kv["CALL1_STATUS"] == str(_OK), msg
    assert kv["CALL2_STATUS"] == str(_OK), msg
    logs = [ln for ln in r.stdout.splitlines()
            if ln.startswith("LOG level=WARN comp=keepalive ")]
    assert any("too_many_pings" in ln and
               "doubling keepalive_time from 250 ms to 500 ms" in ln
               for ln in logs), msg
    # The custom sink replaced the default stderr sink.
    assert "doubling keepalive_time" not in r.stderr, msg
