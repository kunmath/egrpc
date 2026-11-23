"""M5 acceptance (design §5.4, §5.8, §8/M5): server-streaming ClientReader,
Read-driven HTTP/2 backpressure, early Finish / destructor cancel-and-discard,
and concurrent mixed traffic against an unmodified upstream grpcio TLS server.
"""

import os
import subprocess

import pytest

from conftest import require_test_binary
from server.h2_test_server import H2TestServer
from server.route_guide_server import RouteGuideTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_TIMEOUT_S = 30
_H2_PROTOCOL_ERROR = 0x1
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
def probe_bin():
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(build_dir, "tests", "integration", "egrpc_stream_probe")
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


def _run_probe(probe_bin, certs, port, *extra_args):
    return subprocess.run(
        [
            probe_bin,
            "--host", "localhost",
            "--port", str(port),
            "--ca-file", certs.cert,
            *extra_args,
        ],
        capture_output=True,
        text=True,
        timeout=_CLIENT_TIMEOUT_S,
    )


def _parse_kv_lines(stdout):
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


def test_list_features_streams_all_messages_and_metadata(
    shim_bin, certs, route_guide_server
):
    """Happy path: WaitForInitialMetadata, NextMessageSize, 5 features, -bin metadata."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "0",
        "--wait-init", "1",
        "--check-next-size", "1",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "INIT_METADATA_READY" in lines, _fail_msg(result)
    assert "INITIAL_EARLY egrpc-stream-init=siv1" in lines, _fail_msg(result)
    assert "INITIAL_EARLY egrpc-stream-init-bin=\\xde\\xad" in lines, _fail_msg(result)
    for i in range(5):
        assert (
            "FEATURE name=feat-{} latitude={} longitude={}".format(
                i, 100 + i, 200 + i
            )
            in lines
        ), _fail_msg(result)
    assert "LISTFEATURES code=0 messages=5 next_size_ok=1 message=" in lines, _fail_msg(
        result
    )
    assert "TRAILING egrpc-stream-trail=stv1" in lines, _fail_msg(result)
    assert "TRAILING egrpc-stream-trail-bin=\\xbe\\xef" in lines, _fail_msg(result)


def test_list_features_empty_stream(shim_bin, certs, route_guide_server):
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "1",
        "--wait-init", "1",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "LISTFEATURES code=0 messages=0 next_size_ok=1 message=" in lines, _fail_msg(
        result
    )
    assert "INITIAL egrpc-stream-init=empty" in lines, _fail_msg(result)
    assert "TRAILING egrpc-stream-trail=empty-ok" in lines, _fail_msg(result)


def test_list_features_error_mid_stream(shim_bin, certs, route_guide_server):
    """3 messages arrive before server aborts with RESOURCE_EXHAUSTED (8)."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "2",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    for i in range(3):
        assert (
            "FEATURE name=ok-{} latitude={} longitude={}".format(i, i, i) in lines
        ), _fail_msg(result)
    assert (
        "LISTFEATURES code=8 messages=3 next_size_ok=1 message=mid-stream boom"
        in lines
    ), _fail_msg(result)
    assert "TRAILING egrpc-err-trail=et1" in lines, _fail_msg(result)


def test_list_features_trailers_only_error(shim_bin, certs, route_guide_server):
    """Trailers-only PERMISSION_DENIED (7) with 0 messages."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "3",
        "--wait-init", "1",
    )
    assert result.returncode == 1, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "INIT_METADATA_READY" in lines, _fail_msg(result)
    assert (
        "LISTFEATURES code=7 messages=0 next_size_ok=1 message=stream denied" in lines
    ), _fail_msg(result)
    assert "TRAILING egrpc-to-trail=to1" in lines, _fail_msg(result)


def test_list_features_draining_exceeds_queue_depth(
    shim_bin, certs, route_guide_server
):
    """40 small messages (> default max_queued_messages=16) drain cleanly without loss."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "listfeatures",
        "--lo-lat", "4",
        "--hi-lat", "40",
    )
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "LISTFEATURES code=0 messages=40 next_size_ok=1 message=" in lines, _fail_msg(
        result
    )
    assert "FEATURE name=drain-0 latitude=0 longitude=0" in lines, _fail_msg(result)
    assert "FEATURE name=drain-39 latitude=39 longitude=0" in lines, _fail_msg(result)


def test_slow_consumer_backpressure_bounds_memory(
    probe_bin, certs, route_guide_server
):
    """M5 acceptance: fast producer (60 x 4 KiB = 240 KiB) vs slow consumer with
    a ~12 KiB stream window must keep reassembly & queue high-water marks bounded.
    """
    result = subprocess.run(
        [
            probe_bin,
            "--host", "localhost",
            "--port", str(route_guide_server.port),
            "--ca-file", certs.cert,
            "--max-msg-bytes", "8192",
            "--slack-bytes", "4096",
            "--max-queued-msgs", "4",
            "--count", "60",
            "--payload-bytes", "4096",
            "--read-delay-ms", "5",
        ],
        capture_output=True,
        text=True,
        timeout=_CLIENT_TIMEOUT_S,
    )
    assert result.returncode == 0, _fail_msg(result)
    line = next(
        ln for ln in result.stdout.splitlines() if ln.startswith("BACKPRESSURE ")
    )
    fields = dict(tok.split("=", 1) for tok in line.split()[1:])
    assert int(fields["code"]) == 0, _fail_msg(result)
    assert int(fields["messages"]) == 60, _fail_msg(result)
    assert int(fields["payload_ok"]) == 1, _fail_msg(result)

    window = int(fields["window"])
    # window = max_msg_bytes (8192) + 5 + slack (4096) = 12293.
    assert window == 8192 + 5 + 4096, _fail_msg(result)
    # 240 KiB total transferred; buffered reassembly & queue must stay bounded
    # by the stream window and max_queued_msgs (never ballooning toward 240 KiB).
    assert int(fields["max_reassembly"]) <= window, _fail_msg(result)
    assert int(fields["max_queued_msgs"]) <= 4, _fail_msg(result)
    assert int(fields["max_queued_bytes"]) <= window, _fail_msg(result)


def test_early_finish_without_draining_does_not_deadlock(
    shim_bin, certs, route_guide_server
):
    """§5.8: calling Finish() after reading 2 of 200 large messages (1.6 MiB total
    against an ~80 KiB stream window) cancels-and-discards promptly instead of
    deadlocking on withheld HTTP/2 flow control, and the server sees cancellation.
    """
    route_guide_server.reset_cancellations()
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "early_finish",
        "--lo-lat", "5",
        "--hi-lat", "200",
        "--hi-lon", "8192",
        "--stop-after", "2",
        "--max-recv-bytes", "16384",
    )
    lines = result.stdout.splitlines()
    summary = next(ln for ln in lines if ln.startswith("LISTFEATURES "))
    assert "messages=2" in summary, _fail_msg(result)
    code = int(summary.split("code=")[1].split()[0])
    assert code == 1, _fail_msg(result)  # CANCELLED
    elapsed_line = next(ln for ln in lines if ln.startswith("ELAPSED_MS "))
    assert int(elapsed_line.split()[1]) < 5000, _fail_msg(result)
    assert route_guide_server.wait_for_cancellation(timeout=3.0)


def test_drop_reader_without_finish_is_non_blocking_and_cancels(
    shim_bin, certs, route_guide_server
):
    """§5.8: destroying ClientReader without calling Finish() cancels on the wire
    without blocking on the network.
    """
    route_guide_server.reset_cancellations()
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "drop_reader",
        "--lo-lat", "6",
        "--lo-lon", "2",
        "--hi-lat", "10000",
        "--stop-after", "1",
    )
    assert result.returncode == 0, _fail_msg(result)
    line = next(ln for ln in result.stdout.splitlines() if ln.startswith("DROPPED "))
    assert "messages=1" in line, _fail_msg(result)
    elapsed_ms = int(line.split("elapsed_ms=")[1])
    assert elapsed_ms < 2000, _fail_msg(result)
    assert route_guide_server.wait_for_cancellation(timeout=3.0)


def test_mixed_concurrent_unary_streaming_and_cancel(
    shim_bin, certs, route_guide_server
):
    """TSan stress: 12 threads mixing unary, streaming, and concurrent TryCancel."""
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "mixed_concurrent",
        "--threads", "12",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert "MIXED ok=12 total=12" in result.stdout.splitlines(), _fail_msg(result)


def test_compressed_frame_resets_stream_and_keeps_channel_alive(shim_bin, certs):
    """Receiving a compressed gRPC frame fails the stream with INTERNAL (13),
    sends RST_STREAM(CANCEL) on the wire, and leaves the channel healthy for
    subsequent RPCs.
    """
    with H2TestServer(
        certs.cert, certs.key, mode="send_compressed_then_ok"
    ) as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "framing_error_then_unary",
        )
        assert result.returncode == 0, _fail_msg(result)
        assert (
            "FRAMING_THEN_UNARY stream_code=13 unary_code=0"
            in result.stdout.splitlines()
        ), _fail_msg(result)
        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) >= 1, "expected RST_STREAM on compressed frame"
        assert resets[0][1] == _H2_CANCEL


def test_oversize_frame_resets_stream_and_keeps_channel_alive(shim_bin, certs):
    """Receiving an oversize gRPC frame prefix fails the stream with
    RESOURCE_EXHAUSTED (8), sends RST_STREAM(CANCEL), and keeps the channel
    healthy for subsequent RPCs.
    """
    with H2TestServer(
        certs.cert, certs.key, mode="send_oversize_then_ok"
    ) as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "framing_error_then_unary",
            "--max-recv-bytes", "4096",
        )
        assert result.returncode == 0, _fail_msg(result)
        assert (
            "FRAMING_THEN_UNARY stream_code=8 unary_code=0"
            in result.stdout.splitlines()
        ), _fail_msg(result)
        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) >= 1, "expected RST_STREAM on oversize frame"
        assert resets[0][1] == _H2_CANCEL


def test_early_finish_resets_stream_and_keeps_channel_alive(shim_bin, certs):
    """Calling Finish() without draining an open stream fails it with
    CANCELLED (1), sends RST_STREAM(CANCEL), and leaves the channel usable.
    """
    with H2TestServer(certs.cert, certs.key, mode="stream_then_ok") as srv:
        result = _run_client(
            shim_bin, certs, srv.port,
            "--mode", "early_finish_then_unary",
        )
        assert result.returncode == 0, _fail_msg(result)
        assert (
            "EARLY_THEN_UNARY stream_code=1 unary_code=0"
            in result.stdout.splitlines()
        ), _fail_msg(result)
        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert len(resets) >= 1, "expected RST_STREAM on early Finish()"
        assert resets[0][1] == _H2_CANCEL


def test_channel_destroyed_while_reader_alive_returns_unavailable(
    shim_bin, certs, route_guide_server
):
    """Destroying Channel and Stub while ClientReader is still alive mid-stream
    cleanly unblocks Read() and returns UNAVAILABLE (14) from Finish().
    """
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "drop_channel_mid_stream",
        "--lo-lat", "6",
        "--lo-lon", "2",
        "--hi-lat", "10000",
    )
    assert result.returncode == 0, _fail_msg(result)
    line = next(
        ln for ln in result.stdout.splitlines() if ln.startswith("DROP_CHANNEL ")
    )
    fields = dict(tok.split("=", 1) for tok in line.split()[1:])
    assert int(fields["code"]) == 14, _fail_msg(result)


def test_draining_early_finish_preserves_server_ok_status(
    shim_bin, certs, route_guide_server
):
    """Calling Finish() early on a kDraining stream whose wire RPC already
    completed preserves the server's OK (0) status.
    """
    result = _run_client(
        shim_bin, certs, route_guide_server.port,
        "--mode", "draining_early_finish",
    )
    assert result.returncode == 0, _fail_msg(result)
    assert "DRAINING_EARLY code=0" in result.stdout.splitlines(), _fail_msg(result)


def test_concurrent_start_cancel_shutdown_race(
    probe_bin, certs, route_guide_server
):
    """TSan/ASan stress: 8 threads racing StartCall + CancelCall against
    concurrent Shutdown() from 2 threads, both when the server drops on first
    request (TearDown with active calls) and against a live grpcio server.
    """
    with H2TestServer(certs.cert, certs.key, mode="drop_on_request") as srv:
        drop_result = _run_probe(probe_bin, certs, srv.port, "--mode", "shutdown_race")
        assert drop_result.returncode == 0, _fail_msg(drop_result)
        assert "SHUTDOWN_RACE ok=1" in drop_result.stdout.splitlines(), _fail_msg(
            drop_result
        )
        kv = _parse_kv_lines(drop_result.stdout)
        assert int(kv["SHUTDOWN_MS"]) < 1000, _fail_msg(drop_result)
        assert int(kv["POST_SHUTDOWN_STATUS"]) == 14, _fail_msg(drop_result)

    live_result = _run_probe(
        probe_bin, certs, route_guide_server.port, "--mode", "shutdown_race"
    )
    assert live_result.returncode == 0, _fail_msg(live_result)
    assert "SHUTDOWN_RACE ok=1" in live_result.stdout.splitlines(), _fail_msg(
        live_result
    )
    kv = _parse_kv_lines(live_result.stdout)
    assert int(kv["SHUTDOWN_MS"]) < 1000, _fail_msg(live_result)
    assert int(kv["POST_SHUTDOWN_STATUS"]) == 14, _fail_msg(live_result)


def test_502_large_body_does_not_stall_manual_flow(probe_bin, certs):
    """Non-gRPC 502 response with a 32 KiB HTML body exceeding the 4101 B manual
    stream window must return flow-control credit for ignored DATA, complete
    call 1 with UNAVAILABLE (14) without stalling, and leave the channel
    healthy for call 2 to return OK (0) with 1 message.
    """
    with H2TestServer(
        certs.cert, certs.key, mode="502_large_body_then_ok"
    ) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "502_large_body_then_ok"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["STREAM_WINDOW"]) == 4101, _fail_msg(result)
        assert int(kv["CALL1_STATUS"]) == 14, _fail_msg(result)
        assert int(kv["CALL2_STATUS"]) == 0, _fail_msg(result)
        assert int(kv["CALL2_MESSAGES"]) == 1, _fail_msg(result)


def test_burst_then_close_preserves_draining_messages(probe_bin, certs):
    """Server sends 30 messages + trailers (END_STREAM) and immediately closes
    the TLS/TCP socket while 28 messages sit in scanner_ (kDraining); client
    must still read all 30 messages and observe final status OK (0).
    """
    with H2TestServer(certs.cert, certs.key, mode="burst_then_close") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "burst_then_close"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["MESSAGES"]) == 30, _fail_msg(result)
        assert int(kv["STATUS"]) == 0, _fail_msg(result)


def test_no_settings_hits_connect_timeout(probe_bin, certs):
    """Peer that completes TLS/ALPN h2 but never sends HTTP/2 SETTINGS must
    trip connect_timeout with UNAVAILABLE (14) mentioning SETTINGS.
    """
    with H2TestServer(certs.cert, certs.key, mode="no_settings") as srv:
        result = _run_probe(probe_bin, certs, srv.port, "--mode", "no_settings")
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["STATUS"]) == 14, _fail_msg(result)
        assert "SETTINGS" in kv["MESSAGE"], _fail_msg(result)
        assert 200 <= int(kv["ELAPSED_MS"]) < 2000, _fail_msg(result)


def test_next_message_size_is_nonblocking_on_stalled_stream(shim_bin, certs):
    """ClientReader::NextMessageSize() must return immediately (true, sz > 0)
    without blocking for server DATA even on a completely stalled stream.
    """
    with H2TestServer(certs.cert, certs.key, mode="stall_streams") as srv:
        result = _run_client(
            shim_bin, certs, srv.port, "--mode", "next_size_nonblocking"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["NEXT_SIZE_OK"]) == 1, _fail_msg(result)
        assert int(kv["NEXT_SIZE_MS"]) < 200, _fail_msg(result)
        assert int(kv["STATUS"]) == 1, _fail_msg(result)


def test_drop_channel_unblocks_blocked_read(probe_bin, certs):
    """Destroying/shutting down a channel while a reader thread is actively
    blocked inside PopMessage() must promptly unblock it with false and
    status UNAVAILABLE (14).
    """
    with H2TestServer(certs.cert, certs.key, mode="stall_streams") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "drop_channel_blocked_read"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["READ_RET"]) == 0, _fail_msg(result)
        assert int(kv["STATUS"]) == 14, _fail_msg(result)
        assert int(kv["UNBLOCK_MS"]) < 1000, _fail_msg(result)


def test_max_queued_bytes_cap_independent_of_message_count(probe_bin, certs):
    """max_queued_bytes caps delivery-queue memory independently of
    max_queued_messages even when max_queued_bytes < stream_window.
    """
    payload_bytes = 1024
    # Serialized Feature(name="p"*1024, location=Point(lat, 1024)) + 5 B gRPC prefix.
    single_message_wire_size = 5 + payload_bytes + 32
    with H2TestServer(
        certs.cert,
        certs.key,
        mode="stream_frames",
        stream_frames_count=40,
        stream_frame_payload_bytes=payload_bytes,
    ) as srv:
        result = _run_probe(
            probe_bin,
            certs,
            srv.port,
            "--mode", "backpressure",
            "--max-msg-bytes", "32768",
            "--slack-bytes", "0",
            "--max-queued-msgs", "100",
            "--max-queued-bytes", "4096",
            "--count", "40",
            "--payload-bytes", str(payload_bytes),
            "--read-delay-ms", "10",
        )
        assert result.returncode == 0, _fail_msg(result)
        line = next(
            ln for ln in result.stdout.splitlines() if ln.startswith("BACKPRESSURE ")
        )
        fields = dict(tok.split("=", 1) for tok in line.split()[1:])
        assert int(fields["code"]) == 0, _fail_msg(result)
        assert int(fields["messages"]) == 40, _fail_msg(result)
        assert int(fields["payload_ok"]) == 1, _fail_msg(result)
        assert int(fields["window"]) == 32768 + 5, _fail_msg(result)
        assert int(fields["max_queued_bytes"]) <= 4096 + single_message_wire_size, (
            _fail_msg(result)
        )
        assert int(fields["max_queued_bytes"]) <= 4096, _fail_msg(result)
        assert int(fields["max_queued_msgs"]) < 100, _fail_msg(result)


def test_queued_call_pumps_after_flush_closes_malformed_stream(probe_bin, certs):
    """When nghttp2 rejects malformed response HEADERS (missing :status) and
    closes stream 1 only during FlushOutput(), a queued call behind
    max_concurrent_streams=1 must still be pumped immediately without stalling.
    """
    with H2TestServer(certs.cert, certs.key, max_concurrent_streams=1) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "queued_after_malformed"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["CALL1_STATUS"]) != 0, _fail_msg(result)
        assert int(kv["CALL2_STATUS"]) == 0, _fail_msg(result)
        assert kv["CALL2_MSG"] == "second-ok", _fail_msg(result)
        assert int(kv["ELAPSED_MS"]) < 2000, _fail_msg(result)
        resets = srv.wait_for_resets(count=1, timeout=3.0)
        assert (1, _H2_PROTOCOL_ERROR) in resets, "expected RST_STREAM(PROTOCOL_ERROR) on stream 1"


def test_connection_protocol_error_tears_down_without_hanging(probe_bin, certs):
    """A connection-level HTTP/2 protocol error (SETTINGS ACK with non-zero
    payload) must tear down the session promptly with UNAVAILABLE (14) even
    when the peer keeps the TLS socket open sending PING frames.
    """
    with H2TestServer(certs.cert, certs.key) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "conn_error_no_deadline"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["STATUS"]) == 14, _fail_msg(result)
        assert int(kv["ELAPSED_MS"]) < 1500, _fail_msg(result)


def test_trailers_only_completes_under_zero_request_window(probe_bin, certs):
    """Trailers-only response received while the request body is blocked on
    SETTINGS_INITIAL_WINDOW_SIZE=0 must complete promptly with the trailer
    status instead of hanging waiting for upload flow-control credit.
    """
    with H2TestServer(certs.cert, certs.key, initial_window_size=0) as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "trailers_only_zero_window"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["STATUS"]) == 7, _fail_msg(result)
        assert kv["MESSAGE"] == "denied", _fail_msg(result)
        assert int(kv["ELAPSED_MS"]) < 1500, _fail_msg(result)


def test_shutdown_fails_draining_call_with_unavailable(probe_bin, certs):
    """Channel::Shutdown() on a wire-complete stream in draining_calls_ must
    fail the call with UNAVAILABLE (14) rather than silently truncating unread
    buffered messages with Status::OK.
    """
    with H2TestServer(certs.cert, certs.key, mode="burst_then_close") as srv:
        result = _run_probe(
            probe_bin, certs, srv.port, "--mode", "shutdown_draining_call"
        )
        assert result.returncode == 0, _fail_msg(result)
        kv = _parse_kv_lines(result.stdout)
        assert int(kv["RECEIVED"]) < 8, _fail_msg(result)
        assert int(kv["STATUS"]) == 14, _fail_msg(result)
