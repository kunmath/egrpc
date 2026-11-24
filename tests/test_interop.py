"""M8 acceptance (design §8): upstream gRPC interop subset.

``egrpc_interop_client`` — built from the vendored gRPC v1.82.0 testing protos
and UNMODIFIED grpc_cpp_plugin output over egrpc's grpcpp shim — runs the
in-scope upstream interop cases against a real grpcio ``TestService`` that
mirrors upstream ``tests/interop/service.py``. See docs/interop.md.
"""

import os
import subprocess

import pytest

from conftest import require_test_binary
from server.interop_server import InteropTestServer

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CLIENT_TIMEOUT_S = 60

_CASES = (
    "empty_unary",
    "large_unary",
    "server_streaming",
    "status_code_and_message",
    "custom_metadata",
    "timeout_on_sleeping_server",
    "cancel_after_begin",
    "unimplemented_method",
)

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
def interop_bin():
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(build_dir, "tests", "interop", "egrpc_interop_client")
    require_test_binary(
        path,
        "build the project first: cmake -S . -B build && cmake --build build -j",
    )
    return path


@pytest.fixture(scope="module")
def interop_server(certs):
    with InteropTestServer(certs.cert, certs.key) as srv:
        yield srv


def _run(interop_bin, *args):
    return subprocess.run(
        [interop_bin, *args],
        capture_output=True,
        text=True,
        timeout=_CLIENT_TIMEOUT_S,
    )


def _run_case(interop_bin, certs, port, case):
    return _run(
        interop_bin,
        "--server-host", "localhost",
        "--server-port", str(port),
        "--ca-file", certs.cert,
        "--test-case", case,
    )


def _fail_msg(result):
    return "\nrc={}\n--- stdout ---\n{}\n--- stderr ---\n{}".format(
        result.returncode, result.stdout, result.stderr
    )


@pytest.mark.parametrize("case", _CASES)
def test_interop_case(interop_bin, certs, interop_server, case):
    result = _run_case(interop_bin, certs, interop_server.port, case)
    assert result.returncode == 0, _fail_msg(result)
    assert "INTEROP_CASE {} PASS".format(case) in result.stdout.splitlines(), (
        _fail_msg(result)
    )
    _assert_no_sanitizer_reports(result.stderr)


def test_interop_all_cases(interop_bin, certs, interop_server):
    result = _run_case(interop_bin, certs, interop_server.port, "all")
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    for case in _CASES:
        assert "INTEROP_CASE {} PASS".format(case) in lines, _fail_msg(result)
    _assert_no_sanitizer_reports(result.stderr)


def test_interop_excluded_streaming_stubs_return_unimplemented(
    interop_bin, certs, interop_server
):
    """Client-streaming and bidi stubs compile but return UNIMPLEMENTED (12)."""
    result = _run_case(interop_bin, certs, interop_server.port, "excluded_stubs")
    assert result.returncode == 0, _fail_msg(result)
    lines = result.stdout.splitlines()
    assert "INTEROP_CASE excluded_stubs PASS" in lines, _fail_msg(result)
    assert "EXCLUDED StreamingInputCall code=12" in lines, _fail_msg(result)
    assert "EXCLUDED FullDuplexCall code=12" in lines, _fail_msg(result)
    _assert_no_sanitizer_reports(result.stderr)


def test_interop_cli_validation(interop_bin, certs):
    ok = ["--server-port", "1", "--ca-file", certs.cert, "--test-case", "empty_unary"]
    bad_invocations = [
        [],
        ["--ca-file", certs.cert, "--test-case", "empty_unary"],  # no port
        ["--server-port", "1", "--test-case", "empty_unary"],  # no ca-file
        ["--server-port", "1", "--ca-file", certs.cert],  # no test-case
        ok + ["--bogus", "x"],  # unknown flag
        ok + ["--server-host"],  # dangling flag
        ["--server-port", "0"] + ok[2:],
        ["--server-port", "70000"] + ok[2:],
        ["--server-port", "abc"] + ok[2:],
        ["--server-port", "1", "--ca-file", certs.cert, "--test-case", "nope"],
    ]
    for argv in bad_invocations:
        result = _run(interop_bin, *argv)
        assert result.returncode == 2, "argv={}{}".format(argv, _fail_msg(result))
        _assert_no_sanitizer_reports(result.stderr)
