"""Shared pytest fixtures for the egrpc M1 TLS integration tests."""

import collections
import os
import subprocess

import pytest

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

Certs = collections.namedtuple("Certs", ["cert", "key"])


@pytest.fixture(scope="session")
def certs(tmp_path_factory):
    """Generate a self-signed cert+key via the openssl CLI (session-scoped)."""
    d = tmp_path_factory.mktemp("certs")
    cert = d / "cert.pem"
    key = d / "key.pem"
    subprocess.run(
        [
            "openssl", "req", "-x509", "-newkey", "rsa:2048",
            "-keyout", str(key), "-out", str(cert),
            "-days", "2", "-nodes",
            "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
        ],
        check=True,
        capture_output=True,
    )
    return Certs(cert=str(cert), key=str(key))


@pytest.fixture(scope="session")
def probe_bin():
    """Absolute path to the compiled egrpc_tls_connect probe binary."""
    build_dir = os.environ.get("EGRPC_BUILD_DIR") or os.path.join(_REPO_ROOT, "build")
    path = os.path.join(build_dir, "tests", "integration", "egrpc_tls_connect")
    if not os.path.isfile(path):
        pytest.skip(
            "probe binary not found at {}; build the project first: "
            "cmake -S . -B build && cmake --build build -j".format(path),
            allow_module_level=True,
        )
    return path


@pytest.fixture
def run_probe(probe_bin):
    """Return a function(host, port, *extra_args) that runs the probe binary."""

    def _run(host, port, *extra_args):
        return subprocess.run(
            [
                probe_bin,
                "--host", host,
                "--port", str(port),
                "--timeout-ms", "5000",
                *extra_args,
            ],
            capture_output=True,
            text=True,
            timeout=20,
        )

    return _run
