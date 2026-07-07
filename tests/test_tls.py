"""egrpc M1 acceptance: TLS connect + ALPN h2 negotiation (design doc §8, M1)."""

from server.tls_test_server import TlsTestServer


def test_alpn_h2_negotiated(certs, run_probe):
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=["h2"]) as srv:
        result = run_probe("localhost", srv.port, "--ca", certs.cert)
    assert result.returncode == 0, result.stderr
    assert "CONNECTED alpn=h2" in result.stdout


def test_server_alpn_http11_fails_cleanly(certs, run_probe):
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=["http/1.1"]) as srv:
        result = run_probe("localhost", srv.port, "--ca", certs.cert)
    assert result.returncode == 1, result.stdout
    assert "did not negotiate HTTP/2 via ALPN" in result.stderr


def test_server_without_alpn_fails_cleanly(certs, run_probe):
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=None) as srv:
        result = run_probe("localhost", srv.port, "--ca", certs.cert)
    assert result.returncode == 1, result.stdout
    assert "did not negotiate HTTP/2 via ALPN" in result.stderr


def test_untrusted_cert_fails_with_handshake_detail(certs, run_probe):
    with TlsTestServer(certs.cert, certs.key, alpn_protocols=["h2"]) as srv:
        result = run_probe("localhost", srv.port)
    assert result.returncode == 1, result.stdout
    assert "certificate verification failed" in result.stderr
