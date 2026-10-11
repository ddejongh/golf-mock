from fastapi.testclient import TestClient

from venuepulse_agent.config import Settings
from venuepulse_agent.errors import GatewayError
from venuepulse_agent.main import REQUEST_ID_HEADER, create_app


def make_client() -> TestClient:
    app = create_app(Settings(log_json=False))

    @app.get("/boom-agent")
    async def boom_agent() -> None:
        raise GatewayError("gateway down")

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("unexpected")

    return TestClient(app, raise_server_exceptions=False)


def test_health_endpoints() -> None:
    client = make_client()
    assert client.get("/healthz").json() == {"status": "ok"}
    assert client.get("/readyz").json() == {"status": "ready"}


def test_request_id_is_propagated_or_generated() -> None:
    client = make_client()
    assert (
        client.get("/healthz", headers={REQUEST_ID_HEADER: "req-1"}).headers[REQUEST_ID_HEADER]
        == "req-1"
    )
    assert client.get("/healthz").headers[REQUEST_ID_HEADER]


def test_agent_error_maps_to_problem_json() -> None:
    resp = make_client().get("/boom-agent", headers={REQUEST_ID_HEADER: "req-2"})
    assert resp.status_code == 502
    assert resp.headers["content-type"] == "application/problem+json"
    body = resp.json()
    assert body["title"] == "gateway_error"
    assert body["retryable"] is True
    assert body["request_id"] == "req-2"


def test_unhandled_error_is_500_problem_json() -> None:
    resp = make_client().get("/boom")
    assert resp.status_code == 500
    assert resp.json()["status"] == 500
    assert resp.headers[REQUEST_ID_HEADER]
