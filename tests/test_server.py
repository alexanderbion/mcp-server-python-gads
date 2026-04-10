import os
import pytest
from httpx import ASGITransport, AsyncClient
from asgi_lifespan import LifespanManager

os.environ["MCP_API_TOKEN"] = "test-token"

BASE_URL = "http://test"
SECRET_PATH = "/mcp-primeads-secure-proxy-829xyz"


@pytest.fixture(scope="module")
async def managed_app():
    # Import inside the fixture so MCP_API_TOKEN is set before server loads.
    from server import create_app
    app = create_app()
    async with LifespanManager(app) as manager:
        yield manager.app


# ─── Health check ──────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_health(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


# ─── Secret-path mount ─────────────────────────────────────────────────


@pytest.mark.anyio
async def test_old_mcp_path_returns_404(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        resp = await client.post("/mcp", json={})
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_secret_path_messages_rejects_without_session(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        resp = await client.post(f"{SECRET_PATH}/messages/", json={})
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_secret_path_messages_rejects_invalid_session(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        resp = await client.post(
            f"{SECRET_PATH}/messages/?session_id=not-a-real-session",
            json={},
        )
    assert resp.status_code == 400
