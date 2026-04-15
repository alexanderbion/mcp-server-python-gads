import json
import os
import pytest
from httpx import ASGITransport, AsyncClient
from asgi_lifespan import LifespanManager

os.environ["MCP_API_TOKEN"] = "test-token"

BASE_URL = "http://test"

# Standard MCP init request
MCP_INIT = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-03-26",
        "capabilities": {},
        "clientInfo": {"name": "test", "version": "0.1.0"},
    },
}

MCP_HEADERS = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


def parse_sse_json(text: str) -> dict:
    """Extract the first JSON object from an SSE stream."""
    for line in text.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:])
    return {}


@pytest.fixture(scope="module")
async def managed_app():
    # Import here so MCP_API_TOKEN env var is set first
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


# ─── Auth tests ────────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_mcp_no_token_when_required(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        resp = await client.post("/mcp", json=MCP_INIT, headers=MCP_HEADERS)
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_mcp_wrong_token(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        headers = {**MCP_HEADERS, "Authorization": "Bearer wrong-token"}
        resp = await client.post("/mcp", json=MCP_INIT, headers=headers)
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_mcp_correct_token(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        headers = {**MCP_HEADERS, "Authorization": "Bearer test-token"}
        resp = await client.post("/mcp", json=MCP_INIT, headers=headers)
    assert resp.status_code == 200
    data = parse_sse_json(resp.text)
    assert data.get("result", {}).get("serverInfo", {}).get("name") == "prime-ads"


# ─── No-auth mode ─────────────────────────────────────────────────────


def test_no_auth_middleware_when_token_unset():
    """When MCP_API_TOKEN is unset, create_app() does not add BearerAuthMiddleware."""
    import server as srv
    saved = srv.MCP_API_TOKEN
    try:
        srv.MCP_API_TOKEN = None
        app = srv.create_app()
        # Walk the middleware list on the Starlette app object
        has_bearer = any(
            hasattr(m, "cls") and m.cls.__name__ == "BearerAuthMiddleware"
            for m in getattr(app, "middleware", [])
        )
        assert not has_bearer
    finally:
        srv.MCP_API_TOKEN = saved
