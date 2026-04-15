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


# ─── Auth/Proxy tests ──────────────────────────────────────────────────


@pytest.mark.anyio
async def test_mcp_secret_path(managed_app):
    # The application is mounted at the secret path. 
    # Claude Web connectors drop auth/query params, so the path itself is the auth.
    secret_path = "/mcp-primeads-secure-proxy-829xyz"
    
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        # A simple test to check hitting the correct endpoint returns 200 properly.
        # FastMCP sse_app exposes /sse (GET). We use .stream() to avoid hanging on SSE.
        async with client.stream("GET", f"{secret_path}/sse") as response:
            assert response.status_code == 200
            assert "text/event-stream" in response.headers.get("content-type", "")

@pytest.mark.anyio
async def test_mcp_wrong_path(managed_app):
    async with AsyncClient(
        transport=ASGITransport(app=managed_app), base_url=BASE_URL
    ) as client:
        # Hitting standard endpoint without the secret proxy path should 404
        resp = await client.post("/mcp", json=MCP_INIT, headers=MCP_HEADERS)
    assert resp.status_code == 404

