"""
FastAPI application configuration for the MCP server.

Used when running via `uvicorn server.app:combined_app` (e.g. local dev).
For the Databricks App deployment, uvicorn uses `app:app` (root app.py),
which builds the combined app directly.
"""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse
from fastmcp import FastMCP

from .tools import load_tools
from .utils import header_store

mcp_server = FastMCP(name="custom-mcp-server")

STATIC_DIR = Path(__file__).parent / "../static"

load_tools(mcp_server)
mcp_app = mcp_server.http_app()

app = FastAPI(
    title="Custom MCP Server",
    description="Custom MCP Server for the app",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/", include_in_schema=False)
async def serve_index():
    if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    return {"message": "Custom Open API Spec MCP Server is running", "status": "healthy"}


combined_app = FastAPI(
    title="Combined MCP App",
    routes=[
        *mcp_app.routes,
        *app.routes,
    ],
    lifespan=mcp_app.lifespan,
)


@combined_app.middleware("http")
async def capture_headers(request: Request, call_next):
    header_store.set(dict(request.headers))
    return await call_next(request)