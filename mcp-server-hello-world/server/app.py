"""
FastAPI application configuration for the MCP server.

This module sets up the core application by:
1. Creating and configuring the FastMCP server instance
2. Loading and registering all MCP tools
3. Setting up CORS middleware for cross-origin requests
4. Combining MCP routes with standard FastAPI routes
5. Optionally serving static files for a web frontend
6. Exposing REST endpoints for uptime status and configuration

The MCP (Model Context Protocol) server provides tools that can be called by
AI assistants and other clients. FastMCP makes it easy to expose these tools
over HTTP using the MCP protocol standard.
"""

import os
import re
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastmcp import FastMCP
from pydantic import BaseModel

from .tools import load_tools
from .utils import header_store

mcp_server = FastMCP(name="custom-mcp-server")

STATIC_DIR = Path(__file__).parent / "../static"
APP_YAML = Path(__file__).parent / "../app.yaml"

# Load and register all tools with the MCP server
load_tools(mcp_server)

# Convert the MCP server to a streamable HTTP application
mcp_app = mcp_server.http_app()

# ============================================================================
# FastAPI Application Setup
# ============================================================================

app = FastAPI(
    title="Custom MCP Server",
    description="Custom MCP Server for the app",
    version="0.1.0",
    lifespan=mcp_app.lifespan,
)


@app.get("/", include_in_schema=False)
async def serve_index():
    """Serve the index page"""
    if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    return {"message": "Custom Open API Spec MCP Server is running", "status": "healthy"}


# ─── Uptime / status API ──────────────────────────────────────────────────────

@app.get("/api/status")
async def api_status():
    """Return current uptime state: remaining seconds and max uptime setting."""
    # Import lazily to avoid circular deps; app.py is the top-level entry point
    try:
        import app as root_app  # noqa: PLC0415
        root_app.refresh_activity()
        remaining = root_app.get_remaining_seconds()
        max_uptime_min = int(root_app.MAX_UPTIME_SECONDS // 60)
    except Exception:
        remaining = -1
        max_uptime_min = 0

    return JSONResponse({
        "remaining_seconds": remaining,
        "max_uptime_min": max_uptime_min,
    })


class UptimePayload(BaseModel):
    minutes: int


@app.post("/api/set-uptime")
async def api_set_uptime(payload: UptimePayload):
    """
    Update MAX_UPTIME_MIN at runtime and persist the change to app.yaml.
    Also resets the inactivity timer.
    """
    minutes = max(1, payload.minutes)

    # ── Update in-memory state ────────────────────────────────────────────────
    try:
        import app as root_app  # noqa: PLC0415
        root_app.MAX_UPTIME_SECONDS = minutes * 60
        root_app.refresh_activity()
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

    # ── Persist to app.yaml ───────────────────────────────────────────────────
    try:
        if APP_YAML.exists():
            content = APP_YAML.read_text()
            # Replace the value of MAX_UPTIME_MIN in the yaml
            updated = re.sub(
                r'(MAX_UPTIME_MIN:\s*")[^"]*(")',
                rf'\g<1>{minutes}\g<2>',
                content,
            )
            if updated == content:
                # Try unquoted variant
                updated = re.sub(
                    r'(MAX_UPTIME_MIN:\s*)(\S+)',
                    rf'\g<1>"{minutes}"',
                    content,
                )
            APP_YAML.write_text(updated)
    except Exception as e:
        # Non-fatal: in-memory update succeeded
        print(f"[WARN] Could not write app.yaml: {e}")

    return JSONResponse({"ok": True, "max_uptime_min": minutes})


# ─── Combined app ─────────────────────────────────────────────────────────────

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
    """Middleware to capture request headers for authentication and refresh activity timer."""
    header_store.set(dict(request.headers))

    # Refresh inactivity timer on every request
    try:
        import app as root_app  # noqa: PLC0415
        root_app.refresh_activity()
    except Exception:
        pass

    return await call_next(request)