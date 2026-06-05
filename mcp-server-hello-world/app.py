"""
Entry point for uvicorn: `uvicorn app:app`

This module:
  1. Reads configuration from environment variables (APP_NAME, MAX_UPTIME_MIN)
  2. Tracks inactivity and shuts down the Databricks App when the timeout expires
  3. Builds the full ASGI app (FastAPI + MCP routes + static index + REST API)
     and exports it as `app` so uvicorn finds it.
"""

import os
import re
import threading
import time
from datetime import datetime
from pathlib import Path

from databricks.sdk import WorkspaceClient
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastmcp import FastMCP
from pydantic import BaseModel

# ─── Paths ────────────────────────────────────────────────────────────────────
_HERE      = Path(__file__).parent
STATIC_DIR = _HERE / "static"
APP_YAML   = _HERE / "app.yaml"

# ─── Configuration ────────────────────────────────────────────────────────────
APP_NAME          = os.getenv("APP_NAME", "mcp-server-ai-dev-kit")
MAX_UPTIME_SECONDS = int(os.getenv("MAX_UPTIME_MIN", "0")) * 60

print(f"[DEBUG] APP_NAME: {APP_NAME}")
print(f"[DEBUG] MAX_UPTIME_SECONDS: {MAX_UPTIME_SECONDS}")

_start_time    = time.monotonic()
_last_activity = time.monotonic()   # reset on every HTTP request

print(f"[DEBUG] started at {datetime.now()}")


# ─── State accessors ──────────────────────────────────────────────────────────
def get_remaining_seconds() -> float:
    """Seconds left before auto-shutdown; -1 means no limit."""
    if MAX_UPTIME_SECONDS <= 0:
        return -1
    elapsed   = time.monotonic() - _last_activity
    remaining = MAX_UPTIME_SECONDS - elapsed
    return max(remaining, 0)


def refresh_activity():
    """Reset the inactivity timer (called on every HTTP request)."""
    global _last_activity
    _last_activity = time.monotonic()


# ─── Databricks helpers ───────────────────────────────────────────────────────
def stop_databricks_app(app_name: str):
    try:
        w = WorkspaceClient()
    except Exception as e:
        print(f"[ERROR] WorkspaceClient init failed: {e}")
        raise

    print(f"[DEBUG] Stopping app: '{app_name}'")
    try:
        w.apps.stop(name=app_name)
        print(f"[INFO] App '{app_name}' stopped.")
    except Exception as e:
        print(f"[ERROR] Stop failed: {e}")
        os._exit(1)


# ─── Background shutdown monitor ──────────────────────────────────────────────
def _uptime_shutdown_monitor() -> None:
    while True:
        time.sleep(30)
        now = datetime.now()

        # Hard stop at 15:30
        if now.hour == 15 and now.minute >= 30:
            print(f"[TIME STOP] {now} — shutting down.")
            try:
                stop_databricks_app(APP_NAME)
            except Exception:
                os._exit(0)
            return

        # Inactivity timeout
        if MAX_UPTIME_SECONDS > 0:
            if time.monotonic() - _last_activity >= MAX_UPTIME_SECONDS:
                print("[UPTIME STOP] Inactivity timeout — shutting down.")
                try:
                    stop_databricks_app(APP_NAME)
                except Exception:
                    os._exit(0)
                return


threading.Thread(
    target=_uptime_shutdown_monitor,
    name="uptime-shutdown-monitor",
    daemon=True,
).start()


# ─── MCP server ───────────────────────────────────────────────────────────────
from contextlib import asynccontextmanager          # noqa: E402
from starlette.routing import Mount                 # noqa: E402
from databricks_mcp_server.server import mcp as _mcp_server  # noqa: E402

_mcp_asgi = _mcp_server.http_app(path="/mcp", stateless_http=True)


# ─── FastAPI app (UI + REST endpoints) ────────────────────────────────────────
# Lifespan wraps the MCP app's own lifespan so its startup/shutdown hooks run.
@asynccontextmanager
async def _lifespan(app_: FastAPI):
    async with _mcp_asgi.router.lifespan_context(app_):
        yield


app = FastAPI(title="MCP Server + UI", lifespan=_lifespan)


@app.middleware("http")
async def _activity_middleware(request: Request, call_next):
    refresh_activity()
    return await call_next(request)


# ── UI / REST routes ──────────────────────────────────────────────────────────
@app.get("/", include_in_schema=False)
async def serve_index():
    if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
        return FileResponse(STATIC_DIR / "index.html")
    return {"message": "MCP Server is running", "status": "healthy"}


@app.get("/api/status")
async def api_status():
    """Return remaining seconds and current max-uptime setting."""
    refresh_activity()
    return JSONResponse({
        "remaining_seconds": get_remaining_seconds(),
        "max_uptime_min":    int(MAX_UPTIME_SECONDS // 60),
    })


class UptimePayload(BaseModel):
    minutes: int


@app.post("/api/set-uptime")
async def api_set_uptime(payload: UptimePayload):
    """Update MAX_UPTIME_MIN in memory and persist to app.yaml."""
    global MAX_UPTIME_SECONDS
    minutes = max(0, payload.minutes)
    MAX_UPTIME_SECONDS = minutes * 60
    refresh_activity()

    try:
        if APP_YAML.exists():
            content = APP_YAML.read_text()
            updated = re.sub(
                r'(MAX_UPTIME_MIN:\s*")[^"]*(")',
                rf'\g<1>{minutes}\g<2>',
                content,
            )
            if updated == content:
                updated = re.sub(
                    r'(MAX_UPTIME_MIN:\s*)(\S+)',
                    rf'\g<1>"{minutes}"',
                    content,
                )
            APP_YAML.write_text(updated)
    except Exception as e:
        print(f"[WARN] Could not write app.yaml: {e}")

    return JSONResponse({"ok": True, "max_uptime_min": minutes})


# ── Mount MCP at /mcp ─────────────────────────────────────────────────────────
# Use Starlette Mount so the sub-app's routes are never merged into the
# FastAPI router (which caused the on_startup kwarg crash).
app.mount("/mcp", _mcp_asgi)