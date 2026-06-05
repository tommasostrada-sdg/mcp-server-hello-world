import os
import threading
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient

# ─── Configuration ────────────────────────────────────────────────────────────
APP_NAME = os.getenv("APP_NAME", "mcp-server-ai-dev-kit")
MAX_UPTIME_SECONDS = int(os.getenv("MAX_UPTIME_MIN", "0")) * 60

print("[DEBUG] APP_NAME:", APP_NAME)
print("[DEBUG] MAX_UPTIME_SECONDS:", MAX_UPTIME_SECONDS)

# Override with hardcoded defaults if env not set (legacy behaviour)
if APP_NAME == "default-app":
    APP_NAME = "mcp-server-ai-dev-kit"

_start_time = time.monotonic()
_last_activity = time.monotonic()   # refreshed on every HTTP request

print(f"[DEBUG] _start_time:{_start_time}; now:{datetime.now()}")


# ─── State accessors (used by API endpoints) ──────────────────────────────────
def get_remaining_seconds() -> float:
    """Return seconds left before auto-shutdown. -1 if no limit set."""
    if MAX_UPTIME_SECONDS <= 0:
        return -1
    elapsed = time.monotonic() - _last_activity
    remaining = MAX_UPTIME_SECONDS - elapsed
    return max(remaining, 0)


def refresh_activity():
    """Call this on every request to reset the inactivity timer."""
    global _last_activity
    _last_activity = time.monotonic()


# ─── Databricks helpers ───────────────────────────────────────────────────────
def stop_databricks_app(app_name: str):
    """Stop the active deployment of a Databricks app by name."""
    try:
        w = WorkspaceClient()
    except Exception as e:
        print(f"[ERROR] Failed to initialize WorkspaceClient: {e}")
        raise

    print(f"[DEBUG] Attempting to stop app: '{app_name}'")
    try:
        w.apps.stop(name=app_name)
        print(f"[INFO] App '{app_name}' stopped successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to stop app '{app_name}': {e}")
        os._exit(1)


# ─── Background shutdown monitor ──────────────────────────────────────────────
def _uptime_shutdown_monitor() -> None:
    while True:
        time.sleep(30)

        now = datetime.now()

        # Hard stop at 15:30
        if now.hour == 15 and now.minute >= 30:
            print(f"[TIME STOP] Current time {now}. Shutting down app...")
            try:
                stop_databricks_app(APP_NAME)
            except Exception:
                os._exit(0)
            return

        # Inactivity-based stop
        if MAX_UPTIME_SECONDS > 0:
            elapsed = time.monotonic() - _last_activity
            if elapsed >= MAX_UPTIME_SECONDS:
                print(f"[UPTIME STOP] Inactivity timeout reached. Shutting down app...")
                try:
                    stop_databricks_app(APP_NAME)
                except Exception:
                    os._exit(0)
                return


_monitor_thread = threading.Thread(
    target=_uptime_shutdown_monitor,
    name="uptime-shutdown-monitor",
    daemon=True,
)
_monitor_thread.start()


# ─── ASGI app ─────────────────────────────────────────────────────────────────
# Import after state is initialised so server/app.py can reference this module
from databricks_mcp_server.server import mcp  # noqa: E402

def _wrap_asgi_app(app):
    async def wrapper(scope, receive, send):
        await app(scope, receive, send)
    return wrapper


inner_app = mcp.http_app(path="/mcp", stateless_http=True)
app = _wrap_asgi_app(inner_app)