import os
import threading
import time
from datetime import datetime
from databricks.sdk import WorkspaceClient
from databricks_mcp_server.server import mcp

# MAX_UPTIME_SECONDS = 2 * 60  # 2 minuti hard stop
APP_NAME = os.getenv("APP_NAME", "default-app")
MAX_UPTIME_SECONDS = int(os.getenv("MAX_UPTIME_MIN", "0")) * 60

print("[DEBUG] APP_NAME:", os.getenv("APP_NAME"))
print("[DEBUG] ALL ENV KEYS:", list(os.environ.keys()))
print(f"[DEBUG] Uptime set: {MAX_UPTIME_SECONDS}s")

_start_time = time.monotonic()
print(f"[DEBUG] _start_time:{_start_time}; now:{datetime.now()}")


def stop_databricks_app(app_name: str):
    """Stop the active deployment of a Databricks app by name with debug logs."""
    # print("[DEBUG] Initializing WorkspaceClient...")
    try:
        w = WorkspaceClient()
        # print("[DEBUG] WorkspaceClient initialized successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to initialize WorkspaceClient: {e}")
        raise

    print(f"[DEBUG] Attempting to stop app: '{app_name}'")

    try:
        response = w.apps.stop(name=app_name)
        # print(f"[DEBUG] Stop request sent. Response: {response}")
        print(f"[INFO] App '{app_name}' stopped successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to stop app '{app_name}': {e}")
        import os
        os._exit(1)


def _uptime_shutdown_monitor() -> None:
    while True:
        time.sleep(30)

        now = datetime.now()

        # Force to stop at 17:30
        if now.hour == 15 and now.minute >= 30:
            print(f"[TIME STOP] Current time {now}. Shutting down app...")
            try:
                stop_databricks_app(APP_NAME)
            except Exception:
                os._exit(0)
            return

        # ⏱️ Stop for uptime
        uptime = time.monotonic() - _start_time
        if uptime >= MAX_UPTIME_SECONDS and MAX_UPTIME_SECONDS > 0:
            print(f"[UPTIME STOP] Reached max uptime. Shutting down app...")
            try:
                stop_databricks_app(APP_NAME)
            except Exception:
                os._exit(0)
            return


# ASGI wrapper (opzionale, puoi tenerlo o rimuoverlo)
def _wrap_asgi_app(app):
    async def wrapper(scope, receive, send):
        await app(scope, receive, send)
    return wrapper


inner_app = mcp.http_app(path="/mcp", stateless_http=True)
app = _wrap_asgi_app(inner_app)

_monitor_thread = threading.Thread(
    target=_uptime_shutdown_monitor,
    name="uptime-shutdown-monitor",
    daemon=True,
)
_monitor_thread.start()
