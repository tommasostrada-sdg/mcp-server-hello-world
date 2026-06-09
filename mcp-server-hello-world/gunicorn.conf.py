# gunicorn.conf.py — auto-loaded by gunicorn from the working directory.
# Databricks Apps injects $PORT at runtime; fall back to 8080 for local dev.
import os

bind      = f"0.0.0.0:{os.environ.get('PORT', '8080')}"
workers   = 1        # Databricks Apps sandbox: keep at 1 to stay within RAM limits
timeout   = 120
keepalive = 5
accesslog = "-"      # stdout → captured in Databricks Apps "Logs" tab
errorlog  = "-"
loglevel  = "info"