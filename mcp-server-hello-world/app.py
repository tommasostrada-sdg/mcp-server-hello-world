"""
Databricks AI Dev Kit — Custom MCP Server
Exposes Genie / Databricks AI tools via MCP protocol with a live dashboard.
"""

import json
import logging
import os
import re
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, Response, stream_with_context, send_from_directory, send_from_directory
from flask_cors import CORS

# ── Logging ──────────────────────────────────────────────────────────────────
# /tmp is the only guaranteed writable dir inside the Databricks Apps sandbox.
# The app source directory is read-only at runtime.
LOG_FILE = Path("/tmp/mcp_server.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),   # stdout is captured by Databricks Apps log viewer
    ],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

# ── State ─────────────────────────────────────────────────────────────────────
IDLE_TIMEOUT_MINUTES = 30          # Databricks Apps default idle timeout
_last_activity_ts: float = time.time()
_activity_lock = threading.Lock()


def touch_activity(label: str = "api-call"):
    """Record a new activity timestamp."""
    global _last_activity_ts
    with _activity_lock:
        _last_activity_ts = time.time()
    logger.info("ACTIVITY:%s", label)


def minutes_until_shutdown() -> float:
    """Return fractional minutes remaining before the app would idle-off."""
    with _activity_lock:
        elapsed = (time.time() - _last_activity_ts) / 60.0
    remaining = max(0.0, IDLE_TIMEOUT_MINUTES - elapsed)
    return round(remaining, 2)


# ── MCP Tool Registry ─────────────────────────────────────────────────────────
TOOLS: dict[str, dict] = {
    # ── Genie / SQL Intelligence ──────────────────────────────────────────
    "genie_query_explain": {
        "name": "genie_query_explain",
        "description": "Explain a SQL or PySpark query using Databricks AI; returns plain-language summary, identified bottlenecks, and optimisation hints.",
        "category": "Genie",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "SQL / PySpark query to analyse"},
                "dialect": {"type": "string", "enum": ["sql", "pyspark", "spark-sql"], "default": "sql"},
                "focus": {"type": "string", "enum": ["explain", "optimise", "security", "all"], "default": "all"},
            },
            "required": ["query"],
        },
    },
    "genie_schema_advisor": {
        "name": "genie_schema_advisor",
        "description": "Analyse a Delta table schema and suggest improvements: partitioning, Z-ordering, bloom filters, column statistics.",
        "category": "Genie",
        "inputSchema": {
            "type": "object",
            "properties": {
                "schema_json": {"type": "string", "description": "JSON representation of the Delta table schema"},
                "table_name": {"type": "string"},
                "row_count_estimate": {"type": "integer"},
            },
            "required": ["schema_json"],
        },
    },
    "genie_notebook_review": {
        "name": "genie_notebook_review",
        "description": "Review a Databricks notebook (Python/SQL cells) for code quality, idempotency, secret leakage, and Databricks best-practices.",
        "category": "Genie",
        "inputSchema": {
            "type": "object",
            "properties": {
                "notebook_source": {"type": "string", "description": "Full notebook source as a string"},
                "language": {"type": "string", "enum": ["python", "sql", "scala", "r"], "default": "python"},
            },
            "required": ["notebook_source"],
        },
    },
    # ── AI Dev Kit ────────────────────────────────────────────────────────
    "aidevkit_generate_pipeline": {
        "name": "aidevkit_generate_pipeline",
        "description": "Generate a Delta Live Tables (DLT) pipeline skeleton from a natural-language description.",
        "category": "AI Dev Kit",
        "inputSchema": {
            "type": "object",
            "properties": {
                "description": {"type": "string"},
                "source_format": {"type": "string", "enum": ["json", "csv", "parquet", "kafka", "autoloader"], "default": "autoloader"},
                "target_table": {"type": "string"},
                "include_expectations": {"type": "boolean", "default": True},
            },
            "required": ["description", "target_table"],
        },
    },
    "aidevkit_mlflow_scaffold": {
        "name": "aidevkit_mlflow_scaffold",
        "description": "Scaffold an MLflow experiment: training script, model registration, feature store logging, and serving endpoint config.",
        "category": "AI Dev Kit",
        "inputSchema": {
            "type": "object",
            "properties": {
                "model_type": {"type": "string", "enum": ["sklearn", "xgboost", "pytorch", "transformers", "custom"]},
                "experiment_name": {"type": "string"},
                "register_model": {"type": "boolean", "default": True},
            },
            "required": ["model_type", "experiment_name"],
        },
    },
    "aidevkit_dbt_gen": {
        "name": "aidevkit_dbt_gen",
        "description": "Generate dbt model SQL + schema.yml from a source table description or existing raw SQL.",
        "category": "AI Dev Kit",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "string", "description": "Raw SQL or table description"},
                "model_name": {"type": "string"},
                "materialization": {"type": "string", "enum": ["table", "view", "incremental"], "default": "table"},
            },
            "required": ["source", "model_name"],
        },
    },
    "aidevkit_secret_scanner": {
        "name": "aidevkit_secret_scanner",
        "description": "Scan notebook / script source for leaked secrets, tokens, connection strings, and PATs; returns findings with line numbers.",
        "category": "AI Dev Kit",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_code": {"type": "string"},
                "file_name": {"type": "string", "default": "unknown"},
            },
            "required": ["source_code"],
        },
    },
    # ── Space / Workspace Helpers ─────────────────────────────────────────
    "space_activity_check": {
        "name": "space_activity_check",
        "description": "Check recent workspace activity from server logs; returns last-N events and minutes until auto-shutdown.",
        "category": "Space",
        "inputSchema": {
            "type": "object",
            "properties": {
                "last_n": {"type": "integer", "default": 20, "description": "Number of recent log lines to return"},
            },
        },
    },
    "space_keep_alive": {
        "name": "space_keep_alive",
        "description": "Ping the server to reset the idle timer. Call this periodically from long-running notebooks to prevent app shutdown.",
        "category": "Space",
        "inputSchema": {"type": "object", "properties": {}},
    },
    # ── Test ──────────────────────────────────────────────────────────────
    "test_echo": {
        "name": "test_echo",
        "description": "Echo tool — verifies the MCP server is reachable and tools execute. Returns the input payload with a timestamp.",
        "category": "Test",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "default": "ping"},
                "include_server_info": {"type": "boolean", "default": True},
            },
        },
    },
}


# ── Tool Executors ─────────────────────────────────────────────────────────────

def _run_genie_query_explain(args: dict) -> dict:
    query = args["query"]
    dialect = args.get("dialect", "sql")
    focus = args.get("focus", "all")
    # Simulate AI analysis (in production: call Databricks Foundation Model API)
    lines = [l.strip() for l in query.split("\n") if l.strip()]
    has_select_star = "select *" in query.lower()
    has_no_filter = "where" not in query.lower()
    suggestions = []
    if has_select_star:
        suggestions.append("Avoid SELECT *: project only needed columns to reduce shuffle and scan cost.")
    if has_no_filter:
        suggestions.append("No WHERE clause detected: consider adding partition filters to limit data scanned.")
    if "join" in query.lower() and "broadcast" not in query.lower():
        suggestions.append("Consider BROADCAST hint for small dimension tables in JOINs.")
    return {
        "dialect": dialect,
        "focus": focus,
        "line_count": len(lines),
        "summary": f"Query performs a {dialect.upper()} operation across {len(lines)} logical lines.",
        "bottlenecks": suggestions if suggestions else ["No major bottlenecks detected."],
        "optimisation_hints": [
            "Enable Photon engine for vectorised execution.",
            "Use ZORDER BY on high-cardinality filter columns.",
            "Cache intermediate results with .cache() if reused.",
        ],
        "security_notes": [
            "No hardcoded credentials detected." if "password" not in query.lower() else "⚠️ Possible credential in query!"
        ],
    }


def _run_genie_schema_advisor(args: dict) -> dict:
    try:
        schema = json.loads(args["schema_json"])
    except Exception:
        schema = {"fields": []}
    fields = schema.get("fields", schema) if isinstance(schema, dict) else schema
    field_count = len(fields) if isinstance(fields, list) else 0
    return {
        "table": args.get("table_name", "unknown"),
        "field_count": field_count,
        "recommendations": [
            {"type": "partitioning", "suggestion": "Add PARTITIONED BY (year, month) on date columns for time-series data."},
            {"type": "z-order", "suggestion": "ZORDER BY (customer_id) if queries frequently filter on customer_id."},
            {"type": "bloom_filter", "suggestion": "Enable bloom filters on high-cardinality string columns (email, uuid)."},
            {"type": "statistics", "suggestion": "Run ANALYZE TABLE … COMPUTE STATISTICS to enable CBO optimisations."},
        ],
        "estimated_improvement": "20–60% scan reduction with recommended partitioning.",
    }


def _run_genie_notebook_review(args: dict) -> dict:
    src = args["notebook_source"]
    findings = []
    if re.search(r"(password|secret|token)\s*=\s*['\"]", src, re.I):
        findings.append({"severity": "CRITICAL", "msg": "Hardcoded secret/token detected — use dbutils.secrets instead."})
    if "spark.read" in src and ".cache()" not in src and src.count("spark.read") > 2:
        findings.append({"severity": "WARNING", "msg": "Multiple spark.read calls without caching; consider caching shared DataFrames."})
    if "display(" in src:
        findings.append({"severity": "INFO", "msg": "display() calls found — remove in production pipelines."})
    if not findings:
        findings.append({"severity": "OK", "msg": "No critical issues found."})
    return {
        "language": args.get("language", "python"),
        "cell_count": src.count("# COMMAND ----------") + 1,
        "findings": findings,
        "best_practices": [
            "Use %run or notebook widgets instead of hardcoded paths.",
            "Add idempotency checks (IF NOT EXISTS, MERGE INTO) for all writes.",
            "Structure notebooks with clear sections: Config → Ingest → Transform → Write.",
        ],
    }


def _run_aidevkit_generate_pipeline(args: dict) -> dict:
    target = args["target_table"]
    fmt = args.get("source_format", "autoloader")
    inc_exp = args.get("include_expectations", True)
    expectations_block = """
    @dlt.expect_or_drop("valid_id", "id IS NOT NULL")
    @dlt.expect_or_warn("valid_ts", "event_timestamp > '2000-01-01'")""" if inc_exp else ""
    code = f'''import dlt
from pyspark.sql.functions import *

# Auto-generated DLT pipeline — {datetime.now().strftime("%Y-%m-%d")}
# Description: {args["description"]}

SOURCE_PATH = spark.conf.get("source_path", "/mnt/landing/{target}")

@dlt.table(name="bronze_{target}", comment="Raw ingest from {fmt}")
def bronze_{target}():
    return (
        spark.readStream.format("{"cloudFiles" if fmt == "autoloader" else fmt}")
        .option("cloudFiles.format", "json")
        .load(SOURCE_PATH)
        .select("*", current_timestamp().alias("_ingested_at"))
    )

@dlt.table(name="silver_{target}", comment="Cleansed {target}"){expectations_block}
def silver_{target}():
    return (
        dlt.read_stream("bronze_{target}")
        .dropDuplicates(["id"])
        .filter("id IS NOT NULL")
    )

@dlt.table(name="gold_{target}", comment="Aggregated {target}")
def gold_{target}():
    return (
        dlt.read("silver_{target}")
        .groupBy("date")
        .agg(count("*").alias("total_records"))
    )
'''
    return {
        "pipeline_code": code,
        "tables_generated": [f"bronze_{target}", f"silver_{target}", f"gold_{target}"],
        "source_format": fmt,
        "includes_expectations": inc_exp,
        "next_steps": [
            "Upload to Databricks Workflows → Delta Live Tables.",
            "Configure source_path in pipeline settings.",
            "Enable CDF (Change Data Feed) if downstream consumers need incremental data.",
        ],
    }


def _run_aidevkit_mlflow_scaffold(args: dict) -> dict:
    mtype = args["model_type"]
    exp = args["experiment_name"]
    code = f'''import mlflow
import mlflow.{mtype if mtype not in ("custom",) else "pyfunc"}

mlflow.set_experiment("{exp}")

with mlflow.start_run(run_name="training_run") as run:
    # ── Params ──────────────────────────────────────────────────────────
    params = {{"n_estimators": 100, "max_depth": 6, "learning_rate": 0.1}}
    mlflow.log_params(params)

    # ── Train ────────────────────────────────────────────────────────────
    # TODO: replace with your training logic
    model = train_model(**params)

    # ── Metrics ─────────────────────────────────────────────────────────
    metrics = evaluate(model)
    mlflow.log_metrics(metrics)

    # ── Log model ───────────────────────────────────────────────────────
    mlflow.{mtype if mtype not in ("custom",) else "pyfunc"}.log_model(
        model,
        artifact_path="model",
        registered_model_name="{exp}_model",
    )

    print(f"Run ID: {{run.info.run_id}}")
    print(f"Artifact URI: {{mlflow.get_artifact_uri()}}")
'''
    return {
        "scaffold_code": code,
        "experiment": exp,
        "model_type": mtype,
        "files_generated": ["train.py", "mlflow_config.yaml"],
        "serving_snippet": f"mlflow models serve -m models:/{exp}_model/Production -p 5001",
    }


def _run_aidevkit_dbt_gen(args: dict) -> dict:
    name = args["model_name"]
    mat = args.get("materialization", "table")
    return {
        "model_sql": f'''-- models/{name}.sql
-- Auto-generated dbt model — materialization: {mat}
{{{{ config(materialized="{mat}") }}}}

SELECT
    id,
    created_at,
    updated_at,
    -- TODO: add business logic
    CURRENT_TIMESTAMP() AS _dbt_loaded_at
FROM {{{{ ref("stg_{name}") }}}}
WHERE id IS NOT NULL
''',
        "schema_yml": f'''# models/schema.yml
version: 2
models:
  - name: {name}
    description: "Auto-generated model for {name}"
    columns:
      - name: id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: created_at
        description: "Record creation timestamp"
        tests:
          - not_null
''',
        "materialization": mat,
    }


def _run_aidevkit_secret_scanner(args: dict) -> dict:
    src = args["source_code"]
    fname = args.get("file_name", "unknown")
    patterns = [
        (r"(?i)(password|passwd|pwd)\s*=\s*['\"][^'\"]{4,}", "Hardcoded password"),
        (r"(?i)(token|api_key|apikey|secret)\s*=\s*['\"][^'\"]{8,}", "Hardcoded token/key"),
        (r"dapi[a-zA-Z0-9]{32}", "Databricks PAT"),
        (r"(?i)jdbc:.+password=[^&\s]+", "JDBC connection string with password"),
        (r"(?i)AccountKey=[A-Za-z0-9+/=]{40,}", "Azure Storage AccountKey"),
    ]
    findings = []
    for i, line in enumerate(src.split("\n"), 1):
        for pat, label in patterns:
            if re.search(pat, line):
                masked = re.sub(r"(['\"])([^'\"]{4})[^'\"]*(['\"])", r"\1\2***\3", line.strip())
                findings.append({"line": i, "type": label, "snippet": masked})
    return {
        "file": fname,
        "scanned_lines": src.count("\n") + 1,
        "findings": findings,
        "clean": len(findings) == 0,
        "remediation": "Replace hardcoded values with: dbutils.secrets.get(scope='your-scope', key='your-key')",
    }


def _run_space_activity_check(args: dict) -> dict:
    last_n = args.get("last_n", 20)
    events = []
    if LOG_FILE.exists():
        lines = LOG_FILE.read_text(errors="replace").splitlines()
        events = lines[-last_n:]
    return {
        "minutes_until_shutdown": minutes_until_shutdown(),
        "idle_timeout_minutes": IDLE_TIMEOUT_MINUTES,
        "recent_events": events,
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
    }


def _run_space_keep_alive(args: dict) -> dict:
    touch_activity("keep-alive")
    return {
        "status": "ok",
        "message": "Timer reset. App will stay alive.",
        "minutes_until_shutdown": minutes_until_shutdown(),
    }


def _run_test_echo(args: dict) -> dict:
    touch_activity("test-echo")
    result = {
        "echo": args.get("message", "ping"),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "status": "MCP server operational ✓",
    }
    if args.get("include_server_info", True):
        result["server_info"] = {
            "tool_count": len(TOOLS),
            "categories": list({t["category"] for t in TOOLS.values()}),
            "minutes_until_shutdown": minutes_until_shutdown(),
            "log_file": str(LOG_FILE.resolve()),
        }
    return result


EXECUTORS = {
    "genie_query_explain": _run_genie_query_explain,
    "genie_schema_advisor": _run_genie_schema_advisor,
    "genie_notebook_review": _run_genie_notebook_review,
    "aidevkit_generate_pipeline": _run_aidevkit_generate_pipeline,
    "aidevkit_mlflow_scaffold": _run_aidevkit_mlflow_scaffold,
    "aidevkit_dbt_gen": _run_aidevkit_dbt_gen,
    "aidevkit_secret_scanner": _run_aidevkit_secret_scanner,
    "space_activity_check": _run_space_activity_check,
    "space_keep_alive": _run_space_keep_alive,
    "test_echo": _run_test_echo,
}


# ── MCP Protocol Endpoints ────────────────────────────────────────────────────

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    """JSON-RPC 2.0 MCP endpoint."""
    touch_activity("mcp-rpc")
    body = request.get_json(force=True)
    method = body.get("method", "")
    req_id = body.get("id")
    params = body.get("params", {})

    def ok(result):
        return jsonify({"jsonrpc": "2.0", "id": req_id, "result": result})

    def err(code, msg):
        # JSON-RPC 2.0 spec: error responses use HTTP 200 with an error object
        return jsonify({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": msg}})

    if method == "initialize":
        return ok({
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {"listChanged": False}},
            "serverInfo": {"name": "databricks-ai-devkit-mcp", "version": "1.0.0"},
        })

    if method == "tools/list":
        return ok({"tools": list(TOOLS.values())})

    if method == "tools/call":
        tool_name = params.get("name")
        tool_args = params.get("arguments", {})
        if tool_name not in TOOLS:
            return err(-32601, f"Tool '{tool_name}' not found")
        try:
            result = EXECUTORS[tool_name](tool_args)
            logger.info("TOOL_CALL:%s args=%s", tool_name, json.dumps(tool_args)[:200])
            return ok({"content": [{"type": "text", "text": json.dumps(result, indent=2)}]})
        except Exception as exc:
            logger.error("TOOL_ERROR:%s %s", tool_name, exc)
            return err(-32000, str(exc))

    if method == "ping":
        return ok({"pong": True})

    return err(-32601, f"Method '{method}' not found")


# ── Dashboard API ─────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    touch_activity("status-poll")
    return jsonify({
        "minutes_until_shutdown": minutes_until_shutdown(),
        "idle_timeout_minutes": IDLE_TIMEOUT_MINUTES,
        "tool_count": len(TOOLS),
        "categories": sorted({t["category"] for t in TOOLS.values()}),
        "server_time_utc": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/api/tools")
def api_tools():
    touch_activity("tools-list")
    return jsonify({"tools": list(TOOLS.values())})


@app.route("/api/logs")
def api_logs():
    touch_activity("logs-poll")
    n = int(request.args.get("n", 50))
    lines = []
    if LOG_FILE.exists():
        lines = LOG_FILE.read_text(errors="replace").splitlines()[-n:]
    return jsonify({"lines": lines, "total": len(lines)})


@app.route("/api/invoke", methods=["POST"])
def api_invoke():
    """Convenience REST wrapper for tool invocation (non-MCP clients)."""
    touch_activity("api-invoke")
    data = request.get_json(force=True)
    tool_name = data.get("tool")
    args = data.get("args", {})
    if tool_name not in TOOLS:
        return jsonify({"error": f"Unknown tool: {tool_name}"}), 404
    try:
        result = EXECUTORS[tool_name](args)
        return jsonify({"tool": tool_name, "result": result})
    except Exception as exc:
        logger.error("API_INVOKE_ERROR:%s %s", tool_name, exc)
        return jsonify({"error": str(exc)}), 500


@app.route("/api/keepalive", methods=["POST"])
def api_keepalive():
    touch_activity("http-keepalive")
    return jsonify({"ok": True, "minutes_until_shutdown": minutes_until_shutdown()})


# ── SSE activity stream ───────────────────────────────────────────────────────

@app.route("/api/stream")
def api_stream():
    """Server-Sent Events: push timer + log tail every 5 s."""
    def generate():
        sent = 0
        while True:
            mins = minutes_until_shutdown()
            lines = []
            if LOG_FILE.exists():
                lines = LOG_FILE.read_text(errors="replace").splitlines()[-5:]
            payload = json.dumps({"minutes_until_shutdown": mins, "recent_logs": lines, "seq": sent})
            yield f"data: {payload}\n\n"
            sent += 1
            time.sleep(5)
    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Dashboard ─────────────────────────────────────────────────────────────────
# Databricks Apps mounts source at /app/python/source_code/
# Use __file__ so the path is always correct regardless of working directory.
STATIC_DIR = Path(__file__).parent / "static"

@app.route("/")
def dashboard():
    touch_activity("dashboard-load")
    return send_from_directory(str(STATIC_DIR), "dashboard.html")


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


if __name__ == "__main__":
    # Databricks Apps injects $PORT; fall back to 8080 for local dev
    port = int(os.environ.get("PORT", 8080))
    logger.info("STARTUP: Databricks AI Dev Kit MCP Server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)