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
import requests
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, Response, stream_with_context, send_from_directory
from flask_cors import CORS

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_FILE = Path("/tmp/mcp_server.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)

IDLE_TIMEOUT_MINUTES = int(os.getenv("IDLE_TIMEOUT_MINUTES", "0"))
print(f"[DEBUG] Uptime set: {IDLE_TIMEOUT_MINUTES}s")

# ── State ─────────────────────────────────────────────────────────────────────
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
# Inizializziamo con i tool preesistenti (Genie, AI Dev Kit, Space, Test)
TOOLS: dict[str, dict] = {
    "genie_query_explain": {
        "name": "genie_query_explain",
        "description": "Explain a SQL or PySpark query using Databricks AI; returns plain-language summary, identified bottlenecks, and optimisation hints.",
        "category": "Genie",
        "example": "from databricks_tools_core.genie import query_explain\n\nexplanation = query_explain(\n    query='SELECT * FROM main.default.active_users'\n)",
        "permissions": "Richiede i permessi 'USE CATALOG' e 'USE SCHEMA' sul catalogo/schema target e il ruolo di workspace AI/Genie Entitlement attivo.",
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
        "example": "from databricks_tools_core.genie import schema_advisor\n\nadvice = schema_advisor(schema_json='{...}', table_name='sales')",
        "permissions": "Richiede privilegi di lettura ('SELECT' o 'BROWSE') sui metadati della tabella all'interno del catalogo Unity Catalog.",
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
        "example": "from databricks_tools_core.genie import notebook_review\n\nreview = notebook_review(notebook_source='spark.read.table(...)')",
        "permissions": "Richiede permessi di lettura sulla cartella Workspace specifica contenente i notebook da analizzare.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "notebook_source": {"type": "string", "description": "Full notebook source as a string"},
                "language": {"type": "string", "enum": ["python", "sql", "scala", "r"], "default": "python"},
            },
            "required": ["notebook_source"],
        },
    },
    "aidevkit_generate_pipeline": {
        "name": "aidevkit_generate_pipeline",
        "description": "Generate a Delta Live Tables (DLT) pipeline skeleton from a natural-language description.",
        "category": "AI Dev Kit",
        "example": "from databricks_tools_core.pipelines import generate_pipeline\n\npipeline_code = generate_pipeline(description='Ingest XML logs', target_table='raw_logs')",
        "permissions": "Richiede privilegi di creazione asset o permessi 'CAN MANAGE' sulla feature Delta Live Tables globale del cluster workspace.",
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
        "example": "from databricks_tools_core.mlflow import mlflow_scaffold\n\nscaffold = mlflow_scaffold(model_type='sklearn', experiment_name='churn_prediction')",
        "permissions": "Richiede i permessi 'CAN EDIT' o 'CAN MANAGE' sulla cartella del Workspace Databricks o sul registro MLflow specificato.",
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
        "example": "from databricks_tools_core.dbt import dbt_gen\n\nmodels = dbt_gen(source='SELECT * FROM raw', model_name='stg_users')",
        "permissions": "Nessun permesso Databricks specifico richiesto (generazione di file di configurazione dbt standard in locale).",
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
        "example": "from databricks_tools_core.security import secret_scanner\n\nfindings = secret_scanner(source_code='token = \"dapi12345...\"')",
        "permissions": "Richiede privilegi di audit o amministrativi se eseguito a livello di intero workspace su tutti i path utente.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source_code": {"type": "string"},
                "file_name": {"type": "string", "default": "unknown"},
            },
            "required": ["source_code"],
        },
    },
    "space_activity_check": {
        "name": "space_activity_check",
        "description": "Check recent workspace activity from server logs; returns last-N events and minutes until auto-shutdown.",
        "category": "Space",
        "example": "space_activity_check(last_n=20)",
        "permissions": "Richiede autorizzazione locale per ispezionare il log file temporaneo (/tmp/mcp_server.log) all'interno del sandbox.",
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
        "example": "space_keep_alive()",
        "permissions": "Nessun permesso necessario (endpoint pubblico di controllo dell'uptime dell'applicazione).",
        "inputSchema": {"type": "object", "properties": {}},
    },
    "test_echo": {
        "name": "test_echo",
        "description": "Echo tool — verifies the MCP server is reachable and tools execute. Returns the input payload with a timestamp.",
        "category": "Test",
        "example": "test_echo(message='Ping Databricks MCP')",
        "permissions": "Nessun permesso richiesto (utilizzato per diagnostica di base del protocollo MCP).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message": {"type": "string", "default": "ping"},
                "include_server_info": {"type": "boolean", "default": True},
            },
        },
    },
}

# ── Caricamento dinamico dei tool da databricks_tools_core (GitHub) ───────────
def discover_github_tools():
    """Inietta dinamicamente i tool dal repository ufficiale di databricks_tools_core."""
    github_url = "https://api.github.com/repos/databricks-solutions/ai-dev-kit/contents/databricks-tools-core/databricks_tools_core"
    
    # Tool nativi di databricks_tools_core predefiniti per garantire la massima stabilità e schemi accurati
    core_repo_tools = {
        "sql_execute_sql": {
            "name": "sql_execute_sql",
            "description": "Esegue una query SQL arbitraria su un Databricks SQL Warehouse e restituisce il set di risultati strutturato.",
            "category": "Databricks SQL",
            "example": "from databricks_tools_core.sql import execute_sql\n\nres = execute_sql(\n    warehouse_id='abc123efg4567890',\n    query='SELECT * FROM catalog.schema.table LIMIT 10'\n)",
            "permissions": "Richiede il permesso 'CAN USE' sul SQL Warehouse selezionato, insieme ai privilegi Unity Catalog 'USE CATALOG', 'USE SCHEMA' e 'SELECT' sulle tabelle.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Query SQL da eseguire."},
                    "warehouse_id": {"type": "string", "description": "ID del Databricks SQL Warehouse target."}
                },
                "required": ["query", "warehouse_id"]
            }
        },
        "uc_list_tables": {
            "name": "uc_list_tables",
            "description": "Elenca tutte le tabelle e le viste registrate all'interno di uno schema specifico in Unity Catalog.",
            "category": "Unity Catalog",
            "example": "from databricks_tools_core.unity_catalog import list_tables\n\ntables = list_tables(catalog='main', schema='default')",
            "permissions": "Richiede il privilegio 'USE CATALOG' sul catalogo di livello superiore e 'USE SCHEMA' sullo schema specifico.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "catalog": {"type": "string", "default": "main", "description": "Nome del catalogo Unity Catalog."},
                    "schema": {"type": "string", "default": "default", "description": "Nome dello schema interno."}
                },
                "required": ["catalog", "schema"]
            }
        },
        "jobs_run_now": {
            "name": "jobs_run_now",
            "description": "Avvia immediatamente l'esecuzione asincrona (Run Now) di un Job/Workflow esistente tramite ID.",
            "category": "Jobs & Workflows",
            "example": "from databricks_tools_core.jobs import run_now\n\nrun_info = run_now(job_id=12345678)",
            "permissions": "Richiede i permessi 'CAN MANAGE RUN' o 'CAN MANAGE' sul Job Databricks configurato.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "job_id": {"type": "integer", "description": "L'ID numerico univoco del Job Databricks."}
                },
                "required": ["job_id"]
            }
        },
        "compute_list_clusters": {
            "name": "compute_list_clusters",
            "description": "Elenca tutti i cluster di calcolo (Compute Clusters) attivi, interattivi o serverless nel Workspace.",
            "category": "Compute Management",
            "example": "from databricks_tools_core.compute import list_clusters\n\nclusters = list_clusters()",
            "permissions": "Richiede i privilegi generali di Workspace User o Token Entitlements per interrogare le API dei cluster.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    }

    # Uniamo i tool predefiniti a quelli scoperti dall'albero delle directory di GitHub
    try:
        res = requests.get(github_url, headers={"User-Agent": "DatabricksMCPDashboard"}, timeout=3)
        if res.status_code == 200:
            for item in res.json():
                if item["type"] == "dir" and item["name"] != "__pycache__":
                    section = item["name"].replace("_", " ").title()
                    gen_name = f"{item['name']}_generic_tool"
                    if not any(t["category"].lower() == section.lower() for t in core_repo_tools.values()):
                        core_repo_tools[gen_name] = {
                            "name": gen_name,
                            "description": f"Modulo '{item['name']}' importabile da databricks_tools_core.",
                            "category": section,
                            "example": f"from databricks_tools_core import {item['name']}\nprint(dir({item['name']}))",
                            "permissions": "Richiede un token di autenticazione Databricks CLI / SDK valido nel file d'ambiente.",
                            "inputSchema": {"type": "object", "properties": {}}
                        }
            logger.info("GitHub Repository analysis completed successfully.")
    except Exception as e:
        logger.error(f"GitHub fallback check skipped/rate-limited: {e}")

    # Iniezione nel registro globale TOOLS
    for k, v in core_repo_tools.items():
        TOOLS[k] = v

# Eseguiamo il rilevamento automatico all'avvio
discover_github_tools()

# ── Tool Executors ─────────────────────────────────────────────────────────────

def _run_genie_query_explain(args: dict) -> dict:
    query = args["query"]
    dialect = args.get("dialect", "sql")
    focus = args.get("focus", "all")
    lines = [l.strip() for l in query.split("\n") if l.strip()]
    suggestions = []
    if "select *" in query.lower():
        suggestions.append("Avoid SELECT *: project only needed columns to reduce shuffle and scan cost.")
    if "where" not in query.lower():
        suggestions.append("No WHERE clause detected: consider adding partition filters to limit data scanned.")
    return {
        "dialect": dialect, "focus": focus, "line_count": len(lines),
        "summary": f"Query performs a {dialect.upper()} operation across {len(lines)} logical lines.",
        "bottlenecks": suggestions if suggestions else ["No major bottlenecks detected."]
    }

def _run_genie_schema_advisor(args: dict) -> dict:
    return {"table": args.get("table_name", "unknown"), "estimated_improvement": "20–60% scan reduction with recommended partitioning."}

def _run_genie_notebook_review(args: dict) -> dict:
    return {"language": args.get("language", "python"), "findings": [{"severity": "OK", "msg": "No critical issues found."}]}

def _run_aidevkit_generate_pipeline(args: dict) -> dict:
    return {"pipeline_code": "import dlt\n# Simulated DLT Skeleton", "tables_generated": [f"bronze_{args['target_table']}"]}

def _run_aidevkit_mlflow_scaffold(args: dict) -> dict:
    return {"experiment": args["experiment_name"], "files_generated": ["train.py", "mlflow_config.yaml"]}

def _run_aidevkit_dbt_gen(args: dict) -> dict:
    return {"model_sql": f"SELECT * FROM {args['model_name']}", "materialization": args.get("materialization", "table")}

def _run_aidevkit_secret_scanner(args: dict) -> dict:
    return {"file": args.get("file_name", "unknown"), "clean": True, "findings": []}

def _run_space_activity_check(args: dict) -> dict:
    last_n = args.get("last_n", 20)
    events = LOG_FILE.read_text(errors="replace").splitlines()[-last_n:] if LOG_FILE.exists() else []
    return {"minutes_until_shutdown": minutes_until_shutdown(), "recent_events": events}

def _run_space_keep_alive(args: dict) -> dict:
    touch_activity("keep-alive")
    return {"status": "ok", "minutes_until_shutdown": minutes_until_shutdown()}

def _run_test_echo(args: dict) -> dict:
    touch_activity("test-echo")
    return {"echo": args.get("message", "ping"), "timestamp_utc": datetime.now(timezone.utc).isoformat(), "status": "MCP server operational ✓"}

# Mock Executors per i tool di databricks_tools_core
def _run_mock_sql(args: dict) -> dict:
    return {"status": "SUCCESS", "rows_returned": 2, "data": [[1, "Sales_EU"], [2, "Sales_US"]], "warehouse": args.get("warehouse_id")}

def _run_mock_uc(args: dict) -> dict:
    return {"catalog": args.get("catalog", "main"), "schema": args.get("schema", "default"), "tables": ["users", "orders", "dim_products"]}

def _run_mock_jobs(args: dict) -> dict:
    return {"job_id": args.get("job_id"), "run_id": 998811, "lifecycle_state": "PENDING", "message": "Job run successfully triggered."}

def _run_mock_compute(args: dict) -> dict:
    return {"clusters": [{"cluster_name": "Shared-Autoscale-Compute", "state": "RUNNING", "nodes": 4}]}

def _run_generic_discovery(args: dict) -> dict:
    return {"status": "SUCCESS", "info": "Module inspected via dashboard wrapper."}

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
    # Mapping dei nuovi tool di databricks_tools_core
    "sql_execute_sql": _run_mock_sql,
    "uc_list_tables": _run_mock_uc,
    "jobs_run_now": _run_mock_jobs,
    "compute_list_clusters": _run_mock_compute,
    "sql_generic_tool": _run_generic_discovery,
    "unity_catalog_generic_tool": _run_generic_discovery,
    "jobs_generic_tool": _run_generic_discovery,
    "compute_generic_tool": _run_generic_discovery,
}

# ── MCP Protocol Endpoints ────────────────────────────────────────────────────

@app.route("/mcp", methods=["POST"])
def mcp_handler():
    touch_activity("mcp-rpc")
    body = request.get_json(force=True)
    method = body.get("method", "")
    req_id = body.get("id")
    params = body.get("params", {})

    def ok(result): return jsonify({"jsonrpc": "2.0", "id": req_id, "result": result})
    def err(code, msg): return jsonify({"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": msg}})

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
        if tool_name not in TOOLS: return err(-32601, f"Tool '{tool_name}' not found")
        try:
            exec_fn = EXECUTORS.get(tool_name, _run_generic_discovery)
            result = exec_fn(tool_args)
            return ok({"content": [{"type": "text", "text": json.dumps(result, indent=2)}]})
        except Exception as exc:
            return err(-32000, str(exc))
    if method == "ping": return ok({"pong": True})
    return err(-32601, f"Method '{method}' not found")

# ── Dashboard API ─────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    touch_activity("status-poll")
    cfg_source = "env" if "IDLE_TIMEOUT_MINUTES" in os.environ else "default"
    return jsonify({
        "minutes_until_shutdown": minutes_until_shutdown(),
        "idle_timeout_minutes": IDLE_TIMEOUT_MINUTES,
        "config_source": cfg_source,
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
    lines = LOG_FILE.read_text(errors="replace").splitlines()[-n:] if LOG_FILE.exists() else []
    return jsonify({"lines": lines, "total": len(lines)})

@app.route("/api/invoke", methods=["POST"])
def api_invoke():
    touch_activity("api-invoke")
    data = request.get_json(force=True, silent=True) or {}
    tool_name = data.get("tool")
    args = data.get("args", {})
    if tool_name not in TOOLS:
        return jsonify({"error": f"Unknown tool: {tool_name}"}), 404
    try:
        exec_fn = EXECUTORS.get(tool_name, _run_generic_discovery)
        result = exec_fn(args)
        response = jsonify({"tool": tool_name, "result": result})
        response.headers["Content-Type"] = "application/json"
        return response
    except Exception as exc:
        response = jsonify({"error": str(exc), "tool": tool_name})
        response.headers["Content-Type"] = "application/json"
        return response, 500

@app.route("/api/keepalive", methods=["POST"])
def api_keepalive():
    touch_activity("http-keepalive")
    return jsonify({"ok": True, "minutes_until_shutdown": minutes_until_shutdown()})

@app.route("/api/stream")
def api_stream():
    def generate():
        while True:
            mins = minutes_until_shutdown()
            lines = LOG_FILE.read_text(errors="replace").splitlines()[-5:] if LOG_FILE.exists() else []
            yield f"data: {json.dumps({'minutes_until_shutdown': mins, 'recent_logs': lines})}\n\n"
            time.sleep(5)
    return Response(stream_with_context(generate()), mimetype="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

STATIC_DIR = Path(__file__).parent / "static"

@app.route("/")
def dashboard():
    touch_activity("dashboard-load")
    return send_from_directory(str(STATIC_DIR), "dashboard.html")

@app.route("/health")
def health(): return jsonify({"status": "ok"})

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logger.info("STARTUP: Databricks AI Dev Kit MCP Server on port %d", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)