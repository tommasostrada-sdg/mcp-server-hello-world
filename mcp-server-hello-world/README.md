# Databricks AI Dev Kit — Custom MCP Server

A production-ready MCP (Model Context Protocol) server deployed as a **Databricks App**, exposing Genie and AI Dev Kit tools for improving code quality, pipeline generation, and workspace management.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                Databricks App (port 8080)            │
│                                                      │
│  ┌──────────────┐   ┌─────────────────────────────┐ │
│  │  Dashboard   │   │    MCP Server (Flask)        │ │
│  │  (React SPA) │   │                              │ │
│  │  • Timer     │   │  POST /mcp  (JSON-RPC 2.0)  │ │
│  │  • Tool UI   │   │  POST /api/invoke  (REST)   │ │
│  │  • Live logs │   │  GET  /api/status            │ │
│  │  • SSE feed  │   │  GET  /api/stream  (SSE)    │ │
│  └──────────────┘   └─────────────────────────────┘ │
└─────────────────────────────────────────────────────┘
```

---

## Tools Included

### 🔮 Genie (SQL & Notebook Intelligence)
| Tool | Description |
|------|-------------|
| `genie_query_explain` | Explain + optimise SQL/PySpark queries |
| `genie_schema_advisor` | Delta table schema + partitioning advice |
| `genie_notebook_review` | Notebook code quality & secret leak scan |

### ⚙️ AI Dev Kit (Code Generation)
| Tool | Description |
|------|-------------|
| `aidevkit_generate_pipeline` | Generate DLT (Delta Live Tables) pipeline |
| `aidevkit_mlflow_scaffold` | Scaffold MLflow experiment + serving config |
| `aidevkit_dbt_gen` | Generate dbt model SQL + schema.yml |
| `aidevkit_secret_scanner` | Scan code for leaked secrets/PATs |

### 🌐 Space (Workspace Helpers)
| Tool | Description |
|------|-------------|
| `space_activity_check` | Read server logs, show time until shutdown |
| `space_keep_alive` | Reset the idle timer from a notebook |

### 🧪 Test
| Tool | Description |
|------|-------------|
| `test_echo` | Verify MCP server is reachable and working |

---

## Quick Start

### 1. Local dev

```bash
pip install -r requirements.txt
python app.py
# Open http://localhost:8080
```

### 2. Run tests

```bash
pip install pytest
pytest tests/test_mcp_server.py -v
```

### 3. Deploy to Databricks Apps

```bash
# Install CLI
pip install databricks-cli

# Authenticate
databricks configure --token

# Deploy
databricks apps deploy --source-code-path . my-mcp-server

# Check status
databricks apps get my-mcp-server
```

---

## MCP Protocol Usage

### Initialize

```json
POST /mcp
{
  "jsonrpc": "2.0", "id": 1,
  "method": "initialize",
  "params": { "clientInfo": { "name": "my-client" } }
}
```

### List tools

```json
{ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }
```

### Call a tool

```json
{
  "jsonrpc": "2.0", "id": 3,
  "method": "tools/call",
  "params": {
    "name": "genie_query_explain",
    "arguments": {
      "query": "SELECT * FROM orders WHERE year = 2024",
      "dialect": "sql",
      "focus": "optimise"
    }
  }
}
```

---

## Keeping the App Alive

Databricks Apps shut down after **30 minutes of inactivity**. To prevent this from a notebook:

```python
import requests, time

MCP_URL = "https://<your-app>.databricksapps.com"

def keep_alive():
    requests.post(f"{MCP_URL}/api/keepalive")

# Call every 20 minutes in your notebook
while True:
    keep_alive()
    time.sleep(20 * 60)
```

Or use the MCP tool directly:

```python
requests.post(f"{MCP_URL}/mcp", json={
    "jsonrpc": "2.0", "id": 1,
    "method": "tools/call",
    "params": { "name": "space_keep_alive", "arguments": {} }
})
```

---

## Activity Monitoring

The server writes structured logs to `mcp_server.log`. The dashboard polls these via SSE every 5 seconds and displays:

- **Ring timer** showing minutes until shutdown
- **Color coding**: green (>10 min) → yellow (<10 min) → red/pulsing (<5 min)  
- **Live log tail** with ACTIVITY events highlighted in green

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IDLE_TIMEOUT_MINUTES` | `30` | Match your Databricks Apps idle timeout |
| `PORT` | `8080` | Server port (set by Databricks Apps) |