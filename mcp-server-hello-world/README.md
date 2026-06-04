# Databricks MCP Server App

Host the [AI Dev Kit](https://github.com/databricks-solutions/ai-dev-kit) MCP server as a Databricks App — letting you experience 80+ Databricks tools from the AI Playground, no local setup required.

## What This Is

A **3-file wrapper** that takes the open-source `databricks-mcp-server` from the [Databricks Solutions](https://github.com/databricks-solutions) team (stdio transport) and deploys it as a Databricks App with Streamable HTTP transport. The Playground auto-discovers all tools.

The app now also monitors inbound activity and will shut itself down after x minutes of inactivity.

```
app.py            # 4 lines — import server, expose as HTTP
app.yaml          # Databricks App config
requirements.txt  # Pull ai-dev-kit from GitHub
databricks.yml    # Databricks Asset Bundle config
```

## Setup

### Prerequisites
- Databricks CLI v0.229.0+ (`databricks --version`)
- A Databricks workspace with Apps enabled
- Authenticated CLI profile (`databricks auth login --host <url>`)

### Deploy

This project uses [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html) for deployment.

```bash
# Authenticate
databricks auth login --host https://your-workspace.cloud.databricks.com

# Validate the bundle
databricks bundle validate

# Deploy the app resource and sync source code
databricks bundle deploy

# Start the app (installs packages and launches the server)
databricks bundle run mcp_ai_dev_kit

# If using a named CLI profile, add --profile to each command:
databricks bundle deploy --profile <profile-name>
databricks bundle run mcp_ai_dev_kit --profile <profile-name>
```

> **Important:** The app name must start with `mcp-` for the Playground to discover it as a custom MCP server. The default name `mcp-ai-dev-kit` already handles this.

### Connect to AI Playground

1. Open your workspace → **AI Playground**
2. Select a model with the **Tools enabled** label
3. Click **Tools** → **Add tool** → **MCP Servers**
4. Add your app's MCP endpoint: `https://<app-url>/mcp`
5. The Playground auto-discovers all 80+ tools

## Demo Script: Usage Dashboard in 3 Prompts

Once connected in the Playground:

1. **"Query system.billing.usage and show me total DBUs by sku_name for the last 30 days"**
   → Uses SQL tools

2. **"Create a view called main.default.monthly_usage_summary that aggregates DBUs from system.billing.usage by month and sku_name"**
   → Uses SQL tools

3. **"Build a clean AI/BI dashboard that shows weekly and monthly usage trends from that view — a line chart for weekly DBUs over time and a bar chart for monthly DBUs by SKU"**
   → Uses Dashboard tools

Switch to the workspace UI — a published Lakeview dashboard, built from conversation.

## Architecture

```
AI Playground ──Streamable HTTP──▶ Databricks App (this repo)
                                        │
                                        ▼
                                  ai-dev-kit MCP Server
                                  (80+ tools via FastMCP)
                                        │
                                        ▼
                              Databricks APIs (SDK)
                              ├── SQL Warehouses
                              ├── Unity Catalog
                              ├── Jobs / Pipelines
                              ├── Vector Search
                              ├── Model Serving
                              ├── Agent Bricks
                              ├── AI/BI Dashboards
                              ├── Genie
                              └── ...
```
