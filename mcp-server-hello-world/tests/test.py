"""
tests/test_mcp_server.py  —  pytest test suite for the Databricks AI Dev Kit MCP server.

Run:
    pip install pytest flask flask-cors
    pytest tests/test_mcp_server.py -v
"""

import json
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
from app import app, TOOLS, minutes_until_shutdown, touch_activity


@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as c:
        yield c


# ── Health ────────────────────────────────────────────────────────────────────

def test_health(client):
    r = client.get('/health')
    assert r.status_code == 200
    assert r.get_json()['status'] == 'ok'


# ── MCP Protocol ──────────────────────────────────────────────────────────────

def mcp(client, method, params=None):
    payload = {'jsonrpc': '2.0', 'id': 1, 'method': method, 'params': params or {}}
    r = client.post('/mcp', data=json.dumps(payload), content_type='application/json')
    assert r.status_code == 200
    return r.get_json()


def test_mcp_initialize(client):
    d = mcp(client, 'initialize', {'clientInfo': {'name': 'pytest'}})
    assert d['result']['protocolVersion'] is not None
    assert d['result']['serverInfo']['name'] == 'databricks-ai-devkit-mcp'


def test_mcp_tools_list(client):
    d = mcp(client, 'tools/list')
    tools_returned = d['result']['tools']
    assert len(tools_returned) == len(TOOLS)
    names = [t['name'] for t in tools_returned]
    assert 'test_echo' in names
    assert 'genie_query_explain' in names


def test_mcp_ping(client):
    d = mcp(client, 'ping')
    assert d['result']['pong'] is True


def test_mcp_unknown_method(client):
    d = mcp(client, 'nonexistent/method')
    assert 'error' in d


# ── Tool: test_echo ───────────────────────────────────────────────────────────

def test_tool_echo_basic(client):
    d = mcp(client, 'tools/call', {'name': 'test_echo', 'arguments': {'message': 'hello'}})
    result = json.loads(d['result']['content'][0]['text'])
    assert result['echo'] == 'hello'
    assert result['status'] == 'MCP server operational ✓'


def test_tool_echo_server_info(client):
    d = mcp(client, 'tools/call', {'name': 'test_echo', 'arguments': {'include_server_info': True}})
    result = json.loads(d['result']['content'][0]['text'])
    assert 'server_info' in result
    assert result['server_info']['tool_count'] > 0


def test_tool_unknown(client):
    d = mcp(client, 'tools/call', {'name': 'nonexistent_tool', 'arguments': {}})
    assert 'error' in d


# ── Tool: genie_query_explain ─────────────────────────────────────────────────

def test_genie_query_explain_basic(client):
    r = client.post('/api/invoke', json={
        'tool': 'genie_query_explain',
        'args': {'query': 'SELECT * FROM orders', 'dialect': 'sql'}
    })
    data = r.get_json()['result']
    assert 'bottlenecks' in data
    # Should detect SELECT *
    assert any('SELECT *' in s for s in data['bottlenecks'])


def test_genie_query_explain_with_filter(client):
    r = client.post('/api/invoke', json={
        'tool': 'genie_query_explain',
        'args': {'query': 'SELECT id, name FROM orders WHERE status = "active"'}
    })
    data = r.get_json()['result']
    # No SELECT * bottleneck
    assert not any('SELECT *' in s for s in data['bottlenecks'])


# ── Tool: genie_schema_advisor ────────────────────────────────────────────────

def test_genie_schema_advisor(client):
    schema = json.dumps({'fields': [{'name': 'id', 'type': 'long'}, {'name': 'event_date', 'type': 'date'}]})
    r = client.post('/api/invoke', json={
        'tool': 'genie_schema_advisor',
        'args': {'schema_json': schema, 'table_name': 'events'}
    })
    data = r.get_json()['result']
    assert data['table'] == 'events'
    assert len(data['recommendations']) > 0


# ── Tool: genie_notebook_review ───────────────────────────────────────────────

def test_notebook_review_clean(client):
    src = "# COMMAND ----------\ndf = spark.read.parquet('/data')\ndf.write.delta('/output')"
    r = client.post('/api/invoke', json={
        'tool': 'genie_notebook_review',
        'args': {'notebook_source': src}
    })
    data = r.get_json()['result']
    assert any(f['severity'] == 'OK' for f in data['findings'])


def test_notebook_review_detects_secret(client):
    src = "password = 'SuperSecret123'\ndf = spark.read.parquet('/data')"
    r = client.post('/api/invoke', json={
        'tool': 'genie_notebook_review',
        'args': {'notebook_source': src}
    })
    data = r.get_json()['result']
    assert any(f['severity'] == 'CRITICAL' for f in data['findings'])


# ── Tool: aidevkit_generate_pipeline ──────────────────────────────────────────

def test_aidevkit_generate_pipeline(client):
    r = client.post('/api/invoke', json={
        'tool': 'aidevkit_generate_pipeline',
        'args': {'description': 'Ingest clickstream events', 'target_table': 'clicks', 'source_format': 'autoloader'}
    })
    data = r.get_json()['result']
    assert 'pipeline_code' in data
    assert 'bronze_clicks' in data['tables_generated']
    assert 'gold_clicks' in data['tables_generated']
    assert 'import dlt' in data['pipeline_code']


# ── Tool: aidevkit_mlflow_scaffold ────────────────────────────────────────────

def test_mlflow_scaffold(client):
    r = client.post('/api/invoke', json={
        'tool': 'aidevkit_mlflow_scaffold',
        'args': {'model_type': 'sklearn', 'experiment_name': 'fraud_detection'}
    })
    data = r.get_json()['result']
    assert 'mlflow' in data['scaffold_code']
    assert data['experiment'] == 'fraud_detection'


# ── Tool: aidevkit_dbt_gen ────────────────────────────────────────────────────

def test_dbt_gen(client):
    r = client.post('/api/invoke', json={
        'tool': 'aidevkit_dbt_gen',
        'args': {'source': 'SELECT * FROM raw.orders', 'model_name': 'orders_clean', 'materialization': 'incremental'}
    })
    data = r.get_json()['result']
    assert 'orders_clean' in data['model_sql']
    assert 'incremental' in data['model_sql']
    assert 'schema.yml' not in data['model_sql']  # schema in separate key
    assert 'version: 2' in data['schema_yml']


# ── Tool: aidevkit_secret_scanner ────────────────────────────────────────────

def test_secret_scanner_clean(client):
    r = client.post('/api/invoke', json={
        'tool': 'aidevkit_secret_scanner',
        'args': {'source_code': 'print("hello world")', 'file_name': 'hello.py'}
    })
    data = r.get_json()['result']
    assert data['clean'] is True


def test_secret_scanner_detects_pat(client):
    r = client.post('/api/invoke', json={
        'tool': 'aidevkit_secret_scanner',
        'args': {'source_code': 'token = dapiABCDEF1234567890abcdef1234567890XX', 'file_name': 'creds.py'}
    })
    data = r.get_json()['result']
    assert data['clean'] is False
    assert any('PAT' in f['type'] or 'token' in f['type'].lower() for f in data['findings'])


# ── Tool: space_activity_check ────────────────────────────────────────────────

def test_space_activity_check(client):
    r = client.post('/api/invoke', json={
        'tool': 'space_activity_check',
        'args': {'last_n': 5}
    })
    data = r.get_json()['result']
    assert 'minutes_until_shutdown' in data
    assert data['minutes_until_shutdown'] >= 0


# ── Tool: space_keep_alive ────────────────────────────────────────────────────

def test_keep_alive_resets_timer(client):
    import time
    time.sleep(0.1)  # ensure some elapsed time
    r = client.post('/api/invoke', json={'tool': 'space_keep_alive', 'args': {}})
    data = r.get_json()['result']
    assert data['status'] == 'ok'
    # Timer should be near IDLE_TIMEOUT
    assert data['minutes_until_shutdown'] > 29


# ── REST API endpoints ────────────────────────────────────────────────────────

def test_api_status(client):
    r = client.get('/api/status')
    d = r.get_json()
    assert 'minutes_until_shutdown' in d
    assert d['tool_count'] == len(TOOLS)


def test_api_logs(client):
    r = client.get('/api/logs?n=10')
    d = r.get_json()
    assert 'lines' in d


def test_api_keepalive(client):
    r = client.post('/api/keepalive')
    d = r.get_json()
    assert d['ok'] is True


def test_dashboard_loads(client):
    r = client.get('/')
    assert r.status_code == 200
    assert b'Databricks AI Dev Kit' in r.data


# ── Timer logic ───────────────────────────────────────────────────────────────

def test_timer_decreases_over_time():
    import time
    touch_activity('test')
    t1 = minutes_until_shutdown()
    time.sleep(0.2)
    t2 = minutes_until_shutdown()
    assert t2 <= t1


def test_touch_resets_timer():
    import time
    time.sleep(0.3)
    touch_activity('test-reset')
    assert minutes_until_shutdown() > 29.9


# ── All tools registered ──────────────────────────────────────────────────────

def test_all_tools_have_executors():
    from app import EXECUTORS
    for name in TOOLS:
        assert name in EXECUTORS, f"Tool '{name}' has no executor"


def test_all_tools_have_schema():
    for name, tool in TOOLS.items():
        assert 'inputSchema' in tool, f"Tool '{name}' missing inputSchema"
        assert 'description' in tool, f"Tool '{name}' missing description"
        assert 'category' in tool, f"Tool '{name}' missing category"


if __name__ == '__main__':
    pytest.main([__file__, '-v', '--tb=short'])