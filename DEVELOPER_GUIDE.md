# DataOps Agent — Developer Guide

## Overview

The DataOps Agent is an AI-powered autonomous database operations assistant for Amazon Aurora PostgreSQL. It is deployed on **Amazon Bedrock AgentCore Runtime** and provides a conversational interface for database health monitoring, query performance tuning, and safe remediation actions.

The system consists of two files:

| File | Purpose |
|------|---------|
| `dataops_agent.py` | The agent backend — all tools, LLM orchestration, and AgentCore entrypoint |
| `dataops_ui.py` | Streamlit chat UI that invokes the deployed agent |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         User (Browser)                               │
│                              │                                       │
│                     ┌────────▼────────┐                             │
│                     │  dataops_ui.py  │  Streamlit Chat UI          │
│                     │  (Streamlit)    │                             │
│                     └────────┬────────┘                             │
│                              │ invoke_agent_runtime()                │
│                              ▼                                       │
│              ┌───────────────────────────────┐                      │
│              │  Amazon Bedrock AgentCore      │                      │
│              │  Runtime (Managed Container)   │                      │
│              │                               │                      │
│              │  ┌─────────────────────────┐  │                      │
│              │  │   dataops_agent.py       │  │                      │
│              │  │                         │  │                      │
│              │  │  ┌───────────────────┐  │  │                      │
│              │  │  │  Strands Agent    │  │  │                      │
│              │  │  │  (Claude Sonnet)  │  │  │                      │
│              │  │  │  + 27 Tools       │  │  │                      │
│              │  │  └────────┬──────────┘  │  │                      │
│              │  └───────────┼─────────────┘  │                      │
│              └──────────────┼────────────────┘                      │
│                             │                                        │
│              ┌──────────────┼──────────────────────┐                │
│              │              │                      │                 │
│              ▼              ▼                      ▼                 │
│   ┌──────────────┐  ┌────────────┐  ┌──────────────────────┐      │
│   │ Aurora        │  │ CloudWatch │  │ Performance Insights │      │
│   │ PostgreSQL    │  │ Metrics    │  │ (PI API)             │      │
│   │ (psycopg2)   │  │ (boto3)    │  │ (boto3)              │      │
│   └──────────────┘  └────────────┘  └──────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| LLM | Amazon Bedrock — Claude Sonnet 4 (`us.anthropic.claude-sonnet-4-20250514-v1:0`) |
| Agent Framework | [Strands Agents](https://github.com/strands-agents/strands-agents) |
| Runtime | Amazon Bedrock AgentCore Runtime |
| Database | Amazon Aurora PostgreSQL (connected via psycopg2 with SSL) |
| Metrics | AWS CloudWatch, RDS Performance Insights |
| Infrastructure | AWS RDS API (boto3) |
| UI | Streamlit |
| Deployment | bedrock-agentcore-starter-toolkit |

---

## dataops_agent.py — Detailed Breakdown

### Configuration (Lines 35–55)

All configuration is hardcoded at the top of the file — no `.env` file or environment variables needed:

```python
AWS_REGION         = "us-east-1"
BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-20250514-v1:0"
DB_HOST            = ""   # Aurora cluster endpoint
DB_PORT            = 5432
DB_NAME            = "postgres"
DB_USER            = "postgres"
DB_PASSWORD        = ""
DB_SSLMODE         = "verify-full"
AURORA_CLUSTER_ID  = ""   # For CloudWatch and PI tools
AURORA_INSTANCE_ID = ""   # For CloudWatch and PI tools
```

### Database Layer (Lines 58–145)

The agent connects directly to Aurora PostgreSQL using psycopg2 with TLS:

- `_load_secret()` — Optionally loads credentials from AWS Secrets Manager
- `_ensure_rds_ca_bundle()` — Auto-downloads the RDS global CA bundle for SSL verification
- `_get_db_params()` — Resolves connection parameters (Secrets Manager or hardcoded)
- `_get_connection()` — Returns a psycopg2 connection
- `execute_query(sql)` — Executes a read query, returns `list[dict]`
- `execute_command(sql)` — Executes a write command (DDL), returns status string
- `_is_safe_sql(sql)` — Validates SQL against blocked patterns (DROP, DELETE, TRUNCATE, UPDATE, GRANT, REVOKE)

### Tools (27 total)

The agent has 27 tools organized into 7 categories:

#### Category 1: Health Check (Read-Only) — 6 tools

| Tool | Description |
|------|-------------|
| `get_database_summary` | Database name, size, connections, max_connections, PG version |
| `get_largest_tables` | Top 10 tables by total disk usage (table + indexes) |
| `get_unused_indexes` | Indexes with zero scans (wasting space and slowing writes) |
| `get_table_bloat` | Tables with dead tuples, bloat percentage, last vacuum timestamps |
| `get_index_bloat` | Indexes sorted by size with scan/tuple stats |
| `get_top_queries` | Slowest queries via `aurora_stat_plans()` or `pg_stat_statements` fallback |

#### Category 2: Query Tuning & Explain Plan — 5 tools

| Tool | Description |
|------|-------------|
| `explain_query(query, analyze)` | Runs `EXPLAIN (FORMAT JSON, VERBOSE, COSTS, BUFFERS)` on SELECT queries. Safety-gated: blocks INSERT/UPDATE/DELETE/DROP. |
| `get_query_stats(query_substring, min_calls, sort_by)` | Queries pg_stat_statements with filtering and sorting (total_time, mean_time, calls, rows, shared_blks_hit, shared_blks_read) |
| `suggest_missing_indexes(table_name)` | Analyzes seq_scan ratio, existing indexes, and column statistics to recommend new indexes |
| `get_table_column_stats(table_name)` | Column-level distribution: n_distinct, correlation, null_fraction, most_common_values |
| `deep_analyze_query(query)` | **All-in-one diagnostic** — runs EXPLAIN + automated plan issue detection + pg_stat_statements + PI lookup + table analysis. Returns consolidated tuning_options. |

#### Category 3: Performance Insights — 4 tools

| Tool | Description |
|------|-------------|
| `get_pi_top_sql(period_minutes, max_results)` | Top SQL by DB load (Average Active Sessions) from PI API |
| `get_pi_wait_events(period_minutes)` | Wait events ranked by DB load (CPU, IO, Lock, LWLock, Client) |
| `get_pi_db_load_by_dimension(group_by, period_minutes)` | Slice DB load by: wait_event, sql, user, host, application, session_type |
| `get_pi_counter_metrics(period_minutes)` | Buffer cache hit ratio, TPS, tuples, checkpoint time, CPU, memory, connections |

#### Category 4: Aurora-Specific — 5 tools

| Tool | Description |
|------|-------------|
| `list_aurora_clusters` | All clusters with endpoints, members, encryption, deletion protection |
| `get_aurora_instance_details` | Instance class, AZ, engine version, PI status, enhanced monitoring |
| `get_aurora_replica_lag(db_instance_id, period_minutes)` | Reader replica lag from CloudWatch |
| `get_aurora_wait_events` | Live wait events from `pg_stat_activity` with sample queries |
| `get_aurora_active_sessions` | Active non-idle sessions with queries, durations, wait info |

#### Category 5: CloudWatch Metrics — 3 tools

| Tool | Description |
|------|-------------|
| `get_cloudwatch_cpu_utilization(db_instance_id, period_minutes)` | CPU % with avg/max at 5-min intervals |
| `get_cloudwatch_db_connections(db_instance_id, period_minutes)` | Connection count over time |
| `get_cloudwatch_storage_metrics(db_instance_id)` | Free storage (GB), Read/Write IOPS, Freeable Memory |

#### Category 6: AWS Documentation — 1 tool

| Tool | Description |
|------|-------------|
| `search_aws_docs(query, service)` | Searches AWS documentation using Bedrock LLM knowledge. Returns practical answers with parameter names, CLI examples, and references. |

#### Category 7: Safe Actions (Write) — 3 tools

| Tool | Description |
|------|-------------|
| `create_index_concurrently(table_name, column_names, index_name)` | Creates index without blocking reads/writes. Input-validated against SQL injection. |
| `analyze_table(table_name)` | Runs ANALYZE to update planner statistics |
| `vacuum_table(table_name)` | Runs VACUUM (non-full, non-blocking) to reclaim dead tuple space |

</text>
</invoke>

### Deep Analyze Query — The Core Intelligence

`deep_analyze_query` is the flagship tool. When a user pastes a slow query, it runs the entire diagnostic chain in a single call:

```
Step 1: EXPLAIN (FORMAT JSON) → execution plan
Step 2: _find_plan_issues() → automated detection of:
        - Seq Scans on large tables (severity: high if >10K rows)
        - Sort spilling to disk (external merge, temp blocks)
        - Nested Loops with high row counts (>50K)
        - Row estimate mismatches (actual vs planned >10x)
        - High-cost nodes (>100K cost units)
Step 3: pg_stat_statements → historical execution stats
Step 4: Performance Insights → check if query appears in PI top SQL
Step 5: Table analysis → scan stats + existing indexes for each table in plan
Step 6: Consolidated tuning_options → prioritized fix recommendations
```

Each tuning option includes:
- `option` — what to do (e.g., "Add index on 'orders'")
- `type` — immediate / short_term / medium_term
- `detail` — explanation with specific commands
- `impact` — high / medium / low

### Plan Issue Detection (`_find_plan_issues`)

The plan walker recursively traverses the EXPLAIN JSON tree and flags:

| Issue | Detection Logic | Severity |
|-------|----------------|----------|
| Seq Scan (large) | Node Type = "Seq Scan" AND Plan Rows > 10,000 | high |
| Seq Scan (small) | Node Type = "Seq Scan" AND Plan Rows ≤ 10,000 | medium |
| Sort spill | Node Type contains "Sort" AND (Sort Method = "external" OR Temp Written > 0) | high |
| Nested Loop (expensive) | Node Type = "Nested Loop" AND Plan Rows > 50,000 | high |
| Row estimate mismatch | Actual Rows / Plan Rows > 10x OR < 0.1x | medium |
| High-cost node | Total Cost > 100,000 | medium |
| Hash disk reads | Node Type contains "Hash" AND Shared Read Blocks > 1,000 | medium |

### Safety Layer

All write operations pass through `_is_safe_sql()` which blocks:
- `DROP TABLE/DATABASE/SCHEMA/INDEX` (but allows `DROP INDEX CONCURRENTLY`)
- `DELETE FROM`
- `TRUNCATE`
- `ALTER TABLE ... DROP`
- `UPDATE`
- `GRANT` / `REVOKE`

The `explain_query` and `deep_analyze_query` tools additionally block:
- Any query not starting with SELECT / WITH / VALUES
- Embedded DML keywords (INSERT, UPDATE, DELETE, DROP, TRUNCATE, ALTER, CREATE)

### Agent System Prompt

The system prompt (≈200 lines) contains:
1. Tool descriptions with usage guidance
2. Performance Insights workflow (8 steps)
3. Query tuning workflow (10 steps)
4. Wait Event → Root Cause → Fix mapping table (11 wait events)
5. Counter Metric → Threshold → Fix mapping table (8 metrics)
6. Diagnosis structure template (Diagnosis → Root Cause → Recommendations → Offer to Implement)
7. Safety rules

This ensures the agent doesn't just return raw data — it always interprets results and produces actionable recommendations.

### AgentCore Entrypoint

```python
@app.entrypoint
def invoke_dataops(payload, context=None):
```

Accepts a JSON payload:
```json
{
  "prompt": "user's natural language question",
  "action": "agent" | "health",
  "session_id": "uuid"
}
```

- `action: "health"` → directly calls `check_health()` (no LLM)
- `action: "agent"` → passes prompt to the Strands Agent which decides which tools to call

Returns:
```json
{
  "answer": "agent's markdown response",
  "action": "agent",
  "query": "original prompt"
}
```

---

## dataops_ui.py — Detailed Breakdown

### Configuration (Line 30)

```python
AGENT_ARN = ""  # Set after deployment
REGION = "us-east-1"
```

### UI Components

| Component | Description |
|-----------|-------------|
| Top Bar | Sticky header with title, subtitle, and connection status badge |
| Sidebar | New Session button, session ID display, agent ARN display |
| Quick Actions | 12 pre-built prompts shown when chat is empty (grid of buttons) |
| Chat History | Full message history with user/assistant bubbles |
| Chat Input | Text input at bottom with placeholder text |

### AgentCore Invocation

```python
def invoke_agent(prompt: str, action: str = "agent") -> dict:
```

- Calls `boto3.client("bedrock-agentcore").invoke_agent_runtime()`
- Retries up to 3 times on 500 errors with exponential backoff
- Parses the response body (handles string and dict responses)
- Returns the parsed response or error dict

### Styling

The UI uses Apple-inspired CSS with:
- Inter font family
- Blue user bubbles (#0071E3), gray assistant bubbles (#F5F5F7)
- Sticky frosted-glass top bar
- Responsive layout (mobile/tablet/desktop breakpoints)
- Hidden Streamlit chrome (menu, footer, toolbar)

---

## Deployment

### Prerequisites

```bash
pip install -r requirements.txt
```

Required packages:
- `strands-agents` — Agent framework
- `strands-agents-builder` — BedrockModel class
- `bedrock-agentcore` — AgentCore runtime SDK
- `bedrock-agentcore-starter-toolkit` — Deployment toolkit
- `boto3` — AWS SDK
- `psycopg2-binary` — PostgreSQL driver
- `streamlit` — UI framework
- `pytest` — Testing

### Deploy to AgentCore

```bash
python deploy_dataops_agent.py
```

This:
1. Configures the AgentCore runtime (ECR, execution role, container)
2. Packages `dataops_agent.py` + `requirements.txt` into a container
3. Deploys to AgentCore Runtime
4. Returns the Agent ARN

### Run the UI

After deployment, set the Agent ARN in `dataops_ui.py` and run:

```bash
streamlit run dataops_ui.py
```

### Local Testing (without AgentCore)

```bash
python dataops_agent.py
```

This starts the AgentCore app locally for development.

---

## Data Flow

### User asks "Why is this query slow?"

```
1. User types query in Streamlit UI
2. UI calls invoke_agent_runtime(payload={prompt: "...", action: "agent"})
3. AgentCore routes to invoke_dataops()
4. Strands Agent receives the prompt
5. LLM decides to call deep_analyze_query(query="SELECT ...")
6. deep_analyze_query runs:
   a. EXPLAIN → psycopg2 → Aurora PostgreSQL
   b. _find_plan_issues() → in-memory plan analysis
   c. pg_stat_statements query → psycopg2 → Aurora
   d. PI describe_dimension_keys → boto3 → Performance Insights API
   e. Table scan stats → psycopg2 → Aurora
7. Returns consolidated JSON to the agent
8. LLM interprets the results using system prompt guidance
9. Produces markdown response with:
   - Plan issues found
   - Historical stats
   - PI load data
   - Numbered tuning options with trade-offs
   - Offer to implement fixes
10. Response returned to UI
11. UI renders markdown in assistant bubble
```

### User says "Yes, create that index"

```
1. LLM calls create_index_concurrently(table, columns, name)
2. Tool validates inputs (regex check for SQL injection)
3. Tool calls _is_safe_sql() on the generated SQL
4. Tool calls execute_command() → psycopg2 → Aurora (autocommit)
5. Returns success/failure
6. LLM reports the result and offers to re-run explain_query to verify
```

---

## Security

| Layer | Protection |
|-------|-----------|
| SQL Injection | All table/column/index names validated with `^[a-zA-Z_][a-zA-Z0-9_]*$` regex |
| Dangerous SQL | `_is_safe_sql()` blocks DROP/DELETE/TRUNCATE/UPDATE/GRANT/REVOKE |
| DML in EXPLAIN | `explain_query` and `deep_analyze_query` only accept SELECT/WITH/VALUES |
| Database SSL | Connects with `sslmode=verify-full` using RDS global CA bundle |
| Secrets | Supports AWS Secrets Manager for credentials (optional) |
| Agent Safety | System prompt explicitly forbids destructive operations |
| Index Creation | Always uses CONCURRENTLY (non-blocking) |
| VACUUM | Always non-full (non-blocking) |

---

## Testing

### Automated (70+ unit tests)

```bash
pytest test_dataops_agent.py -v
```

All external dependencies (psycopg2, boto3, strands) are mocked. Tests cover:
- SQL safety validation (11 tests)
- All 27 tools with success and error paths
- Plan analysis helpers (12 tests for issue detection)
- Deep analyze query chain (10 tests)
- Input validation and SQL injection blocking

### Manual (50+ test cases)

See `testcases.md` — designed to be run from the Streamlit UI by copying prompts and verifying expected behavior.

---

## File Structure

```
.
├── dataops_agent.py          # Agent: 27 tools + system prompt + AgentCore entrypoint
├── dataops_ui.py             # Streamlit chat UI
├── deploy_dataops_agent.py   # One-command deployment to AgentCore
├── requirements.txt          # Python dependencies
├── test_dataops_agent.py     # 70+ automated unit tests
├── testcases.md              # 50+ manual test cases for UI validation
├── DEVELOPER_GUIDE.md        # This document
├── README.md                 # Quick start guide
└── .gitignore
```

---

## Extending the Agent

### Adding a New Tool

1. Define the function with `@tool` decorator in `dataops_agent.py`:
```python
@tool
def my_new_tool(param: str) -> str:
    """Description shown to the LLM.
    
    Args:
        param: What this parameter does.
    """
    # Implementation
    return json.dumps({"result": "..."})
```

2. Add it to the `ALL_TOOLS` list
3. Document it in the `AGENT_SYSTEM_PROMPT`
4. Add it to `check_health()` capabilities list
5. Write tests in `test_dataops_agent.py`
6. Add manual test cases to `testcases.md`

### Changing the LLM Model

Edit the `BEDROCK_MODEL_ID` constant at the top of `dataops_agent.py`:
```python
BEDROCK_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"  # Change this
```

### Adding a New Database

The agent is designed for Aurora PostgreSQL but can be adapted:
1. Replace psycopg2 with the appropriate driver
2. Update SQL queries in health check tools (they use PostgreSQL system catalogs)
3. Update the PI tools (they use the RDS PI API which works for any RDS engine)
4. Update the system prompt with engine-specific guidance

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Single-file agent | Simplifies AgentCore deployment (one entrypoint, no package structure) |
| Hardcoded config | No .env file to manage — single source of truth, deploys with the code |
| `deep_analyze_query` as all-in-one | Reduces LLM tool-calling overhead — one call instead of 5-6 sequential calls |
| Plan walker (`_find_plan_issues`) | Deterministic issue detection without LLM — faster, cheaper, more reliable |
| Safety-first SQL validation | Multiple layers: regex input validation + pattern blocking + query type restriction |
| System prompt with mapping tables | Ensures the agent always produces actionable recommendations, not just raw data |
| Streamlit UI (not React) | Simple to run (`streamlit run`), no build step, easy to customize |
| No authentication in UI | Designed for internal/demo use — add auth as needed for production |
