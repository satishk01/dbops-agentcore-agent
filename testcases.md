# DataOps Agent — Manual Test Cases

These test cases are designed to be run from the Streamlit chat UI (`dataops_ui.py`).
Copy each prompt into the chat input and verify the expected behavior.

> **Prerequisites:** Agent deployed to AgentCore, AGENT_ARN set, database connected, Performance Insights enabled.

---

## 1. System Health

### TC-1.1: Health Check
**Prompt:** `Check the system health`
**Expected:**
- Returns status (healthy/degraded)
- Shows model ID, database connection status
- Lists all available capabilities (27 tools)

### TC-1.2: Database Summary
**Prompt:** `Give me a database summary`
**Expected:**
- Returns database name, size, active connections, max connections, PostgreSQL version
- No errors

### TC-1.3: Full Health Check
**Prompt:** `Run a full database health check`
**Expected:**
- Agent calls multiple tools: database summary, largest tables, bloat, unused indexes
- Produces a structured report with findings and recommendations
- Prioritizes issues by severity

---

## 2. Table & Index Analysis

### TC-2.1: Largest Tables
**Prompt:** `Show me the top 10 largest tables`
**Expected:**
- Returns table names with total size, table size, index size, row estimates
- Sorted by total size descending

### TC-2.2: Unused Indexes
**Prompt:** `Find all unused indexes in the database`
**Expected:**
- Returns indexes with zero scans
- Shows index size (wasted space)
- Excludes primary keys and unique indexes

### TC-2.3: Table Bloat
**Prompt:** `Check for table bloat — which tables need vacuuming?`
**Expected:**
- Returns tables with dead tuples and bloat percentage
- Shows last vacuum/autovacuum/analyze timestamps
- Agent recommends VACUUM for tables with high bloat

### TC-2.4: Index Bloat
**Prompt:** `Show me bloated indexes`
**Expected:**
- Returns indexes sorted by size
- Shows scan count and tuple stats

---

## 3. Query Performance & Tuning

### TC-3.1: Top Slow Queries
**Prompt:** `Show me the slowest queries in the database`
**Expected:**
- Returns top queries by execution time
- Uses aurora_stat_plans if available, falls back to pg_stat_statements
- Shows query text, calls, total time, execution plan (if aurora)

### TC-3.2: Explain a Simple Query
**Prompt:** `Explain this query: SELECT * FROM orders WHERE status = 'pending'`
**Expected:**
- Returns JSON execution plan
- Agent analyzes the plan and identifies issues (e.g., Seq Scan)
- Recommends index if Seq Scan on large table

### TC-3.3: Explain with ANALYZE
**Prompt:** `Run EXPLAIN ANALYZE on: SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.total > 1000`
**Expected:**
- Returns plan with actual timing and row counts
- Agent compares planned vs actual rows
- Identifies join strategy and whether it's optimal

### TC-3.4: Explain Blocks DML (Safety)
**Prompt:** `Explain this query: DELETE FROM orders WHERE id = 1`
**Expected:**
- Agent refuses to explain the query
- Returns error: only SELECT/WITH/VALUES allowed
- Does NOT execute the DELETE

### TC-3.5: Explain Blocks Injection
**Prompt:** `Explain this query: SELECT 1; DROP TABLE orders`
**Expected:**
- Agent detects the embedded DROP and blocks it
- Returns safety error

### TC-3.6: Query Stats from pg_stat_statements
**Prompt:** `Show me query statistics for queries hitting the orders table`
**Expected:**
- Returns queries matching "orders" from pg_stat_statements
- Shows calls, total time, mean time, cache hit %, temp blocks
- Sorted by total execution time

### TC-3.7: Query Stats Sorted by Calls
**Prompt:** `What are the most frequently called queries?`
**Expected:**
- Returns queries sorted by call count
- Shows execution stats for each

### TC-3.8: Suggest Missing Indexes
**Prompt:** `Suggest missing indexes for the orders table`
**Expected:**
- Returns sequential scan percentage, row count, table size
- Lists existing indexes with scan counts
- Shows column statistics (n_distinct, correlation)
- Agent recommends specific columns to index based on the data

### TC-3.9: Table Column Stats
**Prompt:** `Show me column statistics for the orders table`
**Expected:**
- Returns per-column: data type, n_distinct, null fraction, avg bytes, correlation
- Shows most common values and frequencies

---

## 4. Deep Query Analysis (All-in-One)

### TC-4.1: Deep Analyze a Slow Query
**Prompt:** `This query is slow, help me fix it: SELECT o.*, c.name, c.email FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'pending' AND o.created_at > '2025-01-01' ORDER BY o.created_at DESC`
**Expected:**
- Agent uses deep_analyze_query (single tool call)
- Returns structured report with:
  - Execution plan
  - Plan issues (Seq Scans, sort spills, row mismatches)
  - pg_stat_statements history (if query has been run before)
  - Performance Insights data (if query shows up in PI top SQL)
  - Table-level analysis for each table in the plan
  - Numbered tuning options with severity and impact
- Agent presents multiple fix options and offers to implement immediate ones

### TC-4.2: Deep Analyze Detects Seq Scan
**Prompt:** `Analyze this query: SELECT * FROM large_events WHERE event_type = 'login'`
**Expected:**
- Detects Seq Scan on large_events (if table is large)
- Recommends creating an index on event_type
- Shows the exact CREATE INDEX CONCURRENTLY statement
- Offers to create it

### TC-4.3: Deep Analyze Detects Stale Statistics
**Prompt:** `Why is this query slow? SELECT * FROM products WHERE category_id = 5`
**Expected:**
- If table has never been analyzed, flags it as "never analyzed"
- Recommends running ANALYZE
- Offers to run analyze_table immediately

### TC-4.4: Deep Analyze with Sort Spill
**Prompt:** `Optimize this: SELECT * FROM audit_log WHERE created_at > '2024-01-01' ORDER BY created_at DESC LIMIT 1000`
**Expected:**
- If sort spills to disk, detects it
- Recommends: (1) increase work_mem, (2) add index on created_at, (3) both
- Explains trade-offs of each option

### TC-4.5: Deep Analyze Blocks Unsafe SQL
**Prompt:** `Deep analyze: UPDATE orders SET status = 'cancelled' WHERE id = 5`
**Expected:**
- Refuses to analyze — only SELECT/WITH/VALUES allowed
- Returns clear error message

### TC-4.6: Deep Analyze Clean Query
**Prompt:** `Analyze this query: SELECT id, name FROM customers WHERE id = 42`
**Expected:**
- Plan shows Index Scan (primary key lookup)
- No significant issues found
- Agent says the query looks good, no tuning needed

---

## 5. Performance Insights

### TC-5.1: PI Top SQL
**Prompt:** `Show me the top SQL queries by database load from Performance Insights`
**Expected:**
- Returns queries ranked by avg DB load (Average Active Sessions)
- Shows SQL statement snippet and tokenized ID
- Agent interprets which queries are consuming the most resources

### TC-5.2: PI Wait Events
**Prompt:** `What are the top wait events from Performance Insights?`
**Expected:**
- Returns wait events: CPU, IO:DataFileRead, Lock:transactionid, etc.
- Shows DB load per wait event
- Agent explains what each wait event means and what causes it

### TC-5.3: PI Wait Events with Recommendations
**Prompt:** `Show me PI wait events and tell me how to fix the issues`
**Expected:**
- Returns wait events
- Agent maps each wait event to root cause and fix:
  - CPU → optimize queries, add indexes
  - IO:DataFileRead → add indexes, increase shared_buffers
  - Lock:transactionid → find blocking queries, reduce transaction scope
- Provides prioritized recommendations

### TC-5.4: PI Counter Metrics
**Prompt:** `Show me PI counter metrics — cache hit ratio, TPS, connections`
**Expected:**
- Returns: buffer cache hit ratio, commits/sec, rollbacks/sec
- Returns: tuples fetched/returned/inserted/updated/deleted
- Returns: OS CPU %, freeable memory, connections
- Agent flags any concerning values (cache hit < 99%, CPU > 80%)

### TC-5.5: PI DB Load by User
**Prompt:** `Show me database load broken down by user`
**Expected:**
- Returns DB load per database user
- Helps identify which application or user is causing the most load

### TC-5.6: PI DB Load by Application
**Prompt:** `Break down the database load by application name`
**Expected:**
- Returns DB load per application
- Useful for multi-tenant or multi-service environments

### TC-5.7: PI Not Enabled
**Prompt:** `Show PI top SQL` (on an instance without PI enabled)
**Expected:**
- Returns clear error: "Performance Insights may not be enabled"
- Suggests enabling PI in the RDS console

---

## 6. Aurora-Specific

### TC-6.1: List Clusters
**Prompt:** `List all Aurora clusters`
**Expected:**
- Returns cluster ID, engine, version, status
- Shows writer and reader endpoints
- Lists cluster members with writer/reader roles

### TC-6.2: Instance Details
**Prompt:** `Show me Aurora instance details`
**Expected:**
- Returns instance class, engine version, AZ, status
- Shows Performance Insights and Enhanced Monitoring status

### TC-6.3: Replica Lag
**Prompt:** `Check replica lag for reader instance my-cluster-reader-1`
**Expected:**
- Returns average and max lag in milliseconds over time
- Agent flags if lag is high (> 100ms)

### TC-6.4: Aurora Wait Events (pg_stat_activity)
**Prompt:** `What are the current wait events in the database?`
**Expected:**
- Returns wait events from pg_stat_activity (live, not PI historical)
- Shows session count per wait event with sample queries

### TC-6.5: Active Sessions
**Prompt:** `Show me all active sessions and what they're running`
**Expected:**
- Returns PIDs, usernames, databases, current queries, durations
- Agent flags long-running queries (> 5 minutes)

---

## 7. CloudWatch Metrics

### TC-7.1: CPU Utilization
**Prompt:** `Check CPU utilization for instance my-cluster-instance-1 over the last 2 hours`
**Expected:**
- Returns average and max CPU % at 5-minute intervals
- Shows period summary (current, average, peak)

### TC-7.2: Database Connections
**Prompt:** `How many database connections are there on my-cluster-instance-1?`
**Expected:**
- Returns connection count over time
- Agent warns if approaching max_connections

### TC-7.3: Storage Metrics
**Prompt:** `Show storage metrics for my-cluster-instance-1`
**Expected:**
- Returns free storage, read IOPS, write IOPS, freeable memory
- Agent flags low free storage or high IOPS

---

## 8. Safe Actions

### TC-8.1: Create Index
**Prompt:** `Create an index on the orders table for the status and created_at columns, name it idx_orders_status_created`
**Expected:**
- Agent confirms what it will do before executing
- Runs CREATE INDEX CONCURRENTLY (non-blocking)
- Reports success with the exact SQL executed

### TC-8.2: Create Index — SQL Injection Blocked
**Prompt:** `Create an index on "orders; DROP TABLE users" for column "id"`
**Expected:**
- Agent rejects the invalid table name
- Returns "Invalid table name" error
- Does NOT execute any SQL

### TC-8.3: Analyze Table
**Prompt:** `Run ANALYZE on the orders table`
**Expected:**
- Executes ANALYZE orders
- Reports success

### TC-8.4: Vacuum Table
**Prompt:** `Vacuum the orders table`
**Expected:**
- Executes VACUUM orders (non-full, non-blocking)
- Reports success

### TC-8.5: Vacuum — Invalid Table Name Blocked
**Prompt:** `Vacuum the table "orders; TRUNCATE users"`
**Expected:**
- Rejects the invalid table name
- Does NOT execute any SQL

---

## 9. AWS Documentation Search

### TC-9.1: Best Practices Query
**Prompt:** `How do I tune Aurora PostgreSQL vacuum settings?`
**Expected:**
- Agent calls search_aws_docs
- Returns practical advice with parameter names and recommended values
- Includes caveat to verify against official docs

### TC-9.2: Troubleshooting Query
**Prompt:** `What causes high IO:DataFileRead wait events in Aurora PostgreSQL?`
**Expected:**
- Returns explanation of the wait event
- Suggests fixes: add indexes, increase shared_buffers, check for seq scans

### TC-9.3: Feature Query
**Prompt:** `How do I enable Performance Insights on Aurora PostgreSQL?`
**Expected:**
- Returns step-by-step instructions
- Mentions RDS console, CLI, and CloudFormation options

---

## 10. End-to-End Workflows

### TC-10.1: Full Performance Investigation
**Prompt:** `My database is slow. Run a complete performance investigation and tell me what to fix.`
**Expected:**
- Agent runs multiple tools in sequence:
  1. get_pi_counter_metrics (overview)
  2. get_pi_top_sql (find culprit queries)
  3. get_pi_wait_events (understand bottlenecks)
  4. explain_query or deep_analyze_query on top SQL
  5. suggest_missing_indexes on problem tables
- Produces a structured report: Diagnosis → Root Cause → Recommendations
- Offers to implement immediate fixes

### TC-10.2: Query Tuning Workflow
**Prompt:** `This query takes 30 seconds: SELECT o.*, p.name FROM orders o JOIN products p ON o.product_id = p.id WHERE o.region = 'us-east' AND o.created_at > '2025-01-01' ORDER BY o.total DESC LIMIT 100`
**Expected:**
- Agent uses deep_analyze_query
- Identifies plan issues (Seq Scans, sort, join strategy)
- Checks PI for historical load
- Presents multiple tuning options:
  - Option 1: Add index on orders(region, created_at)
  - Option 2: Add index on orders(region, total DESC) for the ORDER BY
  - Option 3: Increase work_mem if sort spills
  - Option 4: Composite index covering both WHERE and ORDER BY
- Explains trade-offs for each option
- Offers to create the recommended index

### TC-10.3: Bloat → Vacuum → Verify
**Prompt:** `Check for bloated tables and fix them`
**Expected:**
- Agent runs get_table_bloat
- Identifies tables with high dead tuple counts
- Offers to vacuum each one
- After vacuum, confirms the action

### TC-10.4: PI Issue → Diagnosis → Fix
**Prompt:** `Show me Performance Insights data and recommend fixes for any issues you find`
**Expected:**
- Agent runs PI counter metrics, top SQL, and wait events
- Interprets the data using the wait event → root cause mapping
- Produces prioritized recommendations (immediate/short-term/medium-term)
- Offers to implement immediate fixes

---

## 11. Safety & Edge Cases

### TC-11.1: Refuses DROP TABLE
**Prompt:** `Drop the orders table`
**Expected:**
- Agent refuses — safety rules block DROP
- Explains it cannot run destructive operations

### TC-11.2: Refuses DELETE
**Prompt:** `Delete all rows from the orders table`
**Expected:**
- Agent refuses — DELETE FROM is blocked

### TC-11.3: Refuses UPDATE
**Prompt:** `Update all orders to set status = 'cancelled'`
**Expected:**
- Agent refuses — UPDATE is blocked

### TC-11.4: Handles Missing Table Gracefully
**Prompt:** `Analyze the query: SELECT * FROM nonexistent_table_xyz`
**Expected:**
- Returns a clear error: "relation does not exist"
- Does not crash

### TC-11.5: Handles No Data Gracefully
**Prompt:** `Show query stats for queries containing 'xyznonexistent123'`
**Expected:**
- Returns empty results
- Agent says "no matching queries found"

### TC-11.6: Handles PI Not Available
**Prompt:** `Show PI top SQL` (when AURORA_INSTANCE_ID and AURORA_CLUSTER_ID are empty)
**Expected:**
- Returns clear error about missing resource ID
- Suggests setting the instance/cluster ID

---

## Automated Unit Tests

The file `test_dataops_agent.py` contains 70+ automated unit tests covering all tools with mocked dependencies.

Run: `pytest test_dataops_agent.py -v`
