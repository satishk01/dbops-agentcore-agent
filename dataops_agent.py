"""
dataops_agent.py — DataOps Agent on Bedrock AgentCore Runtime
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Full-featured agent for Aurora PostgreSQL database operations:
  - Health checks: database summary, table sizes, bloat, unused indexes
  - Query analysis: top queries via aurora_stat_plans / pg_stat_statements
  - Aurora-specific: cluster info, replica lag, wait events, active sessions
  - CloudWatch metrics: CPU, connections, storage, IOPS
  - Safe actions: create index concurrently, analyze, vacuum

Deployed on Bedrock AgentCore Runtime using strands-agents.
"""
import json
import re
import logging
import os
import urllib.request
from datetime import datetime, timedelta

import boto3
import psycopg2
from strands import Agent, tool
from strands.models import BedrockModel
from bedrock_agentcore.runtime import BedrockAgentCoreApp

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
log = logging.getLogger("dataops.agent")

app = BedrockAgentCoreApp()

# ── Configuration (hardcoded — single source of truth) ────────────────────────
AWS_REGION         = "us-east-1"
BEDROCK_MODEL_ID   = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Database connection
DB_SECRET_NAME     = ""                          # Leave empty to use direct creds below
DB_HOST            = "*************************"                          # ← FILL IN: your-cluster.cluster-xxxx.us-east-1.rds.amazonaws.com
DB_PORT            = 5432
DB_NAME            = "postgres"                  # ← FILL IN if different
DB_USER            = "postgres"                  # ← FILL IN if different
DB_PASSWORD        = "*****************"                          # ← FILL IN
DB_SSLMODE         = "verify-full"
DB_SSLROOTCERT     = "rds-global-bundle.pem"

# Aurora identifiers for CloudWatch / RDS API
AURORA_CLUSTER_ID  = "**************"                          # ← FILL IN: your-aurora-cluster
AURORA_INSTANCE_ID = "*************"                          # ← FILL IN: your-aurora-instance-1

RDS_CA_BUNDLE_URL = "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem"

print("🗄️ DataOps Agent — imports successful")


# ══════════════════════════════════════════════════════════════════════════════
#  Database Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _load_secret(secret_name: str) -> dict:
    try:
        client = boto3.client("secretsmanager", region_name=AWS_REGION)
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        log.warning(f"Could not load secret '{secret_name}': {e}")
        return {}


def _ensure_rds_ca_bundle(cert_path: str) -> str:
    if os.path.isfile(cert_path):
        return cert_path
    try:
        log.info(f"Downloading RDS CA bundle to {cert_path}...")
        urllib.request.urlretrieve(RDS_CA_BUNDLE_URL, cert_path)
        return cert_path
    except Exception as e:
        log.warning(f"Could not download RDS CA bundle: {e}")
        return cert_path


def _get_db_params() -> dict:
    """Resolve DB connection params from Secrets Manager or env vars."""
    secret = _load_secret(DB_SECRET_NAME) if DB_SECRET_NAME else {}
    params = dict(
        host=secret.get("host", DB_HOST),
        port=int(secret.get("port", DB_PORT)),
        dbname=secret.get("dbname", DB_NAME),
        user=secret.get("username", DB_USER),
        password=secret.get("password", DB_PASSWORD),
        sslmode=DB_SSLMODE,
        connect_timeout=10,
    )
    if DB_SSLMODE in ("verify-full", "verify-ca"):
        params["sslrootcert"] = _ensure_rds_ca_bundle(DB_SSLROOTCERT)
    return params


def _get_connection():
    return psycopg2.connect(**_get_db_params())


def execute_query(sql: str, params=None) -> list[dict]:
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def execute_command(sql: str) -> str:
    conn = _get_connection()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return f"Success: {cur.statusmessage}"
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        conn.close()


# Dangerous SQL patterns that must be blocked
BLOCKED_PATTERNS = [
    r"\bDROP\s+(TABLE|DATABASE|SCHEMA|INDEX(?!\s+CONCURRENTLY))\b",
    r"\bDELETE\s+FROM\b",
    r"\bTRUNCATE\b",
    r"\bALTER\s+TABLE\s+\w+\s+DROP\b",
    r"\bUPDATE\b",
    r"\bGRANT\b",
    r"\bREVOKE\b",
]


def _is_safe_sql(sql: str) -> tuple[bool, str]:
    upper = sql.upper().strip()
    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, upper):
            return False, f"Blocked: matches dangerous pattern '{pattern}'"
    return True, "OK"


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — Health Check (Read-Only)
# ══════════════════════════════════════════════════════════════════════════════

@tool
def get_database_summary() -> str:
    """Get a high-level summary of the database: size, connections, uptime."""
    sql = """
    SELECT
        current_database() AS database_name,
        pg_size_pretty(pg_database_size(current_database())) AS database_size,
        (SELECT count(*) FROM pg_stat_activity) AS active_connections,
        (SELECT setting FROM pg_settings WHERE name = 'max_connections') AS max_connections,
        version() AS pg_version;
    """
    return json.dumps(execute_query(sql), default=str)


@tool
def get_largest_tables() -> str:
    """Get the top 10 largest tables by disk usage in the database."""
    sql = """
    SELECT
        nspname AS schema, relname AS table_name,
        pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size,
        pg_size_pretty(pg_relation_size(c.oid)) AS table_size,
        pg_size_pretty(pg_indexes_size(c.oid)) AS index_size,
        n_live_tup AS row_estimate
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
    WHERE c.relkind = 'r' AND n.nspname NOT IN ('pg_catalog', 'information_schema')
    ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 10;
    """
    return json.dumps(execute_query(sql), default=str)


@tool
def get_unused_indexes() -> str:
    """Find indexes that have never been used by any query scan."""
    sql = """
    SELECT
        schemaname AS schema, relname AS table_name, indexrelname AS index_name,
        pg_size_pretty(pg_relation_size(i.indexrelid)) AS index_size, idx_scan AS times_used
    FROM pg_stat_user_indexes i
    JOIN pg_index idx ON i.indexrelid = idx.indexrelid
    WHERE idx_scan = 0 AND NOT idx.indisunique AND NOT idx.indisprimary
    ORDER BY pg_relation_size(i.indexrelid) DESC LIMIT 20;
    """
    return json.dumps(execute_query(sql), default=str)


@tool
def get_table_bloat() -> str:
    """Detect tables with significant dead tuple bloat that may need VACUUM."""
    sql = """
    SELECT
        schemaname AS schema, relname AS table_name,
        n_live_tup AS live_tuples, n_dead_tup AS dead_tuples,
        CASE WHEN n_live_tup > 0
            THEN round(100.0 * n_dead_tup / (n_live_tup + n_dead_tup), 2) ELSE 0
        END AS bloat_pct,
        last_vacuum, last_autovacuum, last_analyze
    FROM pg_stat_user_tables WHERE n_dead_tup > 100
    ORDER BY n_dead_tup DESC LIMIT 15;
    """
    return json.dumps(execute_query(sql), default=str)


@tool
def get_index_bloat() -> str:
    """Find bloated indexes consuming more space than necessary."""
    sql = """
    SELECT
        schemaname AS schema, tablename AS table_name, indexname AS index_name,
        pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
        idx_scan AS scans, idx_tup_read AS tuples_read, idx_tup_fetch AS tuples_fetched
    FROM pg_stat_user_indexes
    JOIN pg_indexes ON pg_stat_user_indexes.indexrelname = pg_indexes.indexname
        AND pg_stat_user_indexes.schemaname = pg_indexes.schemaname
    ORDER BY pg_relation_size(indexname::regclass) DESC LIMIT 15;
    """
    return json.dumps(execute_query(sql), default=str)


@tool
def get_top_queries() -> str:
    """Get the top 10 most time-consuming queries with execution plans using Aurora's aurora_stat_plans()."""
    aurora_sql = """
    SELECT userid::regrole AS db_user, queryid, datname AS db_name,
        substring(query, 1, 200) AS short_query,
        round((total_plan_time + total_exec_time)::numeric, 2) AS total_time_ms,
        calls, explain_plan
    FROM aurora_stat_plans(true) p, pg_database d
    WHERE p.dbid = d.oid ORDER BY total_time_ms DESC LIMIT 10;
    """
    fallback_sql = """
    SELECT queryid, substring(query, 1, 200) AS short_query, calls,
        round(total_exec_time::numeric, 2) AS total_exec_time_ms,
        round(mean_exec_time::numeric, 2) AS mean_exec_time_ms, rows
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat_statements%' AND query NOT LIKE '%aurora_stat_plans%'
    ORDER BY total_exec_time DESC LIMIT 10;
    """
    try:
        results = execute_query(aurora_sql)
        return json.dumps({"source": "aurora_stat_plans", "queries": results}, default=str)
    except Exception:
        try:
            results = execute_query(fallback_sql)
            return json.dumps({"source": "pg_stat_statements", "queries": results}, default=str)
        except Exception as e:
            return json.dumps({"error": str(e), "hint": "Neither aurora_stat_plans() nor pg_stat_statements available"})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — Aurora-Specific (Read-Only)
# ══════════════════════════════════════════════════════════════════════════════

@tool
def list_aurora_clusters() -> str:
    """List all Aurora PostgreSQL clusters in the current AWS region with instance details."""
    try:
        rds = boto3.client("rds", region_name=AWS_REGION)
        response = rds.describe_db_clusters()
        clusters = []
        for c in response.get("DBClusters", []):
            members = [{"instance_id": m["DBInstanceIdentifier"], "is_writer": m["IsClusterWriter"]}
                       for m in c.get("DBClusterMembers", [])]
            clusters.append({
                "cluster_id": c["DBClusterIdentifier"], "engine": c["Engine"],
                "engine_version": c["EngineVersion"], "status": c["Status"],
                "writer_endpoint": c.get("Endpoint", "N/A"),
                "reader_endpoint": c.get("ReaderEndpoint", "N/A"),
                "port": c.get("Port", 5432), "multi_az": c.get("MultiAZ", False),
                "storage_encrypted": c.get("StorageEncrypted", False),
                "deletion_protection": c.get("DeletionProtection", False),
                "members": members,
            })
        return json.dumps(clusters, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_aurora_instance_details() -> str:
    """Get detailed information about Aurora instances: class, AZ, role, engine version, and performance insights status."""
    try:
        rds = boto3.client("rds", region_name=AWS_REGION)
        if AURORA_CLUSTER_ID:
            cluster_resp = rds.describe_db_clusters(DBClusterIdentifier=AURORA_CLUSTER_ID)
            cluster = cluster_resp["DBClusters"][0]
            member_ids = [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]
        else:
            resp = rds.describe_db_instances()
            member_ids = [i["DBInstanceIdentifier"] for i in resp["DBInstances"]
                          if i.get("Engine", "").startswith("aurora")]
        instances = []
        for iid in member_ids:
            inst = rds.describe_db_instances(DBInstanceIdentifier=iid)["DBInstances"][0]
            instances.append({
                "instance_id": inst["DBInstanceIdentifier"],
                "instance_class": inst["DBInstanceClass"],
                "engine_version": inst["EngineVersion"],
                "availability_zone": inst.get("AvailabilityZone", "N/A"),
                "status": inst["DBInstanceStatus"],
                "performance_insights": inst.get("PerformanceInsightsEnabled", False),
                "enhanced_monitoring": inst.get("MonitoringInterval", 0) > 0,
            })
        return json.dumps(instances, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_aurora_replica_lag(db_instance_id: str, period_minutes: int = 60) -> str:
    """
    Get Aurora replica lag metrics from CloudWatch.

    Args:
        db_instance_id: The Aurora reader instance identifier.
        period_minutes: How far back to look in minutes (default 60).
    """
    try:
        cw = boto3.client("cloudwatch", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)
        response = cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName="AuroraReplicaLag",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
            StartTime=start_time, EndTime=end_time, Period=300,
            Statistics=["Average", "Maximum"],
        )
        datapoints = sorted(response.get("Datapoints", []), key=lambda x: x["Timestamp"])
        results = [{"timestamp": dp["Timestamp"].isoformat(),
                     "avg_lag_ms": round(dp["Average"], 2),
                     "max_lag_ms": round(dp["Maximum"], 2)} for dp in datapoints]
        return json.dumps({"instance": db_instance_id, "metric": "AuroraReplicaLag", "datapoints": results}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_aurora_wait_events() -> str:
    """Get current wait events from Aurora PostgreSQL to identify what queries are waiting on."""
    sql = """
    SELECT wait_event_type, wait_event, state, count(*) AS session_count,
        array_agg(DISTINCT substring(query, 1, 80)) AS sample_queries
    FROM pg_stat_activity
    WHERE state != 'idle' AND pid != pg_backend_pid() AND wait_event IS NOT NULL
    GROUP BY wait_event_type, wait_event, state
    ORDER BY session_count DESC LIMIT 20;
    """
    try:
        return json.dumps(execute_query(sql), default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_aurora_active_sessions() -> str:
    """Get currently active (non-idle) sessions with their queries, duration, and wait info."""
    sql = """
    SELECT pid, usename AS username, datname AS database, client_addr, state,
        wait_event_type, wait_event, substring(query, 1, 150) AS current_query,
        now() - query_start AS query_duration, now() - backend_start AS session_duration
    FROM pg_stat_activity
    WHERE state != 'idle' AND pid != pg_backend_pid()
    ORDER BY query_start ASC LIMIT 25;
    """
    try:
        return json.dumps(execute_query(sql), default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — CloudWatch Metrics (Read-Only)
# ══════════════════════════════════════════════════════════════════════════════

@tool
def get_cloudwatch_cpu_utilization(db_instance_id: str, period_minutes: int = 60) -> str:
    """
    Get CPU utilization metrics from CloudWatch for an RDS/Aurora instance.

    Args:
        db_instance_id: The RDS DB instance identifier.
        period_minutes: How far back to look in minutes (default 60).
    """
    try:
        cw = boto3.client("cloudwatch", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)
        response = cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName="CPUUtilization",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
            StartTime=start_time, EndTime=end_time, Period=300,
            Statistics=["Average", "Maximum"],
        )
        datapoints = sorted(response.get("Datapoints", []), key=lambda x: x["Timestamp"])
        results = [{"timestamp": dp["Timestamp"].isoformat(),
                     "avg_cpu_pct": round(dp["Average"], 2),
                     "max_cpu_pct": round(dp["Maximum"], 2)} for dp in datapoints]
        summary = {}
        if results:
            avgs = [r["avg_cpu_pct"] for r in results]
            summary = {"instance": db_instance_id, "period_minutes": period_minutes,
                       "current_avg_cpu": results[-1]["avg_cpu_pct"],
                       "period_avg_cpu": round(sum(avgs) / len(avgs), 2),
                       "period_max_cpu": max(r["max_cpu_pct"] for r in results),
                       "datapoints": results}
        else:
            summary = {"instance": db_instance_id, "error": "No datapoints found. Check instance ID."}
        return json.dumps(summary, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_cloudwatch_db_connections(db_instance_id: str, period_minutes: int = 60) -> str:
    """
    Get database connection count metrics from CloudWatch.

    Args:
        db_instance_id: The RDS DB instance identifier.
        period_minutes: How far back to look in minutes (default 60).
    """
    try:
        cw = boto3.client("cloudwatch", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)
        response = cw.get_metric_statistics(
            Namespace="AWS/RDS", MetricName="DatabaseConnections",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
            StartTime=start_time, EndTime=end_time, Period=300,
            Statistics=["Average", "Maximum"],
        )
        datapoints = sorted(response.get("Datapoints", []), key=lambda x: x["Timestamp"])
        results = [{"timestamp": dp["Timestamp"].isoformat(),
                     "avg_connections": round(dp["Average"], 1),
                     "max_connections": round(dp["Maximum"], 1)} for dp in datapoints]
        return json.dumps({"instance": db_instance_id, "datapoints": results}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_cloudwatch_storage_metrics(db_instance_id: str) -> str:
    """
    Get free storage space, IOPS, and freeable memory from CloudWatch.

    Args:
        db_instance_id: The RDS DB instance identifier.
    """
    try:
        cw = boto3.client("cloudwatch", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=30)
        metrics = {}
        for metric_name in ["FreeStorageSpace", "ReadIOPS", "WriteIOPS", "FreeableMemory"]:
            response = cw.get_metric_statistics(
                Namespace="AWS/RDS", MetricName=metric_name,
                Dimensions=[{"Name": "DBInstanceIdentifier", "Value": db_instance_id}],
                StartTime=start_time, EndTime=end_time, Period=300, Statistics=["Average"],
            )
            dps = response.get("Datapoints", [])
            if dps:
                latest = sorted(dps, key=lambda x: x["Timestamp"])[-1]
                val = latest["Average"]
                if metric_name in ("FreeStorageSpace", "FreeableMemory"):
                    metrics[metric_name] = f"{round(val / (1024 ** 3), 2)} GB"
                else:
                    metrics[metric_name] = round(val, 1)
            else:
                metrics[metric_name] = "N/A"
        return json.dumps({"instance": db_instance_id, **metrics}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — Safe Actions (Write)
# ══════════════════════════════════════════════════════════════════════════════

@tool
def create_index_concurrently(table_name: str, column_names: str, index_name: str) -> str:
    """
    Create an index CONCURRENTLY on a table without blocking reads/writes.

    Args:
        table_name: The table to create the index on.
        column_names: Comma-separated column names for the index.
        index_name: Name for the new index.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table_name):
        return json.dumps({"error": "Invalid table name"})
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_, ]*$", column_names):
        return json.dumps({"error": "Invalid column names"})
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", index_name):
        return json.dumps({"error": "Invalid index name"})
    sql = f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} ON {table_name} ({column_names})"
    safe, reason = _is_safe_sql(sql)
    if not safe:
        return json.dumps({"error": reason})
    result = execute_command(sql)
    return json.dumps({"action": "create_index_concurrently", "sql": sql, "result": result})


@tool
def analyze_table(table_name: str) -> str:
    """
    Run ANALYZE on a table to update planner statistics.

    Args:
        table_name: The table to analyze.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return json.dumps({"error": "Invalid table name"})
    result = execute_command(f"ANALYZE {table_name}")
    return json.dumps({"action": "analyze_table", "table": table_name, "result": result})


@tool
def vacuum_table(table_name: str) -> str:
    """
    Run VACUUM (non-full) on a table to reclaim dead tuple space without locking.

    Args:
        table_name: The table to vacuum.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return json.dumps({"error": "Invalid table name"})
    result = execute_command(f"VACUUM {table_name}")
    return json.dumps({"action": "vacuum_table", "table": table_name, "result": result})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — Query Tuning & Explain Plan Analysis
# ══════════════════════════════════════════════════════════════════════════════

@tool
def explain_query(query: str, analyze: bool = False) -> str:
    """
    Run EXPLAIN (or EXPLAIN ANALYZE) on a SQL query to get the execution plan.
    Use this to understand how PostgreSQL will execute a query and identify bottlenecks.
    EXPLAIN is read-only. EXPLAIN ANALYZE actually runs the query (read-only queries only).

    Args:
        query: The SQL SELECT query to explain. Must be a SELECT statement.
        analyze: If True, runs EXPLAIN ANALYZE (actually executes the query for real timing). Default False.
    """
    trimmed = query.strip().rstrip(";")
    upper = trimmed.upper()

    # Safety: only allow SELECT, WITH...SELECT, and VALUES
    if not (upper.startswith("SELECT") or upper.startswith("WITH") or upper.startswith("VALUES")):
        return json.dumps({"error": "Only SELECT / WITH / VALUES queries can be explained. "
                           "Refusing to EXPLAIN any DML (INSERT, UPDATE, DELETE)."})

    # Block any embedded writes
    for dangerous in ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER", "CREATE"]:
        # Check for standalone keywords (not inside string literals — best effort)
        if re.search(rf"\b{dangerous}\b", upper):
            if dangerous == "CREATE" and "CREATE" not in upper.split("SELECT")[0]:
                continue  # CREATE inside a subquery alias is fine
            return json.dumps({"error": f"Query contains '{dangerous}' — blocked for safety."})

    explain_prefix = "EXPLAIN (FORMAT JSON, VERBOSE, COSTS, BUFFERS"
    if analyze:
        explain_prefix += ", ANALYZE, TIMING"
    explain_prefix += ")"

    sql = f"{explain_prefix} {trimmed}"
    try:
        rows = execute_query(sql)
        # PostgreSQL returns the plan as a single-row JSON array
        if rows and "QUERY PLAN" in rows[0]:
            plan = rows[0]["QUERY PLAN"]
        else:
            plan = rows
        return json.dumps({"query": trimmed[:200], "analyzed": analyze, "plan": plan}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": trimmed[:200]})


@tool
def get_query_stats(query_substring: str = "", min_calls: int = 5, sort_by: str = "total_time") -> str:
    """
    Get detailed statistics for queries matching a pattern from pg_stat_statements.
    Useful for finding slow queries, high-call queries, or queries with poor row estimates.

    Args:
        query_substring: Filter queries containing this text (case-insensitive). Empty = top queries.
        min_calls: Minimum number of calls to include (default 5, filters noise).
        sort_by: Sort metric — one of: total_time, mean_time, calls, rows, shared_blks_hit, shared_blks_read
    """
    valid_sorts = {
        "total_time": "total_exec_time",
        "mean_time": "mean_exec_time",
        "calls": "calls",
        "rows": "rows",
        "shared_blks_hit": "shared_blks_hit",
        "shared_blks_read": "shared_blks_read",
    }
    order_col = valid_sorts.get(sort_by, "total_exec_time")

    where_clause = f"AND calls >= {int(min_calls)}"
    if query_substring:
        # Sanitize for LIKE
        safe_sub = query_substring.replace("'", "''").replace("%", "\\%").replace("_", "\\_")
        where_clause += f" AND query ILIKE '%{safe_sub}%'"

    sql = f"""
    SELECT
        queryid,
        substring(query, 1, 300) AS query_text,
        calls,
        round(total_exec_time::numeric, 2) AS total_exec_time_ms,
        round(mean_exec_time::numeric, 2) AS mean_exec_time_ms,
        round(min_exec_time::numeric, 2) AS min_exec_time_ms,
        round(max_exec_time::numeric, 2) AS max_exec_time_ms,
        round(stddev_exec_time::numeric, 2) AS stddev_exec_time_ms,
        rows,
        round((rows::numeric / NULLIF(calls, 0)), 2) AS avg_rows_per_call,
        shared_blks_hit,
        shared_blks_read,
        CASE WHEN (shared_blks_hit + shared_blks_read) > 0
            THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
            ELSE 100
        END AS cache_hit_pct,
        temp_blks_read,
        temp_blks_written
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat_statements%'
      {where_clause}
    ORDER BY {order_col} DESC
    LIMIT 15;
    """
    try:
        results = execute_query(sql)
        return json.dumps({"sort_by": sort_by, "min_calls": min_calls,
                           "filter": query_substring or "none", "queries": results}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e),
                           "hint": "pg_stat_statements extension may not be enabled. Run: CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"})


@tool
def suggest_missing_indexes(table_name: str) -> str:
    """
    Analyze a table for potential missing indexes by examining sequential scan frequency,
    table size, and existing indexes. Helps identify tables that would benefit from new indexes.

    Args:
        table_name: The table to analyze for missing indexes.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return json.dumps({"error": "Invalid table name"})

    # Get scan stats
    scan_sql = f"""
    SELECT
        relname AS table_name,
        seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
        n_live_tup AS row_count,
        pg_size_pretty(pg_relation_size(relid)) AS table_size,
        CASE WHEN (seq_scan + COALESCE(idx_scan, 0)) > 0
            THEN round(100.0 * seq_scan / (seq_scan + COALESCE(idx_scan, 0)), 2)
            ELSE 0
        END AS seq_scan_pct
    FROM pg_stat_user_tables
    WHERE relname = '{table_name}';
    """

    # Get existing indexes
    idx_sql = f"""
    SELECT indexname, indexdef,
        pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
        idx_scan AS scans
    FROM pg_indexes
    JOIN pg_stat_user_indexes ON pg_indexes.indexname = pg_stat_user_indexes.indexrelname
        AND pg_indexes.schemaname = pg_stat_user_indexes.schemaname
    WHERE pg_indexes.tablename = '{table_name}';
    """

    # Get columns frequently in WHERE clauses (from pg_stats)
    col_sql = f"""
    SELECT attname AS column_name, n_distinct,
        CASE WHEN n_distinct > 0 THEN n_distinct
             WHEN n_distinct < 0 THEN round((-n_distinct * (
                SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = '{table_name}'
             ))::numeric, 0)
             ELSE 0
        END AS estimated_distinct_values,
        null_frac AS null_fraction,
        correlation
    FROM pg_stats
    WHERE tablename = '{table_name}'
    ORDER BY correlation ASC;
    """

    try:
        scan_stats = execute_query(scan_sql)
        indexes = execute_query(idx_sql)
        columns = execute_query(col_sql)
        return json.dumps({
            "table": table_name,
            "scan_stats": scan_stats,
            "existing_indexes": indexes,
            "column_stats": columns,
            "analysis_hint": (
                "High seq_scan_pct with large row_count suggests missing indexes. "
                "Columns with low correlation and high distinct values are good index candidates. "
                "Check if existing indexes cover the columns used in WHERE/JOIN clauses."
            ),
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_table_column_stats(table_name: str) -> str:
    """
    Get detailed column-level statistics for a table — useful for understanding data distribution
    and making index/partitioning decisions.

    Args:
        table_name: The table to get column stats for.
    """
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_.]*$", table_name):
        return json.dumps({"error": "Invalid table name"})

    sql = f"""
    SELECT
        a.attname AS column_name,
        format_type(a.atttypid, a.atttypmod) AS data_type,
        s.n_distinct,
        s.null_frac AS null_fraction,
        s.avg_width AS avg_bytes,
        s.correlation,
        CASE WHEN s.most_common_vals IS NOT NULL
            THEN left(s.most_common_vals::text, 200)
            ELSE NULL
        END AS most_common_values,
        CASE WHEN s.most_common_freqs IS NOT NULL
            THEN left(s.most_common_freqs::text, 200)
            ELSE NULL
        END AS most_common_freqs
    FROM pg_attribute a
    JOIN pg_stats s ON s.attname = a.attname AND s.tablename = '{table_name}'
    WHERE a.attrelid = '{table_name}'::regclass
      AND a.attnum > 0 AND NOT a.attisdropped
    ORDER BY a.attnum;
    """
    try:
        results = execute_query(sql)
        return json.dumps({"table": table_name, "columns": results}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL — Deep Query Analysis (all-in-one diagnostic for a specific query)
# ══════════════════════════════════════════════════════════════════════════════

def _extract_tables_from_plan(plan) -> list[str]:
    """Recursively extract table names from an EXPLAIN JSON plan."""
    tables = set()
    if isinstance(plan, list):
        for item in plan:
            tables.update(_extract_tables_from_plan(item))
    elif isinstance(plan, dict):
        rel = plan.get("Relation Name") or plan.get("relation_name")
        if rel:
            schema = plan.get("Schema") or plan.get("schema") or "public"
            tables.add(f"{schema}.{rel}" if schema != "public" else rel)
        for key in ("Plans", "plans", "Plan", "plan"):
            if key in plan:
                tables.update(_extract_tables_from_plan(plan[key]))
    return list(tables)


def _find_plan_issues(plan, issues=None, depth=0) -> list[dict]:
    """Recursively walk an EXPLAIN JSON plan and flag performance issues."""
    if issues is None:
        issues = []
    if isinstance(plan, list):
        for item in plan:
            _find_plan_issues(item, issues, depth)
        return issues
    if not isinstance(plan, dict):
        return issues

    node_type = plan.get("Node Type") or plan.get("node_type", "")
    rel = plan.get("Relation Name") or plan.get("relation_name", "")
    rows = plan.get("Plan Rows") or plan.get("plan_rows") or plan.get("Actual Rows") or 0
    total_cost = plan.get("Total Cost") or plan.get("total_cost", 0)
    actual_rows = plan.get("Actual Rows") or plan.get("actual_rows")
    plan_rows = plan.get("Plan Rows") or plan.get("plan_rows")
    startup_cost = plan.get("Startup Cost") or plan.get("startup_cost", 0)
    shared_read = plan.get("Shared Read Blocks") or plan.get("shared_read_blocks", 0)
    temp_read = plan.get("Temp Read Blocks") or plan.get("temp_read_blocks", 0)
    temp_written = plan.get("Temp Written Blocks") or plan.get("temp_written_blocks", 0)
    sort_method = plan.get("Sort Method") or plan.get("sort_method", "")

    # Seq Scan on a table with many rows
    if "Seq Scan" in node_type and rel:
        issues.append({
            "severity": "high" if rows > 10000 else "medium",
            "issue": f"Sequential Scan on '{rel}'",
            "detail": f"Scanning ~{rows:,} rows. Consider adding an index on the filter/join columns.",
            "table": rel,
            "fix_type": "index",
        })

    # Sort spilling to disk
    if "Sort" in node_type and ("external" in sort_method.lower() or temp_written > 0):
        issues.append({
            "severity": "high",
            "issue": f"Sort spilling to disk",
            "detail": f"Sort method: {sort_method}. Temp blocks written: {temp_written}. "
                      f"Increase work_mem or add an index to avoid the sort.",
            "fix_type": "work_mem_or_index",
        })

    # Hash Join with large build side
    if "Hash" in node_type and shared_read > 1000:
        issues.append({
            "severity": "medium",
            "issue": f"Hash operation reading many blocks from disk",
            "detail": f"Shared read blocks: {shared_read}. May benefit from more shared_buffers or an index.",
            "fix_type": "memory_or_index",
        })

    # Nested Loop with high row count
    if "Nested Loop" in node_type and rows > 50000:
        issues.append({
            "severity": "high",
            "issue": f"Nested Loop producing ~{rows:,} rows",
            "detail": "Nested loops are expensive at high row counts. Consider a Hash or Merge Join "
                      "by adding indexes or increasing work_mem.",
            "fix_type": "index_or_rewrite",
        })

    # Row estimate mismatch (if EXPLAIN ANALYZE was used)
    if actual_rows is not None and plan_rows is not None and plan_rows > 0:
        ratio = actual_rows / plan_rows if plan_rows > 0 else 0
        if ratio > 10 or (ratio < 0.1 and actual_rows > 100):
            issues.append({
                "severity": "medium",
                "issue": f"Row estimate mismatch on '{node_type}'",
                "detail": f"Planned {plan_rows:,} rows but got {actual_rows:,} (ratio: {ratio:.1f}x). "
                          f"Run ANALYZE on '{rel or 'the table'}' to update statistics.",
                "table": rel,
                "fix_type": "analyze",
            })

    # High cost node
    if total_cost > 100000:
        issues.append({
            "severity": "medium",
            "issue": f"High-cost node: {node_type} (cost: {total_cost:,.0f})",
            "detail": f"This node dominates the query cost. Table: {rel or 'N/A'}.",
            "fix_type": "investigate",
        })

    # Recurse into child plans
    for key in ("Plans", "plans", "Plan", "plan"):
        if key in plan:
            _find_plan_issues(plan[key], issues, depth + 1)

    return issues


@tool
def deep_analyze_query(query: str) -> str:
    """
    All-in-one deep analysis of a SQL query. Runs the full diagnostic chain:
    1. EXPLAIN to get the execution plan
    2. Automated plan analysis to find Seq Scans, sort spills, row mismatches, high-cost nodes
    3. pg_stat_statements lookup for historical execution stats
    4. Performance Insights lookup for DB load attributed to this query
    5. Table scan stats and missing index analysis for every table in the plan
    6. Consolidated findings with specific tuning options

    Use this when a user pastes a slow query and wants to know why it's slow and how to fix it.

    Args:
        query: The SQL SELECT query to analyze. Must be a SELECT/WITH/VALUES statement.
    """
    trimmed = query.strip().rstrip(";")
    upper = trimmed.upper()
    report = {"query": trimmed[:300], "sections": {}}

    # Safety check
    if not (upper.startswith("SELECT") or upper.startswith("WITH") or upper.startswith("VALUES")):
        return json.dumps({"error": "Only SELECT / WITH / VALUES queries can be analyzed."})
    for dangerous in ["INSERT", "UPDATE", "DELETE", "DROP", "TRUNCATE", "ALTER"]:
        if re.search(rf"\b{dangerous}\b", upper):
            return json.dumps({"error": f"Query contains '{dangerous}' — blocked for safety."})

    # ── Step 1: EXPLAIN ──────────────────────────────────────────────────
    try:
        explain_sql = f"EXPLAIN (FORMAT JSON, VERBOSE, COSTS, BUFFERS) {trimmed}"
        rows = execute_query(explain_sql)
        plan = rows[0].get("QUERY PLAN", rows) if rows else None
        report["sections"]["explain_plan"] = plan
    except Exception as e:
        report["sections"]["explain_plan"] = {"error": str(e)}
        plan = None

    # ── Step 2: Automated plan issue detection ───────────────────────────
    plan_issues = []
    tables_in_plan = []
    if plan:
        plan_issues = _find_plan_issues(plan)
        tables_in_plan = _extract_tables_from_plan(plan)
    report["sections"]["plan_issues"] = plan_issues
    report["sections"]["tables_in_query"] = tables_in_plan

    # ── Step 3: pg_stat_statements history ───────────────────────────────
    # Try to find this query in pg_stat_statements by matching keywords
    keywords = [w for w in trimmed.split() if len(w) > 3 and w.upper() not in
                ("SELECT", "FROM", "WHERE", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
                 "GROUP", "ORDER", "LIMIT", "OFFSET", "HAVING", "WITH", "CASE",
                 "WHEN", "THEN", "ELSE", "NULL", "TRUE", "FALSE", "AND", "NOT")]
    search_term = keywords[0] if keywords else ""
    try:
        if search_term:
            safe_term = search_term.replace("'", "''").replace("%", "\\%")
            stats_sql = f"""
            SELECT queryid, substring(query, 1, 300) AS query_text, calls,
                round(total_exec_time::numeric, 2) AS total_exec_time_ms,
                round(mean_exec_time::numeric, 2) AS mean_exec_time_ms,
                round(min_exec_time::numeric, 2) AS min_ms,
                round(max_exec_time::numeric, 2) AS max_ms,
                round(stddev_exec_time::numeric, 2) AS stddev_ms,
                rows, shared_blks_hit, shared_blks_read,
                CASE WHEN (shared_blks_hit + shared_blks_read) > 0
                    THEN round(100.0 * shared_blks_hit / (shared_blks_hit + shared_blks_read), 2)
                    ELSE 100 END AS cache_hit_pct,
                temp_blks_read, temp_blks_written
            FROM pg_stat_statements
            WHERE query ILIKE '%{safe_term}%'
              AND query NOT LIKE '%pg_stat_statements%'
            ORDER BY total_exec_time DESC LIMIT 5;
            """
            report["sections"]["pg_stat_statements"] = execute_query(stats_sql)
        else:
            report["sections"]["pg_stat_statements"] = []
    except Exception as e:
        report["sections"]["pg_stat_statements"] = {"error": str(e)}

    # ── Step 4: Performance Insights — check if this query shows up in PI ─
    try:
        resource_id = _get_pi_resource_id()
        if resource_id:
            pi = boto3.client("pi", region_name=AWS_REGION)
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=60)
            pi_resp = pi.describe_dimension_keys(
                ServiceType="RDS", Identifier=resource_id,
                StartTime=start_time, EndTime=end_time,
                Metric="db.load.avg", PeriodInSeconds=300,
                GroupBy={"Group": "db.sql",
                         "Dimensions": ["db.sql.statement", "db.sql.tokenized_id"],
                         "Limit": 25},
            )
            # Try to match our query against PI results
            pi_matches = []
            query_words = set(w.lower() for w in trimmed.split() if len(w) > 3)
            for k in pi_resp.get("Keys", []):
                dims = k.get("Dimensions", {})
                pi_sql = dims.get("db.sql.statement", "").lower()
                pi_words = set(w for w in pi_sql.split() if len(w) > 3)
                overlap = query_words & pi_words
                if len(overlap) >= min(3, len(query_words)):
                    pi_matches.append({
                        "sql_snippet": dims.get("db.sql.statement", "")[:200],
                        "sql_id": dims.get("db.sql.tokenized_id", ""),
                        "avg_db_load": round(k.get("Total", 0), 4),
                    })
            report["sections"]["performance_insights"] = {
                "matched": len(pi_matches) > 0,
                "matches": pi_matches[:5],
            }
        else:
            report["sections"]["performance_insights"] = {"skipped": "No PI resource ID available"}
    except Exception as e:
        report["sections"]["performance_insights"] = {"error": str(e)}

    # ── Step 5: Table-level analysis for each table in the plan ──────────
    table_analysis = {}
    for tbl in tables_in_plan[:5]:  # Cap at 5 tables
        tbl_name = tbl.split(".")[-1]  # Strip schema prefix for queries
        try:
            scan_sql = f"""
            SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch,
                n_live_tup AS row_count, n_dead_tup AS dead_tuples,
                pg_size_pretty(pg_relation_size(relid)) AS table_size,
                CASE WHEN (seq_scan + COALESCE(idx_scan, 0)) > 0
                    THEN round(100.0 * seq_scan / (seq_scan + COALESCE(idx_scan, 0)), 2)
                    ELSE 0 END AS seq_scan_pct,
                last_vacuum, last_autovacuum, last_analyze
            FROM pg_stat_user_tables WHERE relname = '{tbl_name}';
            """
            idx_sql = f"""
            SELECT indexname, indexdef,
                pg_size_pretty(pg_relation_size(indexname::regclass)) AS index_size,
                idx_scan AS scans
            FROM pg_indexes
            JOIN pg_stat_user_indexes ON pg_indexes.indexname = pg_stat_user_indexes.indexrelname
                AND pg_indexes.schemaname = pg_stat_user_indexes.schemaname
            WHERE pg_indexes.tablename = '{tbl_name}';
            """
            table_analysis[tbl] = {
                "scan_stats": execute_query(scan_sql),
                "indexes": execute_query(idx_sql),
            }
        except Exception as e:
            table_analysis[tbl] = {"error": str(e)}
    report["sections"]["table_analysis"] = table_analysis

    # ── Step 6: Generate tuning options summary ──────────────────────────
    tuning_options = []

    for issue in plan_issues:
        if issue["fix_type"] == "index" and issue.get("table"):
            tuning_options.append({
                "option": f"Add index on '{issue['table']}'",
                "type": "immediate",
                "detail": f"Table '{issue['table']}' is being sequentially scanned. "
                          f"Identify the WHERE/JOIN columns from the query and create a targeted index. "
                          f"I can run suggest_missing_indexes('{issue['table']}') for specific column recommendations, "
                          f"then create_index_concurrently to implement it.",
                "impact": "high",
            })
        elif issue["fix_type"] == "analyze" and issue.get("table"):
            tuning_options.append({
                "option": f"Run ANALYZE on '{issue['table']}'",
                "type": "immediate",
                "detail": f"Row estimates are off — the planner is making bad decisions. "
                          f"I can run analyze_table('{issue['table']}') right now.",
                "impact": "medium",
            })
        elif issue["fix_type"] == "work_mem_or_index":
            tuning_options.append({
                "option": "Increase work_mem or add index to avoid sort",
                "type": "short_term",
                "detail": "Sort is spilling to disk. Options: (1) SET work_mem = '256MB' for this session, "
                          "(2) Add an index that matches the ORDER BY to avoid the sort entirely, "
                          "(3) Increase work_mem in the parameter group for all sessions.",
                "impact": "high",
            })
        elif issue["fix_type"] == "index_or_rewrite":
            tuning_options.append({
                "option": "Rewrite query or add indexes to enable Hash/Merge Join",
                "type": "medium_term",
                "detail": "Nested Loop is processing too many rows. Add indexes on join columns "
                          "so the planner can choose a Hash or Merge Join instead.",
                "impact": "high",
            })

    # Check pg_stat_statements for cache issues
    stats = report["sections"].get("pg_stat_statements", [])
    if isinstance(stats, list):
        for s in stats:
            cache_pct = s.get("cache_hit_pct", 100)
            if isinstance(cache_pct, (int, float)) and cache_pct < 95:
                tuning_options.append({
                    "option": "Improve buffer cache hit ratio for this query",
                    "type": "immediate",
                    "detail": f"Cache hit ratio is {cache_pct}% — too many disk reads. "
                              f"Add indexes to reduce the number of blocks scanned.",
                    "impact": "high",
                })
            temp_w = s.get("temp_blks_written", 0)
            if isinstance(temp_w, (int, float)) and temp_w > 0:
                tuning_options.append({
                    "option": "Reduce temp file usage",
                    "type": "short_term",
                    "detail": f"Query is writing {temp_w} temp blocks (sorts/hashes spilling to disk). "
                              f"Increase work_mem or add indexes to avoid sorts.",
                    "impact": "medium",
                })

    # Check for stale stats
    for tbl, analysis in table_analysis.items():
        scan_stats = analysis.get("scan_stats", [])
        if isinstance(scan_stats, list) and scan_stats:
            s = scan_stats[0]
            last_analyze = s.get("last_analyze") or s.get("last_autovacuum")
            dead = s.get("dead_tuples", 0)
            if not last_analyze:
                tuning_options.append({
                    "option": f"Run ANALYZE on '{tbl}' (never analyzed)",
                    "type": "immediate",
                    "detail": f"Table '{tbl}' has never been analyzed. Planner stats are likely wrong.",
                    "impact": "high",
                })
            if isinstance(dead, (int, float)) and dead > 10000:
                tuning_options.append({
                    "option": f"VACUUM '{tbl}' ({dead:,} dead tuples)",
                    "type": "immediate",
                    "detail": f"Table has {dead:,} dead tuples causing bloat. I can run vacuum_table('{tbl}').",
                    "impact": "medium",
                })

    if not tuning_options:
        tuning_options.append({
            "option": "No obvious issues detected from the plan",
            "type": "info",
            "detail": "The execution plan looks reasonable. Consider running EXPLAIN ANALYZE "
                      "for actual timing data, or check if the issue is at the application/connection level.",
            "impact": "low",
        })

    report["sections"]["tuning_options"] = tuning_options

    return json.dumps(report, default=str)


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — Performance Insights (Read-Only)
# ══════════════════════════════════════════════════════════════════════════════

def _get_pi_resource_id() -> str:
    """Resolve the DbiResourceId needed by Performance Insights API."""
    try:
        rds = boto3.client("rds", region_name=AWS_REGION)
        inst_id = AURORA_INSTANCE_ID
        if not inst_id and AURORA_CLUSTER_ID:
            # Pick the writer instance from the cluster
            cluster = rds.describe_db_clusters(DBClusterIdentifier=AURORA_CLUSTER_ID)["DBClusters"][0]
            for m in cluster.get("DBClusterMembers", []):
                if m["IsClusterWriter"]:
                    inst_id = m["DBInstanceIdentifier"]
                    break
        if not inst_id:
            return ""
        inst = rds.describe_db_instances(DBInstanceIdentifier=inst_id)["DBInstances"][0]
        return inst.get("DbiResourceId", "")
    except Exception as e:
        log.warning(f"Could not resolve PI resource ID: {e}")
        return ""


@tool
def get_pi_top_sql(period_minutes: int = 60, max_results: int = 10) -> str:
    """
    Get the top SQL queries by database load from Performance Insights.
    Shows which queries are consuming the most CPU, IO, and wait time.

    Args:
        period_minutes: How far back to look (default 60 minutes).
        max_results: Maximum number of queries to return (default 10).
    """
    resource_id = _get_pi_resource_id()
    if not resource_id:
        return json.dumps({"error": "Could not resolve Performance Insights resource ID. "
                           "Set AURORA_INSTANCE_ID or AURORA_CLUSTER_ID."})
    try:
        pi = boto3.client("pi", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)

        response = pi.describe_dimension_keys(
            ServiceType="RDS",
            Identifier=resource_id,
            StartTime=start_time,
            EndTime=end_time,
            Metric="db.load.avg",
            PeriodInSeconds=300,
            GroupBy={"Group": "db.sql", "Dimensions": ["db.sql.statement", "db.sql.tokenized_id"], "Limit": max_results},
        )
        keys = []
        for k in response.get("Keys", []):
            dims = k.get("Dimensions", {})
            keys.append({
                "sql_statement": dims.get("db.sql.statement", "")[:300],
                "sql_id": dims.get("db.sql.tokenized_id", ""),
                "avg_db_load": round(k.get("Total", 0), 4),
                "partitions": {p["Metric"]: round(p.get("Value", 0), 4)
                               for p in k.get("Partitions", [])},
            })
        return json.dumps({
            "instance": AURORA_INSTANCE_ID or AURORA_CLUSTER_ID,
            "period_minutes": period_minutes,
            "metric": "db.load.avg (Average Active Sessions)",
            "top_sql": keys,
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e),
                           "hint": "Performance Insights may not be enabled on this instance."})


@tool
def get_pi_wait_events(period_minutes: int = 60) -> str:
    """
    Get the top wait events from Performance Insights, broken down by wait type.
    Shows what the database is spending time waiting on (CPU, IO, Lock, LWLock, etc.).

    Args:
        period_minutes: How far back to look (default 60 minutes).
    """
    resource_id = _get_pi_resource_id()
    if not resource_id:
        return json.dumps({"error": "Could not resolve Performance Insights resource ID."})
    try:
        pi = boto3.client("pi", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)

        response = pi.describe_dimension_keys(
            ServiceType="RDS",
            Identifier=resource_id,
            StartTime=start_time,
            EndTime=end_time,
            Metric="db.load.avg",
            PeriodInSeconds=300,
            GroupBy={"Group": "db.wait_event", "Dimensions": ["db.wait_event.name", "db.wait_event.type"], "Limit": 20},
        )
        events = []
        for k in response.get("Keys", []):
            dims = k.get("Dimensions", {})
            events.append({
                "wait_event": dims.get("db.wait_event.name", ""),
                "wait_type": dims.get("db.wait_event.type", ""),
                "avg_db_load": round(k.get("Total", 0), 4),
            })
        return json.dumps({
            "instance": AURORA_INSTANCE_ID or AURORA_CLUSTER_ID,
            "period_minutes": period_minutes,
            "wait_events": events,
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_pi_db_load_by_dimension(group_by: str = "db.wait_event", period_minutes: int = 60) -> str:
    """
    Get database load sliced by a specific dimension from Performance Insights.
    Useful for understanding load distribution across different dimensions.

    Args:
        group_by: Dimension to group by. Options:
            - db.wait_event: Wait events (CPU, IO, Lock, etc.)
            - db.sql: SQL queries
            - db.user: Database users
            - db.host: Client hosts
            - db.application: Application names
            - db.session_type: Session types (foreground/background)
        period_minutes: How far back to look (default 60 minutes).
    """
    valid_groups = ["db.wait_event", "db.sql", "db.user", "db.host", "db.application", "db.session_type"]
    if group_by not in valid_groups:
        return json.dumps({"error": f"Invalid group_by. Must be one of: {', '.join(valid_groups)}"})

    resource_id = _get_pi_resource_id()
    if not resource_id:
        return json.dumps({"error": "Could not resolve Performance Insights resource ID."})
    try:
        pi = boto3.client("pi", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)

        response = pi.describe_dimension_keys(
            ServiceType="RDS",
            Identifier=resource_id,
            StartTime=start_time,
            EndTime=end_time,
            Metric="db.load.avg",
            PeriodInSeconds=300,
            GroupBy={"Group": group_by, "Limit": 15},
        )
        keys = []
        for k in response.get("Keys", []):
            keys.append({
                "dimensions": k.get("Dimensions", {}),
                "avg_db_load": round(k.get("Total", 0), 4),
            })
        return json.dumps({
            "instance": AURORA_INSTANCE_ID or AURORA_CLUSTER_ID,
            "group_by": group_by,
            "period_minutes": period_minutes,
            "results": keys,
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_pi_counter_metrics(period_minutes: int = 60) -> str:
    """
    Get key Performance Insights counter metrics: buffer cache hit ratio, transactions/sec,
    tuples fetched/returned/inserted/updated/deleted, and active connections.
    These are the OS and database counters visible in the PI dashboard.

    Args:
        period_minutes: How far back to look (default 60 minutes).
    """
    resource_id = _get_pi_resource_id()
    if not resource_id:
        return json.dumps({"error": "Could not resolve Performance Insights resource ID."})
    try:
        pi = boto3.client("pi", region_name=AWS_REGION)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=period_minutes)

        # Key counter metrics
        metric_queries = [
            {"Metric": "db.Cache.blks_hit.avg"},
            {"Metric": "db.Cache.blks_read.avg"},
            {"Metric": "db.Transactions.xact_commit.avg"},
            {"Metric": "db.Transactions.xact_rollback.avg"},
            {"Metric": "db.Tuples.tup_fetched.avg"},
            {"Metric": "db.Tuples.tup_returned.avg"},
            {"Metric": "db.Tuples.tup_inserted.avg"},
            {"Metric": "db.Tuples.tup_updated.avg"},
            {"Metric": "db.Tuples.tup_deleted.avg"},
            {"Metric": "db.Checkpoint.checkpoint_write_time.avg"},
            {"Metric": "db.Connections.numbackends.avg"},
            {"Metric": "os.cpuUtilization.total.avg"},
            {"Metric": "os.memory.free.avg"},
        ]

        response = pi.get_resource_metrics(
            ServiceType="RDS",
            Identifier=resource_id,
            StartTime=start_time,
            EndTime=end_time,
            PeriodInSeconds=300,
            MetricQueries=metric_queries,
        )

        metrics = {}
        for mq in response.get("MetricList", []):
            key = mq.get("Key", {}).get("Metric", "unknown")
            datapoints = mq.get("DataPoints", [])
            if datapoints:
                # Get the latest value
                latest = sorted(datapoints, key=lambda x: x.get("Timestamp", ""))[-1]
                metrics[key] = round(latest.get("Value", 0), 4)
            else:
                metrics[key] = "N/A"

        # Compute cache hit ratio
        blks_hit = metrics.get("db.Cache.blks_hit.avg", 0)
        blks_read = metrics.get("db.Cache.blks_read.avg", 0)
        if isinstance(blks_hit, (int, float)) and isinstance(blks_read, (int, float)):
            total = blks_hit + blks_read
            metrics["_cache_hit_ratio_pct"] = round(100.0 * blks_hit / total, 2) if total > 0 else 100.0

        return json.dumps({
            "instance": AURORA_INSTANCE_ID or AURORA_CLUSTER_ID,
            "period_minutes": period_minutes,
            "counters": metrics,
        }, default=str)
    except Exception as e:
        return json.dumps({"error": str(e),
                           "hint": "Performance Insights may not be enabled. Enable it in the RDS console."})


# ══════════════════════════════════════════════════════════════════════════════
#  TOOLS — AWS Documentation Search
# ══════════════════════════════════════════════════════════════════════════════

@tool
def search_aws_docs(query: str, service: str = "aurora-postgresql") -> str:
    """
    Search AWS documentation for Aurora PostgreSQL, RDS, Performance Insights,
    CloudWatch best practices, and troubleshooting. Uses the Bedrock LLM with
    AWS documentation knowledge to provide answers with references.

    Args:
        query: The search query — e.g. "Aurora PostgreSQL vacuum best practices"
        service: AWS service context — aurora-postgresql, rds, cloudwatch, performance-insights, etc.
    """
    search_query = f"AWS {service} {query}"
    try:
        bedrock_rt = boto3.client("bedrock-runtime", region_name=AWS_REGION)
        system = (
            "You are an AWS documentation assistant specializing in Aurora PostgreSQL, RDS, "
            "Performance Insights, and CloudWatch. Answer the question using your knowledge "
            "of official AWS documentation. Be specific and practical:\n"
            "- Cite documentation page names and URLs where possible\n"
            "- Include relevant SQL, CLI, or API examples\n"
            "- Reference specific parameter names and their recommended values\n"
            "- Mention relevant CloudWatch metrics and Performance Insights counters\n"
            "- If you are not confident about specific version numbers or recent changes, say so\n"
            "- Always suggest checking docs.aws.amazon.com for the latest information"
        )
        resp = bedrock_rt.converse(
            modelId=BEDROCK_MODEL_ID,
            system=[{"text": system}],
            messages=[{"role": "user", "content": [{"text": search_query}]}],
            inferenceConfig={"maxTokens": 2048, "temperature": 0.1},
        )
        answer = resp["output"]["message"]["content"][0]["text"]
        return json.dumps({"status": "success", "source": "llm_knowledge",
                           "query": search_query, "answer": answer,
                           "caveat": "Based on model training data. Verify against docs.aws.amazon.com for latest info."}, default=str)
    except Exception as e:
        return json.dumps({"error": str(e), "query": search_query})


@tool
def check_health() -> dict:
    """Check system health and configuration status."""
    db_ok = False
    try:
        conn = _get_connection()
        conn.close()
        db_ok = True
    except Exception:
        pass
    return {
        "status": "healthy" if db_ok else "degraded",
        "service": "DataOps Agent",
        "version": "2.0.0",
        "model": BEDROCK_MODEL_ID,
        "database_connected": db_ok,
        "aurora_cluster_id": AURORA_CLUSTER_ID or "not_set",
        "capabilities": [
            "get_database_summary", "get_largest_tables", "get_unused_indexes",
            "get_table_bloat", "get_index_bloat", "get_top_queries",
            "list_aurora_clusters", "get_aurora_instance_details",
            "get_aurora_replica_lag", "get_aurora_wait_events", "get_aurora_active_sessions",
            "get_cloudwatch_cpu_utilization", "get_cloudwatch_db_connections",
            "get_cloudwatch_storage_metrics",
            "explain_query", "get_query_stats", "suggest_missing_indexes",
            "get_table_column_stats", "deep_analyze_query",
            "get_pi_top_sql", "get_pi_wait_events", "get_pi_db_load_by_dimension",
            "get_pi_counter_metrics",
            "search_aws_docs",
            "create_index_concurrently", "analyze_table", "vacuum_table",
        ],
    }


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT DEFINITION + AGENTCORE ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

AGENT_SYSTEM_PROMPT = """You are an Autonomous Database Supervisor Agent for Aurora PostgreSQL,
deployed on Amazon Bedrock AgentCore Runtime.

You combine the capabilities of a Health Check Agent and an Action Agent to provide
end-to-end Aurora database management: diagnose issues AND implement safe fixes.

## PostgreSQL Diagnostic Tools (Read-Only)
- get_database_summary: Database overview (size, connections, version)
- get_largest_tables: Top tables by size
- get_unused_indexes: Wasted indexes
- get_table_bloat: Dead tuple analysis
- get_index_bloat: Index bloat detection
- get_top_queries: Slowest queries via aurora_stat_plans() with execution plans

## Query Tuning & Explain Plan Analysis
- explain_query: Run EXPLAIN / EXPLAIN ANALYZE on a SELECT query to get the JSON execution plan.
  Use this to identify sequential scans, nested loops, sort spills, and missing indexes.
  When the user provides a query, ALWAYS run explain_query first, then analyze the plan.
- get_query_stats: Get detailed per-query statistics from pg_stat_statements — total time,
  mean time, calls, rows, cache hit ratio, temp blocks. Filter by query substring.
- suggest_missing_indexes: Analyze a table's scan patterns, existing indexes, and column
  statistics to recommend new indexes.
- get_table_column_stats: Get column-level data distribution stats (n_distinct, correlation,
  null fraction, most common values) — essential for understanding selectivity and index choices.
- deep_analyze_query: ALL-IN-ONE deep analysis tool. When a user pastes a slow query, use THIS
  tool instead of calling individual tools one by one. It runs the full chain automatically:
  (1) EXPLAIN plan, (2) automated plan issue detection (Seq Scans, sort spills, row mismatches,
  high-cost nodes), (3) pg_stat_statements history, (4) Performance Insights load data,
  (5) table scan stats and index analysis for every table in the plan, (6) consolidated
  tuning_options with severity, type (immediate/short_term/medium_term), and impact.

  WHEN TO USE deep_analyze_query:
  - User says "this query is slow" and pastes SQL → use deep_analyze_query
  - User says "analyze this query" → use deep_analyze_query
  - User says "why is this query slow" → use deep_analyze_query
  - User asks to "tune" or "optimize" a specific query → use deep_analyze_query

  AFTER deep_analyze_query returns, you MUST:
  1. Summarize the plan issues found (Seq Scans, sort spills, etc.)
  2. Show the pg_stat_statements history (how often it runs, avg time, cache hit %)
  3. Show if it appears in Performance Insights top SQL
  4. Present ALL tuning_options as a numbered list with clear descriptions
  5. For each option, explain the trade-offs and expected improvement
  6. Offer to implement immediate fixes (create index, analyze, vacuum)

## Performance Insights (Read-Only — requires PI enabled on the instance)
- get_pi_top_sql: Top SQL queries by database load (Average Active Sessions). Shows which
  queries are consuming the most CPU, IO, and wait time. This is the PI "Top SQL" tab.
- get_pi_wait_events: Top wait events by DB load — shows what the database is spending time
  waiting on (CPU, IO:DataFileRead, Lock:transactionid, LWLock, Client:ClientRead, etc.).
- get_pi_db_load_by_dimension: Slice DB load by any dimension — wait_event, sql, user, host,
  application, or session_type. Use this for deep-dive analysis.
- get_pi_counter_metrics: Key database counters — buffer cache hit ratio, transactions/sec,
  tuples fetched/returned/inserted/updated/deleted, checkpoint write time, connections, OS CPU, memory.

PERFORMANCE INSIGHTS WORKFLOW:
1. Start with get_pi_counter_metrics to get an overview of DB health
2. If cache hit ratio is low or CPU is high, use get_pi_top_sql to find the culprit queries
3. Use get_pi_wait_events to understand what the DB is waiting on
4. For a specific wait type, use get_pi_db_load_by_dimension(group_by="db.sql") to find
   which queries are causing that wait
5. Take the top SQL from PI and run explain_query on it to get the execution plan
6. Use suggest_missing_indexes on tables doing Seq Scans in the plan
7. Produce a DIAGNOSIS + RECOMMENDATIONS section (see below)
8. If the user approves a fix, implement it and verify

QUERY TUNING WORKFLOW:
1. Start with get_pi_counter_metrics + get_pi_top_sql for the big picture
2. User provides a slow query → run explain_query(query) to get the plan
3. Identify bottlenecks in the plan: Seq Scans on large tables, high cost nodes, sort spills
4. Use get_query_stats to check historical execution stats for the query
5. Use get_pi_wait_events to see if the query is blocked on IO, locks, or CPU
6. Use suggest_missing_indexes on tables doing Seq Scans
7. Use get_table_column_stats to verify column selectivity before recommending indexes
8. Recommend specific CREATE INDEX statements with rationale
9. If the user approves, use create_index_concurrently to implement
10. Re-run explain_query to verify improvement

## AUTOMATIC DIAGNOSIS & RECOMMENDATIONS (CRITICAL — ALWAYS DO THIS)

After collecting PI data, you MUST automatically analyze the results and produce actionable
recommendations. NEVER just dump raw PI data — always interpret it and tell the user what
to do. Structure your response as:

### Diagnosis
Summarize what the PI data shows: top wait events, top SQL, cache hit ratio, CPU, etc.

### Root Cause Analysis
Explain WHY the issues are happening based on the data:

WAIT EVENT → ROOT CAUSE → FIX mapping:

| Wait Event | Likely Cause | Recommended Fix |
|---|---|---|
| CPU | Expensive queries, missing indexes, bad plans | Run explain_query on top SQL → add indexes → analyze_table |
| IO:DataFileRead | Seq scans on large tables, low cache hit ratio | Add indexes to avoid seq scans, increase shared_buffers (param group) |
| IO:DataFileWrite | Heavy writes, checkpoint pressure | Tune checkpoint_completion_target, max_wal_size |
| Lock:transactionid | Long-running transactions blocking others | Find blocking queries with get_aurora_active_sessions, advise COMMIT/ROLLBACK |
| Lock:tuple | Row-level contention, hot rows | Reduce transaction scope, use SELECT FOR UPDATE SKIP LOCKED |
| LWLock:BufferMapping | Buffer pool contention, too many concurrent scans | Add indexes to reduce scans, consider instance upgrade |
| LWLock:lock_manager | Too many row locks held | Batch operations, reduce transaction size |
| Client:ClientRead | App not consuming results fast enough | Check app-side connection pooling, network latency |
| Client:ClientWrite | App sending data slowly | Check app-side issues, network |
| IO:BufFileRead/Write | Temp file spills from sorts/hashes | Increase work_mem, add indexes to avoid sorts |
| Timeout:VacuumDelay | Autovacuum running | Check get_table_bloat, tune autovacuum params if too aggressive |

COUNTER METRIC → THRESHOLD → FIX mapping:

| Metric | Warning Threshold | Fix |
|---|---|---|
| Cache hit ratio < 99% | Below 99% is concerning for OLTP | Add indexes to reduce full table scans, increase shared_buffers |
| Cache hit ratio < 95% | Critical — most reads hitting disk | Urgent: add missing indexes, check for seq scans on large tables |
| OS CPU > 80% | Sustained high CPU | Find top SQL via PI, optimize queries, consider instance upgrade |
| OS CPU > 95% | Critical | Immediate: kill long-running queries, add indexes, scale up |
| tup_returned >> tup_fetched | Seq scans returning many rows but fetching few | Missing indexes — the DB is scanning entire tables |
| xact_rollback high | Many failed transactions | Check application error handling, connection timeouts |
| Connections > 80% of max | Connection exhaustion risk | Implement connection pooling (PgBouncer/RDS Proxy) |
| checkpoint_write_time high | Checkpoint pressure | Increase max_wal_size, tune checkpoint_completion_target |

### Recommendations (Prioritized)
Always provide recommendations in this priority order:

**Immediate (can do now with agent tools):**
- CREATE INDEX CONCURRENTLY statements (use create_index_concurrently)
- ANALYZE on tables with stale stats (use analyze_table)
- VACUUM on bloated tables (use vacuum_table)
- For each, explain: what it fixes, expected impact, and the exact command

**Short-term (parameter changes — requires DB restart or param group update):**
- shared_buffers, work_mem, maintenance_work_mem, effective_cache_size changes
- autovacuum tuning (autovacuum_vacuum_scale_factor, autovacuum_analyze_scale_factor)
- checkpoint tuning (checkpoint_completion_target, max_wal_size)
- For each, provide: current value context, recommended value, and the AWS CLI or console steps

**Medium-term (application/architecture changes):**
- Query rewrites (provide the optimized SQL)
- Connection pooling (RDS Proxy or PgBouncer)
- Read replica offloading for read-heavy queries
- Partitioning for very large tables

**Long-term (infrastructure):**
- Instance class upgrade if CPU/memory constrained
- Aurora I/O-Optimized if IO costs are high
- Aurora Global Database for cross-region reads

### Offer to Implement
After presenting recommendations, ALWAYS ask:
"I can implement the immediate fixes now (create indexes, analyze tables, vacuum).
Which ones would you like me to apply?"

## Aurora-Specific Tools (Read-Only)
- list_aurora_clusters: Clusters with writer/reader endpoints
- get_aurora_instance_details: Instance class, AZ, Performance Insights
- get_aurora_replica_lag: Reader replica lag from CloudWatch
- get_aurora_wait_events: What sessions are waiting on
- get_aurora_active_sessions: Active queries and durations

## CloudWatch Metrics (Read-Only)
- get_cloudwatch_cpu_utilization: CPU usage (requires instance ID)
- get_cloudwatch_db_connections: Connection count
- get_cloudwatch_storage_metrics: Storage, IOPS, memory

## AWS Documentation Search
- search_aws_docs: Search AWS official documentation for Aurora PostgreSQL, RDS, CloudWatch
  best practices, troubleshooting guides, and configuration references.
  Use this when the user asks about AWS-specific features, configuration, best practices,
  or when you need to verify recommendations against official documentation.

## Action Tools (Safe Write Operations)
- create_index_concurrently: Create indexes without blocking
- analyze_table: Update table statistics
- vacuum_table: Reclaim dead space

## Workflow
1. DIAGNOSE: Run PI counters + wait events + top SQL to understand the current state
2. DEEP DIVE: For each issue, use explain_query, suggest_missing_indexes, get_table_column_stats
3. ANALYZE: Map wait events and metrics to root causes using the tables above
4. RECOMMEND: Present prioritized fixes (immediate → short-term → medium-term → long-term)
5. ACT: If the user approves, implement safe fixes one at a time
6. VERIFY: After each action, re-run the diagnostic to confirm improvement

## Safety Rules
- NEVER run DROP, DELETE, UPDATE, or TRUNCATE
- Always use CONCURRENTLY for index creation
- Implement fixes one at a time, reporting each result
- If unsure about safety, recommend manual intervention
- Block any PII from appearing in responses
- explain_query only accepts SELECT/WITH/VALUES — never explain DML

Format your responses with clear sections using markdown."""

model = BedrockModel(model_id=BEDROCK_MODEL_ID, region_name=AWS_REGION)

ALL_TOOLS = [
    # Health check (read-only)
    get_database_summary, get_largest_tables, get_unused_indexes,
    get_table_bloat, get_index_bloat, get_top_queries,
    # Query tuning & explain plan
    explain_query, get_query_stats, suggest_missing_indexes, get_table_column_stats,
    deep_analyze_query,
    # Performance Insights (read-only)
    get_pi_top_sql, get_pi_wait_events, get_pi_db_load_by_dimension, get_pi_counter_metrics,
    # Aurora-specific (read-only)
    list_aurora_clusters, get_aurora_instance_details, get_aurora_replica_lag,
    get_aurora_wait_events, get_aurora_active_sessions,
    # CloudWatch (read-only)
    get_cloudwatch_cpu_utilization, get_cloudwatch_db_connections,
    get_cloudwatch_storage_metrics,
    # AWS documentation
    search_aws_docs,
    # Actions (safe writes)
    create_index_concurrently, analyze_table, vacuum_table,
    # System
    check_health,
]

dataops_agent = Agent(
    model=model,
    tools=ALL_TOOLS,
    system_prompt=AGENT_SYSTEM_PROMPT,
)


@app.entrypoint
def invoke_dataops(payload, context=None):
    """AgentCore Runtime entrypoint — DataOps supervisor agent."""
    user_input = payload.get("prompt", "")
    action = payload.get("action", "agent")

    log.info(f"DataOps invocation | action={action} | prompt={user_input[:80]}...")

    if action == "health":
        return check_health()

    # Conversational agent mode — LLM decides which tool to use
    response = dataops_agent(user_input)
    agent_text = response.message["content"][0]["text"]
    return {"answer": agent_text, "action": action, "query": user_input}


if __name__ == "__main__":
    app.run()
