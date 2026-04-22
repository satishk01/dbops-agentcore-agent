"""
deploy_dataops_agent.py — Deploy DataOps Agent to Bedrock AgentCore Runtime
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Uses bedrock-agentcore-starter-toolkit: configure() → launch().

Prerequisites:
  pip install bedrock-agentcore bedrock-agentcore-starter-toolkit strands-agents boto3 psycopg2-binary

Usage:
  python deploy_dataops_agent.py
"""
from bedrock_agentcore_starter_toolkit import Runtime

REGION     = "us-east-1"
AGENT_NAME = "dataops_supervisor_agent"

print()
print("=" * 60)
print("  DataOps Agent — AgentCore Deployment")
print("  Aurora PostgreSQL: health checks, diagnostics, safe actions")
print("=" * 60)
print()

# ── Configure ─────────────────────────────────────────────────────────────────
agentcore_runtime = Runtime()

print(f"Configuring AgentCore Runtime...")
print(f"  Agent Name:  {AGENT_NAME}")
print(f"  Region:      {REGION}")
print(f"  Entrypoint:  dataops_agent.py")
print()

configure_response = agentcore_runtime.configure(
    entrypoint="dataops_agent.py",
    auto_create_execution_role=True,
    auto_create_ecr=True,
    requirements_file="requirements.txt",
    region=REGION,
    agent_name=AGENT_NAME,
    container_runtime=None,
)

print("Configuration complete")

# ── Launch ────────────────────────────────────────────────────────────────────
print("\nLaunching agent to AgentCore Runtime...")
print("This may take 5-10 minutes...\n")

launch_result = agentcore_runtime.launch()

print(f"Launch complete!")
print(f"  Agent ARN: {launch_result.agent_arn}")
print()
print("=" * 60)
print("  DEPLOYMENT COMPLETE")
print("=" * 60)
print()
print("Next steps:")
print(f"  1. Copy the Agent ARN above")
print(f"  2. Set it for the Streamlit UI:")
print(f"     export AGENT_ARN={launch_result.agent_arn}")
print(f"  3. Run the UI:")
print(f"     streamlit run dataops_ui.py")
print()
print("Test the agent:")
print(f'  agentcore invoke \'{{"prompt": "", "action": "health"}}\'')
