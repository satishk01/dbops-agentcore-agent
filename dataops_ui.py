"""
dataops_ui.py — Streamlit Chat UI for DataOps Agent on Bedrock AgentCore
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Responsive chat interface for Aurora PostgreSQL database operations.
Talks to the DataOps AgentCore for health checks, diagnostics, and safe actions.

Usage:
  export AGENT_ARN=arn:aws:bedrock-agentcore:us-east-1:********************
  streamlit run dataops_ui.py
"""
import streamlit as st
import boto3
import json
import uuid
import time

# ── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataOps Agent",
    page_icon="🗄️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Configuration (hardcoded — single source of truth) ────────────────────────
AGENT_ARN = "arn:aws:bedrock-agentcore:us-east-1:**********************"  # ← FILL IN after deployment: arn:aws:bedrock-agentcore:us-east-1:ACCOUNT:runtime/dataops_supervisor_agent-XXXXX
REGION = "us-east-1"

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
:root {
    --bg-primary: #FFFFFF; --bg-secondary: #F5F5F7; --text-primary: #1D1D1F;
    --text-secondary: #6E6E73; --text-tertiary: #86868B; --accent: #0071E3;
    --accent-light: #E1F0FF; --border: #D2D2D7; --border-light: #E5E5EA;
    --shadow-sm: 0 1px 3px rgba(0,0,0,0.06); --shadow-md: 0 4px 12px rgba(0,0,0,0.08);
    --radius-md: 12px; --radius-xl: 20px;
    --user-bubble: #0071E3; --assistant-bubble: #F5F5F7; --success: #34C759;
}
html, body, [data-testid="stAppViewContainer"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    -webkit-font-smoothing: antialiased;
}
#MainMenu, footer, header, [data-testid="stToolbar"] { display: none !important; }
.stDeployButton { display: none !important; }
.main .block-container { max-width: 860px; margin: 0 auto; padding: 0 16px 100px 16px; }
</style>
""", unsafe_allow_html=True)

st.markdown("""
<style>
.topbar {
    position: sticky; top: 0; z-index: 999;
    background: rgba(255,255,255,0.72);
    backdrop-filter: saturate(180%) blur(20px);
    border-bottom: 0.5px solid var(--border-light);
    padding: 12px 20px; display: flex; align-items: center; justify-content: space-between;
    margin: -1rem -16px 0 -16px;
}
.topbar-title { font-size: 17px; font-weight: 600; color: var(--text-primary); }
.topbar-subtitle { font-size: 12px; color: var(--text-tertiary); }
.topbar-badge {
    display: inline-flex; align-items: center; gap: 4px;
    font-size: 11px; font-weight: 500; color: var(--success);
    background: rgba(52,199,89,0.1); padding: 3px 8px; border-radius: 100px;
}
[data-testid="stChatMessage"] { background: transparent !important; border: none !important; padding: 4px 0 !important; }
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-user"]) .stMarkdown p {
    background: var(--user-bubble); color: #FFFFFF; padding: 10px 16px;
    border-radius: 18px 18px 4px 18px; display: inline-block; max-width: 85%;
    font-size: 15px; line-height: 1.45;
}
[data-testid="stChatMessage"]:has([data-testid="chatAvatarIcon-assistant"]) .stMarkdown {
    background: var(--assistant-bubble); padding: 14px 18px;
    border-radius: 18px 18px 18px 4px; max-width: 95%; font-size: 15px;
    line-height: 1.55; color: var(--text-primary); box-shadow: var(--shadow-sm);
}
[data-testid="chatAvatarIcon-user"], [data-testid="chatAvatarIcon-assistant"] { display: none !important; }
[data-testid="stChatInput"] textarea {
    font-family: 'Inter', -apple-system, sans-serif !important; font-size: 15px !important;
    border-radius: var(--radius-xl) !important; border: 1px solid var(--border) !important;
    padding: 12px 18px !important; box-shadow: var(--shadow-md) !important;
}
[data-testid="stChatInput"] textarea:focus {
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px rgba(0,113,227,0.15), var(--shadow-md) !important;
}
.meta-pill {
    font-size: 11px; font-weight: 500; color: var(--text-tertiary);
    background: var(--bg-secondary); padding: 3px 10px; border-radius: 100px;
    display: inline-flex; align-items: center; gap: 4px; margin-right: 6px;
}
.stButton > button { border-radius: var(--radius-xl) !important; font-weight: 500 !important; }
.stButton > button[kind="primary"] { background: var(--accent) !important; border: none !important; color: #fff !important; }
@media (max-width: 768px) {
    .main .block-container { max-width: 100%; padding: 0 8px 100px 8px; }
    .topbar { padding: 10px 12px; }
}
</style>
""", unsafe_allow_html=True)


# ── Session State ─────────────────────────────────────────────────────────────
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())
if "messages" not in st.session_state:
    st.session_state.messages = []


# ── AgentCore Invocation ──────────────────────────────────────────────────────
def invoke_agent(prompt: str, action: str = "agent") -> dict:
    """Call the DataOps agent via AgentCore Runtime."""
    if not AGENT_ARN:
        return {"error": "AGENT_ARN not set. Export it before running: export AGENT_ARN=arn:aws:..."}
    payload = {
        "prompt": prompt,
        "action": action,
        "session_id": st.session_state.session_id,
    }
    for attempt in range(3):
        try:
            client = boto3.client("bedrock-agentcore", region_name=REGION)
            response = client.invoke_agent_runtime(
                agentRuntimeArn=AGENT_ARN,
                runtimeSessionId=st.session_state.session_id,
                payload=json.dumps(payload),
            )
            raw = response["response"].read()
            body = json.loads(raw)
            if isinstance(body, str):
                try:
                    body = json.loads(body)
                except (json.JSONDecodeError, TypeError):
                    return {"answer": body}
            return body
        except Exception as e:
            if "500" in str(e) and attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            return {"error": str(e)}
    return {"error": "Max retries exceeded"}


def extract_answer(body: dict) -> str:
    if isinstance(body, str):
        return body
    if not isinstance(body, dict):
        return str(body)
    if "error" in body:
        return f"⚠️ {body['error']}"
    for key in ("answer", "response", "output", "text", "content"):
        if key in body:
            return str(body[key])
    return str(body)


# ── Top Bar ───────────────────────────────────────────────────────────────────
status_badge = "● Connected" if AGENT_ARN else "○ No Agent ARN"
status_color = "var(--success)" if AGENT_ARN else "#FF3B30"
st.markdown(f"""
<div class="topbar">
    <div>
        <div class="topbar-title">🗄️ DataOps Agent</div>
        <div class="topbar-subtitle">Aurora PostgreSQL · Health Checks · Query Tuning · AWS Docs · Safe Actions</div>
    </div>
    <div class="topbar-badge" style="color:{status_color}">{status_badge}</div>
</div>
""", unsafe_allow_html=True)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("🗄️ DataOps Agent")
    st.divider()
    if st.button("🔄 New Session", use_container_width=True):
        st.session_state.session_id = str(uuid.uuid4())
        st.session_state.messages = []
        st.rerun()
    st.caption(f"Session: `{st.session_state.session_id[:8]}…`")
    if AGENT_ARN:
        st.caption(f"Agent: `…{AGENT_ARN[-20:]}`")
    else:
        st.warning("Set AGENT_ARN env var before running.")


# ── Quick Actions (when no messages) ─────────────────────────────────────────
QUICK_ACTIONS = [
    "Run a full database health check",
    "Show Performance Insights top SQL by load",
    "What are the top wait events from PI?",
    "Show PI counter metrics (cache hit ratio, TPS)",
    "Show the slowest queries",
    "Deep analyze: SELECT * FROM orders WHERE status = 'pending' AND created_at > '2025-01-01'",
    "Suggest missing indexes for the orders table",
    "Find unused indexes",
    "Check for table bloat",
    "How do I tune Aurora PostgreSQL vacuum settings?",
    "List Aurora clusters",
    "What are the active sessions?",
]

if not st.session_state.messages:
    st.markdown("""
    <div style="text-align:center;padding:40px 20px 20px 20px;">
        <div style="font-size:40px;margin-bottom:8px;">🗄️</div>
        <h2 style="font-size:22px;font-weight:600;color:#1D1D1F;margin:0 0 6px 0;">
            Aurora PostgreSQL Operations</h2>
        <p style="font-size:14px;color:#86868B;margin:0;">
            Powered by Amazon Bedrock AgentCore</p>
    </div>
    """, unsafe_allow_html=True)

    cols = st.columns(min(len(QUICK_ACTIONS), 4))
    for i, action_text in enumerate(QUICK_ACTIONS):
        with cols[i % 4]:
            if st.button(action_text, key=f"qa-{i}", use_container_width=True):
                st.session_state.messages.append({"role": "user", "content": action_text})
                st.rerun()


# ── Chat History ──────────────────────────────────────────────────────────────
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])


# ── Chat Input ────────────────────────────────────────────────────────────────
if user_input := st.chat_input("Ask about your Aurora database…"):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        with st.spinner(""):
            body = invoke_agent(user_input, action="agent")
            answer = extract_answer(body)
        st.markdown(answer)
        st.session_state.messages.append({"role": "assistant", "content": answer})
