import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & 2026 MINIMALIST STYLING ---
VERSION = "9.5"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; color: #0f172a; }
    .block-container { max-width: 850px; padding-top: 4rem; }
    .hero { text-align: center; margin-bottom: 3.5rem; }
    .hero h1 { font-weight: 600; font-size: 2.2rem; letter-spacing: -0.03em; margin-bottom: 0.5rem; }
    div[data-testid="stRadio"] > div {
        display: flex; flex-direction: row; justify-content: center;
        gap: 25px; border: 1px solid #e2e8f0; padding: 15px;
        border-radius: 12px; background: #f8fafc;
    }
    .status-dot { height: 8px; width: 8px; border-radius: 50%; display: inline-block; margin-right: 8px; }
    .online { background-color: #22c55e; box-shadow: 0 0 8px #22c55e; }
    .offline { background-color: #cbd5e1; }
    .stButton > button { 
        background: #0f172a; color: white; width: 100%; border-radius: 8px; 
        font-weight: 500; padding: 0.7rem; border: none; transition: 0.2s;
    }
    [data-testid="stSidebar"] { background-color: #f8fafc; border-right: 1px solid #e2e8f0; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE & AUDIT LOGGING ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []

def log_event(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.audit_trail.append({"Timestamp": timestamp, "User": st.session_state.user_name, "Action": action})

# --- 3. MODEL CONFIGURATION ---
model_config = {
    "Groq": {"id": "groq/llama-3.3-70b-versatile", "keys": ["GROQ_API_KEY"]},
    "OpenAI": {"id": "openai/gpt-4o", "keys": ["OPENAI_API_KEY"]},
    "Claude": {"id": "anthropic/claude-3-5-sonnet-20240620", "keys": ["ANTHROPIC_API_KEY"]},
    "Gemini": {"id": "gemini/gemini-1.5-pro", "keys": ["GEMINI_API_KEY", "GOOGLE_API_KEY"]} 
}

# --- 4. LANDING PAGE ---
def show_landing_page():
    st.markdown('<div class="hero"><h1>Traceability Architect Pro</h1><p>GAMP 5 Risk-Based Compliance Hub</p></div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 3, 1])
    with col:
        st.markdown("### 🔐 Secure Authorization")
        u = st.text_input("Professional ID", placeholder="Username")
        p = st.text_input("Security Key", type="password", placeholder="••••••••")
        if st.button("Initialize Engine"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                log_event("Session Initialized")
                st.rerun()

# --- 5. MAIN AUDIT DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.markdown("### System Health")
        for name, cfg in model_config.items():
            is_active = any(st.secrets.get(k) for k in cfg["keys"])
            status_class = "online" if is_active else "offline"
            st.markdown(f'<span class="status-dot {status_class}"></span>{name}', unsafe_allow_html=True)
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name} | Site: 91362")
        if st.checkbox("View Audit Ledger"):
            st.table(pd.DataFrame(st.session_state.audit_trail).tail(5))
        if st.button("Terminate Session"):
            log_event("User Logout")
            st.session_state.authenticated = False
            st.rerun()

    st.markdown("### Intelligence Engine")
    engine = st.radio("Provider", list(model_config.keys()), label_visibility="collapsed")
    st.divider()
    st.markdown("### Document Ingestion")
    uploaded_file = st.file_uploader("Upload SOP or URS (PDF)", type="pdf", label_visibility="collapsed")

    if uploaded_file and st.button("🚀 Execute Risk-Based Scan"):
        cfg = model_config[engine]
        api_key = next((st.secrets.get(k) for k in cfg["keys"] if st.secrets.get(k)), None)

        if not api_key:
            st.error(f"Engine {engine} is offline.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = " ".join([p.page_content for p in loader.load()])
                with st.spinner(f"Analyzing GAMP 5 Risk Profiles..."):
                    # Refined Prompt: Risk + Protocol + 10 Columns
                    prompt = (
                        f"Analyze: {full_text[:7500]}. Return pipe-separated rows with 10 columns. "
                        "Column 7 (OQ_Test) must be step-by-step: 'Step 1: [Action]; Expected Result: [Result].' "
                        "Column 10 (Risk) must be 'Low', 'Medium', or 'High' based on GAMP 5 categories. "
                        "Columns: ID | URS_Req | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA | Status | Risk"
                    )
                    try:
                        res = completion(model=cfg["id"], messages=[{"role": "user", "content": prompt}], api_key=api_key)
                    except Exception as primary_e:
                        st.warning(f"Failover to Groq triggered.")
                        res = completion(model=model_config["Groq"]["id"], messages=[{"role": "user", "content": prompt}], api_key=st.secrets.get("GROQ_API_KEY"))

                    raw_lines = res.choices[0].message.content.strip().split('\n')
                    rows = []
                    for line in raw_lines:
                        if '|' in line:
                            p = [item.strip() for item in line.split('|')]
                            rows.append(p[:10] if len(p) >= 10 else p + ["N/A"]*(10-len(p)))
                    st.session_state.master_data = rows
                    log_event(f"Matrix & Risk Assessment via {engine}")
                    st.success("Verification complete.")
            except Exception as e: st.error(f"System Error: {e}")
            finally: 
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.master_data:
        st.divider()
        cols = ["ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test (Protocol)", "Ref", "ALCOA", "Status", "Risk_Level"]
        df = pd.DataFrame(st.session_state.master_data, columns=cols)
        st.markdown("#### Validated Traceability & Risk Matrix")
        st.data_editor(df, use_container_width=True, hide_index=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Traceability_Risk_Matrix')
            pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='Audit_Trail_Log')
        
        st.download_button(label="💾 Download Certified Workbook", data=output.getvalue(), 
                           file_name=f"GxP_Audit_Risk_{datetime.date.today()}.xlsx",
                           on_click=lambda: log_event("Workbook Exported to Site 91362"))

# --- 6. ROUTING ---
if not st.session_state.authenticated: show_landing_page()
else: show_main_dashboard()