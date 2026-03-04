import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & 2026 MINIMALIST STYLING ---
VERSION = "9.0"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; color: #0f172a; }
    .block-container { max-width: 850px; padding-top: 4rem; }
    .hero { text-align: center; margin-bottom: 3rem; }
    
    /* Horizontal Radio Alignment */
    div[data-testid="stRadio"] > div {
        display: flex; flex-direction: row; justify-content: center;
        gap: 25px; border: 1px solid #e2e8f0; padding: 15px;
        border-radius: 12px; background: #f8fafc;
    }

    /* Status Indicators */
    .status-dot { height: 8px; width: 8px; border-radius: 50%; display: inline-block; margin-right: 8px; }
    .online { background-color: #22c55e; box-shadow: 0 0 8px #22c55e; }
    .offline { background-color: #cbd5e1; }

    /* Custom Audit Log Table */
    .audit-log { font-size: 0.85rem; color: #64748b; background: #f1f5f9; padding: 10px; border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE & AUDIT LOGGING ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []

def log_event(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.session_state.audit_trail.append({"Timestamp": timestamp, "User": st.session_state.user_name, "Action": action})

# --- 3. LANDING PAGE ---
def show_landing_page():
    st.markdown('<div class="hero"><h1>Traceability Architect</h1><p>V9.0 | GAMP 5 Compliance Hub</p></div>', unsafe_allow_html=True)
    _, col, _ = st.columns([1, 4, 1])
    with col:
        u = st.text_input("Professional ID", placeholder="Username")
        p = st.text_input("Security Key", type="password")
        if st.button("Initialize Session"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                log_event("User Login")
                st.rerun()

# --- 4. MAIN DASHBOARD ---
def show_main_dashboard():
    model_config = {
        "Groq": {"id": "groq/llama-3.3-70b-versatile", "keys": ["GROQ_API_KEY"]},
        "OpenAI": {"id": "openai/gpt-4o", "keys": ["OPENAI_API_KEY"]},
        "Claude": {"id": "anthropic/claude-3-5-sonnet-20240620", "keys": ["ANTHROPIC_API_KEY"]},
        "Gemini": {"id": "gemini/gemini-1.5-pro-latest", "keys": ["GEMINI_API_KEY", "GOOGLE_API_KEY"]}
    }

    with st.sidebar:
        st.markdown("### System Health")
        for name, cfg in model_config.items():
            is_active = any(st.secrets.get(k) for k in cfg["keys"])
            status_class = "online" if is_active else "offline"
            st.markdown(f'<span class="status-dot {status_class}"></span>{name}', unsafe_allow_html=True)
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name} | Site: 91362")
        
        # Show Audit Log in Sidebar for transparency
        if st.checkbox("Show Session Audit Log"):
            st.markdown("#### §11.10(e) Trail")
            st.table(pd.DataFrame(st.session_state.audit_trail).tail(5))
            
        if st.button("Sign Out"): 
            log_event("User Logout")
            st.session_state.authenticated = False
            st.rerun()

    st.markdown("### Intelligence Engine")
    engine = st.radio("Provider", list(model_config.keys()), label_visibility="collapsed")
    
    st.divider()
    uploaded_file = st.file_uploader("Upload PDF Source", type="pdf")

    if uploaded_file and st.button("Execute Compliance Scan"):
        cfg = model_config[engine]
        api_key = next((st.secrets.get(k) for k in cfg["keys"] if st.secrets.get(k)), None)

        if not api_key:
            st.error(f"Engine {engine} is offline.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = " ".join([p.page_content for p in loader.load()])
                with st.spinner(f"Auditing via {engine}..."):
                    prompt = (f"Analyze: {full_text[:7000]}. Return pipe-separated rows with 9 columns: "
                              f"ID | URS_Text | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA_Score | Status.")
                    res = completion(model=cfg["id"], messages=[{"role": "user", "content": prompt}], api_key=api_key)
                    raw_lines = res.choices[0].message.content.strip().split('\n')
                    rows = []
                    for line in raw_lines:
                        if '|' in line:
                            p = [item.strip() for item in line.split('|')]
                            rows.append(p[:9] if len(p) >= 9 else p + ["N/A"]*(9-len(p)))
                    st.session_state.master_data = rows
                    log_event(f"AI Analysis Completed using {engine}")
                    st.success("Verification complete.")
            except Exception as e: st.error(f"Error: {e}")
            finally: 
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.master_data:
        st.divider()
        df = pd.DataFrame(st.session_state.master_data, columns=["ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Ref", "ALCOA", "Status"])
        st.dataframe(df, use_container_width=True, hide_index=True)

        # Download logic with Audit Event
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, index=False, sheet_name='Traceability')
            # Append Audit Log to a separate sheet in the Excel for Compliance
            pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='Audit_Trail')
        
        st.download_button(
            label="💾 Download Certified Workbook",
            data=output.getvalue(),
            file_name=f"GxP_Audit_{datetime.date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            on_click=lambda: log_event("Excel Workbook Exported")
        )

# --- 5. ROUTING ---
if not st.session_state.authenticated: show_landing_page()
else: show_main_dashboard()