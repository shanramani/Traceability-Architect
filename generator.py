import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.3"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# Custom Professional Theme
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    /* Main Background */
    .stApp { background-color: #fcfcfd; }
    .main .block-container { max-width: 1200px; padding-top: 2rem; }
    
    /* Sidebar Aesthetics - Keeping dark for contrast, but text is crisp */
    [data-testid="stSidebar"] { background-color: #0f172a; color: white; border-right: 1px solid #1e293b; }
    [data-testid="stSidebar"] * { color: #f8fafc !important; }
    
    /* LIGHT MODE RESET for Input Boxes and Buttons */
    /* This ensures text inputs and buttons remain high-contrast and professional */
    div[data-baseweb="input"] { background-color: white !important; border-radius: 8px !important; }
    input { color: #0f172a !important; }
    
    /* Button Styling - Blue Professional */
    .stButton > button { 
        background-color: #2563eb !important; 
        color: white !important; 
        border-radius: 8px !important; 
        border: none !important;
        font-weight: 500 !important;
    }
    .stButton > button:hover { background-color: #1d4ed8 !important; }

    /* Tab Styling */
    .stTabs [data-baseweb="tab-list"] { gap: 12px; background: #f1f5f9; padding: 8px; border-radius: 12px; }
    .stTabs [data-baseweb="tab"] { 
        padding: 10px 20px; border-radius: 8px; border: none; font-weight: 600; 
    }
    .stTabs [aria-selected="true"] { background-color: #ffffff !important; box-shadow: 0 4px 6px -1px rgb(0 0 0 / 0.1); color: #0f172a !important; }

    /* Login Screen Center */
    .login-box { text-align: center; padding: 3rem; background: white; border-radius: 16px; border: 1px solid #e2e8f0; box-shadow: 0 10px 15px -3px rgb(0 0 0 / 0.1); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION & AUDIT ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []
# PERSISTENCE: Initialize selected_model if not present
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"

def log_event(action):
    st.session_state.audit_trail.append({"Timestamp": datetime.datetime.now().strftime("%H:%M:%S"), "User": st.session_state.user_name, "Action": action})

# Model Mapping for LiteLLM
MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 3. AUTHENTICATION GATE ---
def show_login():
    _, col, _ = st.columns([1, 1.5, 1])
    with col:
        st.markdown('<div class="login-box">', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        st.subheader("GxP Validation Intelligence")
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password", placeholder="••••••••")
        if st.button("Initialize Secure Session"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                log_event("Login Success")
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# --- 4. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f"### 🚀 Architect v{VERSION}")
        st.divider()
        
        # PERSISTENCE: Multi-Model Switcher with session state binding
        st.markdown("#### 🤖 Intelligence Engine")
        
        # Find index for selectbox persistence
        current_index = list(MODELS.keys()).index(st.session_state.selected_model)
        
        engine_name = st.selectbox(
            "Select AI Model to use", 
            list(MODELS.keys()), 
            index=current_index,
            key="model_selector"
        )
        
        # Update session state when changed
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name
            log_event(f"Engine switched to {engine_name}")
        
        st.divider()
        st.markdown("#### 📂 Target System Context")
        system_guide = st.file_uploader("Upload System Guide (SAP/LIMS etc.)", type="pdf")
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name}")
        st.caption(f"Location: 91362")
        if st.button("Terminate Session"): 
            st.session_state.authenticated = False
            st.rerun()

    # Main Body
    st.title("Auto-Generate CSV Documents")
    st.info("Ingest Business SOPs/User Guides to generate context-aware Functional Specs, OQ Protocols and Traceability matrix.")
    
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf")

    if sop_file and st.button("🚀 Run Analysis"):
        model_id = MODELS[st.session_state.selected_model]
        provider = model_id.split('/')[0]
        api_key = st.secrets.get(f"{provider.upper()}_API_KEY") or st.secrets.get("GEMINI_API_KEY")

        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(sop_file.getvalue()); sop_path = tmp.name
        
        sys_context = ""
        if system_guide:
            with tempfile.NamedTemporaryFile(delete=False) as tmp_s:
                tmp_s.write(system_guide.getvalue()); sys_path = tmp_s.name
            sys_context = " ".join([p.page_content for p in PyPDFLoader(sys_path).load()])[:10000]

        try:
            sop_text = " ".join([p.page_content for p in PyPDFLoader(sop_path).load()])[:8000]
            with st.spinner(f"Requesting {st.session_state.selected_model}..."):
                prompt = (
                    f"Requirements: {sop_text}\nTechnical Context: {sys_context}\n\n"
                    "Return pipe-separated: URS_ID | URS_Desc | FS_ID | FS_Detail | OQ_ID | OQ_Protocol | Risk | Ref | Justification"
                )
                res = completion(model=model_id, messages=[{"role":"user","content":prompt}], api_key=api_key)
                data = [l.split('|') for l in res.choices[0].message.content.strip().split('\n') if '|' in l]
                st.session_state.master_df = pd.DataFrame([d[:9] if len(d)>=9 else d+["N/A"]*(9-len(d)) for d in data], 
                                                        columns=["URS_ID", "URS_Description", "FS_ID", "FS_Detail", "OQ_ID", "OQ_Protocol", "Risk", "Ref", "Justification"])
                log_event(f"Matrix Generated via {st.session_state.selected_model}")
        except Exception as e: st.error(f"Engine Error: {e}")

    if st.session_state.master_df is not None:
        st.divider()
        df = st.session_state.master_df
        t1, t2, t3, t4, t5, t6 = st.tabs(["📋 URS", "⚙️ FS", "🧪 OQ Scripts", "🔗 Trace Matrix", "⚠️ Gap Analysis", "📑 Audit Log"])
        
        with t1: st.dataframe(df[["URS_ID", "URS_Description", "Risk"]].drop_duplicates(), use_container_width=True, hide_index=True)
        with t2: st.dataframe(df[["URS_ID", "FS_ID", "FS_Detail"]], use_container_width=True, hide_index=True)
        with t3: st.dataframe(df[["FS_ID", "OQ_ID", "OQ_Protocol"]], use_container_width=True, hide_index=True)
        with t4: st.dataframe(df[["URS_ID", "FS_ID", "OQ_ID", "Risk"]], use_container_width=True, hide_index=True)
        with t5: 
            gaps = df[df['Justification'].str.contains('MISSING|N/A', na=False, case=False)]
            st.warning(f"Detected {len(gaps)} gaps.")
            st.dataframe(gaps, use_container_width=True, hide_index=True)
        with t6: st.table(st.session_state.audit_trail)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Traceability', index=False)
        st.download_button("📥 Download GxP Workbook", data=output.getvalue(), file_name="Validation_Package.xlsx")

# --- 5. EXECUTION ---
if not st.session_state.authenticated: show_login()
else: show_app()