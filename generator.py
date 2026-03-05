import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import requests

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.10"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# Location Logic for 91362 / Thousand Oaks
def get_location():
    try:
        data = requests.get('https://ipapi.co/json/', timeout=2).json()
        if any(x in data.get('city', '') for x in ["The Dalles", "Council Bluffs", "Ashburn"]):
             return "Thousand Oaks, USA"
        return f"{data.get('city', 'Thousand Oaks')}, {data.get('country_name', 'USA')}"
    except:
        return "Thousand Oaks, USA"

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    .stApp { background-color: #fcfcfd; }
    
    /* Sidebar Aesthetics */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.25rem; margin-bottom: 0px; }
    .sb-sub { color: white !important; font-weight: 700 !important; font-size: 1rem; margin-top: 15px; margin-bottom: 5px; }
    
    /* Sidebar Labels/Captions */
    [data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { 
        color: #f8fafc !important; 
    }

    /* ULTRA-TIGHT File Uploader - SIDEBAR ONLY */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] section {
        background-color: white !important;
        border: 1px solid #cbd5e1 !important;
        border-radius: 6px !important;
        padding: 0px 4px !important;
        min-height: 38px !important;
    }
    
    /* Hide Sidebar Instructions */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] section div div { display: none; }
    
    /* FIX: Sidebar "Browse Files" Button - BLUE BACKGROUND */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] button {
        background-color: #2563eb !important;
        color: white !important;
        border: none !important;
        padding: 2px 10px !important;
        font-size: 0.75rem !important;
        font-weight: 600 !important;
    }

    /* Selected File - Small Blue Font */
    [data-testid="stSidebar"] [data-testid="stFileUploaderFileName"] {
        color: #2563eb !important;
        font-size: 0.72rem !important;
        font-weight: 600 !important;
    }

    .banner-text { color: #64748b; font-weight: 600; letter-spacing: 1.5px; text-transform: uppercase; font-size: 0.8rem; margin-bottom: -10px; }
    
    /* Main Action Button (Run Analysis) */
    .stButton > button { 
        background-color: #2563eb !important; 
        color: white !important; 
        border-radius: 8px !important; 
        font-weight: 600 !important;
        padding: 0.6rem 2rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION & PERSISTENCE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 3. AUTHENTICATION ---
def show_login():
    _, col, _ = st.columns([1, 1.5, 1])
    with col:
        st.markdown('<div style="text-align: center; padding: 3rem; background: white; border-radius: 16px; border: 1px solid #e2e8f0;">', unsafe_allow_html=True)
        st.markdown('<p class="banner-text">AI OPTIMIZED CSV</p>', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")
        if st.button("Initialize Secure Session"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# --- 4. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        current_index = list(MODELS.keys()).index(st.session_state.selected_model)
        engine_name = st.selectbox("Model selection", list(MODELS.keys()), index=current_index, label_visibility="collapsed")
        
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name
            st.rerun()
        
        st.divider()
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        # Persistent key ensures the file doesn't vanish on rerun
        system_guide = st.file_uploader("SysGuide", type="pdf", key="sidebar_sys_context", label_visibility="collapsed")
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name}")
        st.caption(f"Location: {st.session_state.location}")
        if st.button("Terminate Session"): 
            st.session_state.authenticated = False
            st.rerun()

    st.title("Auto-Generate CSV Documents")
    st.info("Ingest Business SOPs to generate context-aware Functional Specs, OQ Protocols and Traceability matrix.")
    
    # Main SOP Uploader
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_input")

    # FIX: Check if file exists in session state to show button
    if st.session_state.main_sop_input is not None:
        if st.button("🚀 Run Analysis"):
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
                with st.spinner(f"Processing with {st.session_state.selected_model}..."):
                    prompt = (
                        f"Requirements: {sop_text}\nTechnical Context: {sys_context}\n\n"
                        "Return pipe-separated: URS_ID | URS_Desc | FS_ID | FS_Detail | OQ_ID | OQ_Protocol | Risk | Ref | Justification"
                    )
                    res = completion(model=model_id, messages=[{"role":"user","content":prompt}], api_key=api_key)
                    data = [l.split('|') for l in res.choices[0].message.content.strip().split('\n') if '|' in l]
                    st.session_state.master_df = pd.DataFrame([d[:9] if len(d)>=9 else d+["N/A"]*(9-len(d)) for d in data], 
                                                            columns=["URS_ID", "URS_Description", "FS_ID", "FS_Detail", "OQ_ID", "OQ_Protocol", "Risk", "Ref", "Justification"])
            except Exception as e: st.error(f"Engine Error: {e}")

    if st.session_state.master_df is not None:
        st.divider()
        t1, t2, t3, t4, t5 = st.tabs(["📋 URS", "⚙️ FS", "🧪 OQ Scripts", "🔗 Trace Matrix", "⚠️ Gap Analysis"])
        with t1: st.dataframe(st.session_state.master_df[["URS_ID", "URS_Description", "Risk"]].drop_duplicates(), use_container_width=True, hide_index=True)
        # (Other tabs follow same logic...)

if not st.session_state.authenticated: show_login()
else: show_app()