import streamlit as st
import os

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.30"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Locked to Thousand Oaks, USA [cite: 2025-12-28]
    return "Thousand Oaks, USA"

if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    /* SIDEBAR REVERT TO DARK (v10.18 Style) */
    [data-testid="stSidebar"] { 
        background-color: #0f172a !important; 
    }
    
    /* KILL ARROW TEXT */
    [data-testid="stSidebarCollapseButton"], [title="keyboard_double_arrow_left"] { display: none !important; }

    /* FIX TERMINATE BUTTON (No more text breaking) */
    div.stButton > button[key="terminate_sidebar"] {
        width: 100% !important;
        background-color: #2563eb !important;
        color: white !important;
        border: none !important;
        padding: 0.5rem !important;
        min-height: 45px !important;
    }

    .sb-title { color: white !important; font-weight: 700; font-size: 1.1rem; }
    .sb-sub { color: #cbd5e1 !important; font-weight: 700; font-size: 0.95rem; margin-top: 20px; }
    .sidebar-stats { color: white !important; font-weight: 400; font-size: 0.85rem; margin-top: 5px; }
    .system-spacer { margin-top: 40px; }

    /* RUN ANALYSIS BUTTON */
    div.stButton > button[key="run_analysis_btn"] {
        background-color: #2563eb !important;
        color: white !important;
        padding: 0.75rem 2rem !important;
    }
    </style>
    """, unsafe_allow_html=True)

def show_login():
    st.title("🛡️ Validation Doc Assist")
    u = st.text_input("Professional Identity", placeholder="Username")
    if st.button("Initialize Secure Session", key="login_btn"):
        if u: st.session_state.user_name, st.session_state.authenticated = u, True; st.rerun()

def show_app():
    MODELS = {
        "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
        "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
        "GPT-4o": "openai/gpt-4o",
        "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
    }

    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        
        engine_name = st.selectbox("Model", list(MODELS.keys()), 
                                   index=list(MODELS.keys()).index(st.session_state.selected_model), 
                                   label_visibility="collapsed")
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
        
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        
        if st.button("Terminate Session", key="terminate_sidebar"):
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    
    is_ready = sop_file is not None
    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.success(f"Analysis sequence initiated using {st.session_state.selected_model}.")

if not st.session_state.authenticated: show_login()
else: show_app()