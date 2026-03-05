import streamlit as st
import os
import requests

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.25"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Locked to Thousand Oaks, USA context [cite: 2025-12-28]
    return "Thousand Oaks, USA"

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    .stApp { background-color: #fcfcfd; }
    
    /* BANNER & LOGIN */
    .top-banner {
        background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
        padding: 12px 0px; text-align: center; margin-bottom: 5px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569; font-weight: 400; letter-spacing: 4px;
        text-transform: uppercase; font-size: 0.85rem; margin: 0;
    }
    [data-testid="stTextInput"] { width: 50% !important; margin: 0 auto !important; }

    /* SIDEBAR: NEW SUBTLE BACKGROUND COLOR */
    [data-testid="stSidebar"] { 
        background-color: #1e293b !important; /* Professional Slate-Charcoal */
        border-right: 1px solid #334155; 
    }
    
    /* KILL KEYBOARD_DOUBLE / Sidebar Header */
    [data-testid="stSidebar"] [data-testid="stHeader"], 
    [data-testid="stSidebarCollapseButton"],
    [title="keyboard_double_arrow_left"] { display: none !important; }

    /* MODERN BLUE BUTTON ENGINE */
    div.stButton > button {
        border: none !important;
        transition: all 0.2s ease-in-out !important;
    }

    /* VIBRANT BLUE FOR HIGH CONTRAST AGAINST CHARCOAL SIDEBAR */
    div.stButton > button[key="login_btn"], 
    div.stButton > button[key="terminate_sidebar"],
    div.stButton > button[key="run_analysis_btn"]:not(:disabled) {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
    }

    /* HOVER EFFECTS */
    div.stButton > button:hover:not(:disabled) {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 20px rgba(37, 99, 235, 0.45) !important;
        filter: brightness(1.1);
    }

    /* DISABLED STATE */
    div.stButton > button:disabled {
        background: #e2e8f0 !important;
        color: #94a3b8 !important;
        cursor: not-allowed !important;
    }

    /* SIDEBAR TEXT ELEMENTS */
    .sb-title { color: #f8fafc !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sb-sub { color: #cbd5e1 !important; font-weight: 700 !important; font-size: 0.95rem; }
    .system-spacer { margin-top: 80px; }
    .sidebar-stats { color: #f1f5f9 !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    /* SIDEBAR BUTTON WIDTH */
    div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }
    
    /* LOGIN CENTERING */
    .login-center { display: flex; justify-content: center; width: 100%; }
    .login-center div.stButton > button { width: 50% !important; }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

def show_login():
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")
        st.markdown('<div class="login-center">', unsafe_allow_html=True)
        if st.button("Initialize Secure Session", key="login_btn"):
            if u: st.session_state.user_name, st.session_state.authenticated = u, True; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

def show_app():
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
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        # White non-bold text for stats against the charcoal background
        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        
        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    
    is_ready = sop_file is not None
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.success(f"Analysis sequence initiated using {st.session_state.selected_model}.")

if not st.session_state.authenticated: show_login()
else: show_app()