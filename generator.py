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
VERSION = "10.24"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Persistent location context for 91362 [cite: 2025-12-28]
    return "Thousand Oaks, USA"

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

# --- 3. STYLES ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }

.stApp { background-color: #fcfcfd; }

/* BANNER & LOGIN */
.top-banner {
    background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
    padding: 12px 0px; text-align: center; margin-bottom: 5px;
    box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
}
.banner-text-inner {
    color: #475569; font-weight: 400; letter-spacing: 4px;
    text-transform: uppercase; font-size: 0.85rem; margin: 0;
}
[data-testid="stTextInput"] { width: 50% !important; margin: 0 auto !important; }

/* MODERN BLUE BUTTONS */
div.stButton > button { border: none !important; transition: all 0.2s ease-in-out !important; }

/* Login and Sidebar Terminate buttons */
div.stButton > button[key="login_btn"], 
div.stButton > button[key="terminate_sidebar"] {
    background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
    color: white !important;
    box-shadow: 0 4px 12px rgba(37,99,235,0.2);
}

/* Run Analysis - active */
div.stButton > button[key="run_analysis_btn"] {
    background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
    color: white !important;
    padding: 0.75rem 3rem !important; 
    font-size: 1.1rem !important;
    border-radius: 8px !important;
    box-shadow: 0 4px 15px rgba(37,99,235,0.3);
}

/* Hover effects */
div.stButton > button:hover:not(:disabled) {
    transform: translateY(-2px) !important;
    box-shadow: 0 8px 20px rgba(37,99,235,0.4) !important;
    filter: brightness(1.1);
}

/* Disabled state */
div.stButton > button:disabled {
    background: #e2e8f0 !important;
    color: #94a3b8 !important;
    cursor: not-allowed !important;
    transform: none !important;
    box-shadow: none !important;
}

/* SIDEBAR */
[data-testid="stSidebar"] { background-color: #1e2a44; border-right: 1px solid #16203a; }
.sb-title, .sb-sub, .sidebar-stats { color: #ffffff !important; }
[data-testid="stSidebar"] [data-testid="stHeader"], 
[data-testid="stSidebar"] [data-testid="stSidebarCollapseButton"],
[title="keyboard_double_arrow_left"] { display: none !important; }

.sb-title { font-weight:700 !important; font-size:1.1rem; }
.sb-sub { font-weight:700 !important; font-size:0.95rem; }
.system-spacer { margin-top:80px; }
.sidebar-stats { font-weight:400 !important; font-size:0.85rem; margin-bottom:5px; }

/* Sidebar terminate width */
div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }

/* LOGIN BUTTON - Center vertically and horizontally */
.login-wrapper {
    display: flex;
    justify-content: center; /* horizontal center */
    align-items: center;     /* vertical center */
    height: 80vh;            /* almost full screen */
    flex-direction: column;
}
.login-center div.stButton > button { width: 700px !important; max-width: 90%; margin:auto; }
</style>
""", unsafe_allow_html=True)

# --- 4. MODELS ---
MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 5. AUTHENTICATION ---
def show_login():
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")
        
        # Wrap login button in flex container for vertical + horizontal centering
        st.markdown('<div class="login-wrapper"><div class="login-center">', unsafe_allow_html=True)
        if st.button("Initialize Secure Session", key="login_btn"):
            if u:
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()
        st.markdown('</div></div>', unsafe_allow_html=True)

# --- 6. MAIN APPLICATION ---
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
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown("<br><br>", unsafe_allow_html=True)
        
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
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

# --- 7. ROUTER ---
if not st.session_state.authenticated:
    show_login()
else:
    show_app()