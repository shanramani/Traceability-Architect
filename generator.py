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
VERSION = "10.22"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Persistent location context for 91362 [cite: 2025-12-28]
    return "Thousand Oaks, USA"

# --- 2. SESSION STATE INITIALIZATION (BEFORE UI) ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()
if 'file_is_uploaded' not in st.session_state: st.session_state.file_is_uploaded = False

# CALLBACK: This triggers the instant a file is added or removed
def update_file_state():
    if st.session_state.main_sop_uploader is not None:
        st.session_state.file_is_uploaded = True
    else:
        st.session_state.file_is_uploaded = False

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
    
    /* SIDEBAR HARDENING */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    
    /* EXTREME KILL for keyboard_double / Sidebar Header */
    [data-testid="stSidebar"] [data-testid="stHeader"], 
    [data-testid="stSidebar"] header,
    [data-testid="stSidebarCollapseButton"],
    button[aria-label="Collapse sidebar"],
    .st-emotion-cache-6qob1r, /* Common dynamic class for sidebar header */
    [title="keyboard_double_arrow_left"] {
        display: none !important;
        visibility: hidden !important;
        height: 0px !important;
    }

    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.1rem; margin-bottom: 20px;}
    .sb-sub { color: white !important; font-weight: 700 !important; font-size: 0.95rem; margin-bottom: 10px; }
    
    .system-spacer { margin-top: 80px; }
    .sidebar-stats { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    /* BUTTONS */
    div.stButton > button[key="terminate_sidebar"] {
        width: 100% !important; background-color: #2563eb !important; color: white !important;
    }
    .login-center { display: flex; justify-content: center; width: 100%; }
    div.stButton > button:first-child:not([key="terminate_sidebar"]):not([key="run_analysis_btn"]) { 
        width: 50% !important; background-color: #2563eb !important; color: white !important; 
    }
    div.stButton > button[key="run_analysis_btn"] {
        background-color: #2563eb !important; color: white !important;
        padding: 0.75rem 3rem !important; font-size: 1.1rem !important;
    }
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
        if st.button("Initialize Secure Session"):
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
            st.session_state.selected_model = engine_name
            st.rerun()
        
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        
        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    
    # The Uploader now has a callback function to lock state
    st.file_uploader("Upload SOP (The 'What')", type="pdf", 
                     key="main_sop_uploader", 
                     on_change=update_file_state)

    # We check the session state, which is now persistent across sidebar model swaps
    if st.session_state.file_is_uploaded:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚀 Run Analysis", key="run_analysis_btn"):
            st.success(f"Logic Engine: {st.session_state.selected_model} is ready.")

if not st.session_state.authenticated: show_login()
else: show_app()