import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import requests

VERSION = "10.23"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    return "Thousand Oaks, USA"

# --- SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

# --- STYLES ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }

.stApp { background-color: #fcfcfd; }

/* BANNER */
.top-banner {
    background-color: white;
    border: 1px solid #eef2f6;
    border-radius: 10px;
    padding: 12px 0px;
    text-align: center;
    margin-bottom: 5px;
}

.banner-text-inner {
    color: #475569;
    font-weight: 400;
    letter-spacing: 4px;
    text-transform: uppercase;
    font-size: 0.85rem;
    margin: 0;
}

/* CENTER TEXT INPUTS */
[data-testid="stTextInput"] {
    width: 55% !important;
    margin: 0 auto !important;
}

/* SIDEBAR */
[data-testid="stSidebar"] {
    background-color: #0f172a;
    border-right: 1px solid #1e293b;
}

/* REMOVE SIDEBAR HEADER */
[data-testid="stSidebar"] [data-testid="stHeader"],
[data-testid="stSidebar"] header,
[data-testid="stSidebarCollapseButton"],
button[aria-label="Collapse sidebar"] {
    display: none !important;
}

/* SIDEBAR TEXT */
.sb-title {
    color: white !important;
    font-weight: 700 !important;
    font-size: 1.1rem;
    margin-bottom: 20px;
}

.sb-sub {
    color: white !important;
    font-weight: 700 !important;
    font-size: 0.95rem;
    margin-bottom: 10px;
}

/* MOVE SYSTEM CONTEXT LOWER */
.system-spacer {
    margin-top: 120px;   /* <<< CHANGE >>> moved down more */
}

.sidebar-stats {
    color: white !important;
    font-size: 0.85rem;
}

/* LOGIN BUTTON */
div.stButton > button[key="login_btn"] {
    width: 320px !important;     /* <<< CHANGE >>> longer button */
    display:block;
    margin:auto;
    background-color:#2563eb !important;
    color:white !important;
}

/* RUN ANALYSIS BUTTON */
div.stButton > button[key="run_analysis_btn"] {
    width:340px !important;   /* <<< CHANGE >>> wider */
    display:block;
    margin:auto;
    background-color:#2563eb !important;
    color:white !important;
    padding:0.75rem 2rem !important;
    font-size:1.1rem !important;
    border-radius:8px !important;
}

/* DISABLED STYLE */
div.stButton > button[key="run_analysis_btn"]:disabled {
    background-color:#e2e8f0 !important;
    color:#94a3b8 !important;
}

/* SIDEBAR TERMINATE BUTTON */
div.stButton > button[key="terminate_sidebar"] {
    width:200px !important;      /* <<< CHANGE >>> centered button */
    display:block;
    margin:auto;
    background-color:#2563eb !important;
    color:white !important;
}
</style>
""", unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- LOGIN ---
def show_login():
    _, col, _ = st.columns([1,2,1])

    with col:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")

        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")

        if st.button("Initialize Secure Session", key="login_btn"):   # <<< CHANGE
            if u:
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()

# --- MAIN APP ---
def show_app():

    with st.sidebar:

        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()

        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)

        engine_name = st.selectbox(
            "Model",
            list(MODELS.keys()),
            index=list(MODELS.keys()).index(st.session_state.selected_model),
            label_visibility="collapsed"
        )

        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name
            st.rerun()

        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)

        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")

        st.divider()

        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        if st.button("Terminate Session", key="terminate_sidebar"):   # <<< centered
            st.session_state.authenticated = False
            st.rerun()

    st.title("Auto-Generate CSV Documents")

    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")

    is_ready = sop_file is not None

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.success(f"Analysis sequence initiated using {st.session_state.selected_model}.")

if not st.session_state.authenticated:
    show_login()
else:
    show_app()