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
VERSION = "10.14"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    try:
        data = requests.get('https://ipapi.co/json/', timeout=2).json()
        cloud_cities = ["The Dalles", "Council Bluffs", "Ashburn", "Boardman", "Dublin"]
        if any(x in data.get('city', '') for x in cloud_cities) or "Google" in data.get('org', ''):
             return "Thousand Oaks, USA"
        return f"{data.get('city', 'Thousand Oaks')}, {data.get('country_name', 'USA')}"
    except:
        return "Thousand Oaks, USA"

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    .stApp { background-color: #fcfcfd; }
    
    /* SLEEK MODERN BANNER FIX */
    .top-banner {
        background-color: white;
        border: 1px solid #eef2f6;
        border-radius: 10px;
        padding: 12px 0px; /* Thinner padding for a sleek look */
        text-align: center;
        margin-bottom: 25px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569; /* Sleek Slate Grey */
        font-weight: 400; /* Modern thinner weight */
        letter-spacing: 4px; /* Increased breathing room */
        text-transform: uppercase;
        font-size: 0.85rem; /* Smaller, more sophisticated size */
        margin: 0;
    }

    /* Sidebar Refinement */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.1rem; margin-bottom: 5px; }
    .sb-sub { color: #94a3b8 !important; font-weight: 600 !important; font-size: 0.9rem; margin-top: 15px; margin-bottom: 5px; }
    
    /* Mute keyboard_double and tooltips */
    [data-testid="stSidebar"] [title="keyboard_double_arrow_left"], 
    [data-testid="stSidebar"] [data-testid="stIconChild"] { display: none !important; }
    div[data-testid="stTooltipContent"] { display: none !important; }

    /* File Uploader UI - SIDEBAR */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] section {
        background-color: white !important;
        border: 1px solid #cbd5e1 !important;
        border-radius: 6px !important;
        padding: 4px !important;
        min-height: 40px !important;
    }
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] section div div { display: none; }
    
    /* High-Contrast Sidebar Button */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] button {
        background-color: #2563eb !important;
        color: white !important;
        border: none !important;
        padding: 4px 14px !important;
        font-size: 0.75rem !important;
        font-weight: 700 !important;
    }

    /* Login & Buttons */
    .login-box { text-align: center; padding: 1rem; }
    .stButton > button { background-color: #2563eb !important; color: white !important; border-radius: 8px !important; font-weight: 600 !important; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
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
    _, col, _ = st.columns([1, 2, 1])
    with col:
        # THE MODERN SLEEK BANNER
        st.markdown('''
            <div class="top-banner">
                <p class="banner-text-inner">AI OPTIMIZED CSV</p>
            </div>
        ''', unsafe_allow_html=True)
        
        st.markdown('<div class="login-box">', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")
        if st.button("Initialize Secure Session"):
            if u: st.session_state.user_name, st.session_state.authenticated = u, True; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# --- 4. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">Intelligence Engine</p>', unsafe_allow_html=True)
        
        current_index = list(MODELS.keys()).index(st.session_state.selected_model)
        engine_name = st.selectbox("Engine", list(MODELS.keys()), index=current_index, label_visibility="collapsed")
        
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
        
        st.divider()
        st.markdown('<p class="sb-sub">Target System Context</p>', unsafe_allow_html=True)
        system_guide = st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name}")
        st.caption(f"Location: {st.session_state.location}")
        if st.button("Terminate Session"): 
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    st.info("Ingest Business SOPs to generate context-aware Functional Specs and Traceability matrix.")
    
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")

    if st.session_state.get("main_sop_uploader") is not None:
        if st.button("🚀 Run Analysis"):
            # Model logic would live here
            pass

if not st.session_state.authenticated: show_login()
else: show_app()