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
VERSION = "10.17"
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
    
    /* SLEEK MODERN BANNER */
    .top-banner {
        background-color: white;
        border: 1px solid #eef2f6;
        border-radius: 10px;
        padding: 12px 0px;
        text-align: center;
        margin-bottom: 25px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569;
        font-weight: 400;
        letter-spacing: 4px;
        text-transform: uppercase;
        font-size: 0.85rem;
        margin: 0;
    }

    /* LOGIN INPUTS - HALF WIDTH & CENTERED */
    [data-testid="stTextInput"] {
        width: 50% !important;
        margin: 0 auto !important;
    }

    /* FIX: CENTERED LOGIN BUTTON WITH BLUE BACKGROUND */
    .stButton {
        display: flex;
        justify-content: center;
    }
    
    /* Targets only the Login button to be Blue and Half-Width */
    div.stButton > button:first-child {
        background-color: #2563eb !important;
        color: white !important;
        width: 50% !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        height: 3rem !important;
    }

    /* Sidebar Branding */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.1rem; margin-bottom: 5px; }
    .sb-sub { color: white !important; font-weight: 700 !important; font-size: 0.95rem; margin-top: 15px; margin-bottom: 5px; }
    
    /* Hide keyboard_double on hover */
    [data-testid="stSidebar"] [title="keyboard_double_arrow_left"], 
    [data-testid="stSidebar"] [data-testid="stIconChild"] { display: none !important; }

    /* Sidebar Uploader Button - Blue */
    [data-testid="stSidebar"] div[data-testid="stFileUploader"] button {
        background-color: #2563eb !important;
        color: white !important;
        border: none !important;
        font-weight: 700 !important;
    }

    [data-testid="stSidebar"] [data-testid="stFileUploaderFileName"] {
        color: #2563eb !important;
        font-weight: 700 !important;
    }

    .login-box { text-align: center; padding: 1rem; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o": "openai/gpt-4o"
}

# --- 3. AUTHENTICATION ---
def show_login():
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown('''
            <div class="top-banner">
                <p class="banner-text-inner">AI OPTIMIZED CSV</p>
            </div>
        ''', unsafe_allow_html=True)
        
        st.markdown('<div class="login-box">', unsafe_allow_html=True)
        st.title("🛡️ Validation Doc Assist")
        
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Token", type="password")
        
        # Center the Login Button
        if st.button("Initialize Secure Session"):
            if u: st.session_state.user_name, st.session_state.authenticated = u, True; st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

# --- 4. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        
        current_index = list(MODELS.keys()).index(st.session_state.selected_model)
        engine_name = st.selectbox("Engine", list(MODELS.keys()), index=current_index, label_visibility="collapsed")
        
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
        
        st.divider()
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        system_guide = st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name}")
        st.caption(f"Location: {st.session_state.location}") # Dynamically shows Thousand Oaks [cite: 2025-12-28]
        if st.button("Terminate Session"): 
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    st.info("Ingest Business SOPs to generate context-aware Functional Specs and Traceability matrix.")
    
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")

    if st.session_state.get("main_sop_uploader") is not None:
        if st.button("🚀 Run Analysis"):
            st.success("Analysis sequence initiated.")

if not st.session_state.authenticated: show_login()
else: show_app()