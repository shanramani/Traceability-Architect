import streamlit as st
import os
import datetime
import pandas as pd
import tempfile
import io

# --- 1. SAFE IMPORTS (Try/Except to prevent crashes) ---
try:
    from langchain_groq import ChatGroq
    GROQ_AVAILABLE = True
except: GROQ_AVAILABLE = False

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except: OPENAI_AVAILABLE = False

try:
    from langchain_anthropic import ChatAnthropic
    ANTHROPIC_AVAILABLE = True
except: ANTHROPIC_AVAILABLE = False

from langchain_community.document_loaders import PyPDFLoader

# --- 2. CONFIG & UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }

    /* The Hero Banner */
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px;
        border-radius: 24px;
        color: white;
        margin-bottom: 30px;
        box-shadow: 0 10px 20px rgba(0,122,255,0.2);
        text-align: center;
    }
    .hero-banner h1 { font-size: 3rem; margin-bottom: 5px; font-weight: 600; }
    .hero-banner p { font-size: 1.4rem; opacity: 0.9; font-weight: 300; }

    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px);
        border-right: 1px solid #e5e5ea;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. SESSION LOGIC ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'model_provider' not in st.session_state: st.session_state.model_provider = "Llama 3.3 (Groq)"

# --- 4. SIDEBAR ENGINE ROUTER ---
with st.sidebar:
    st.title("Admin Controls")
    
    if st.session_state.authenticated:
        st.success(f"Verified: **{st.session_state.user_name}**")
        st.header("🤖 Engine Selection")
        
        available_engines = []
        if GROQ_AVAILABLE: available_engines.append("Llama 3.3 (Groq)")
        if OPENAI_AVAILABLE: available_engines.append("GPT-4o (OpenAI)")
        if ANTHROPIC_AVAILABLE: available_engines.append("Claude 3.5 (Anthropic)")
        
        # User selects the engine here
        st.session_state.model_provider = st.radio("Active Intelligence:", available_engines)
        
        if st.button("Revoke Access"):
            st.session_state.authenticated = False
            st.rerun()
    else:
        st.subheader("🔑 Secure Access")
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                st.rerun()

# --- 5. SHARED HEADER (LANDING & DASHBOARD) ---
st.markdown(f"""
    <div class="hero-banner">
        <h1>AI Powered GxP Validation Suite</h1>
        <p>Intelligence Engine: {st.session_state.model_provider}</p>
    </div>
""", unsafe_allow_html=True)

# --- 6. MAIN CONTENT ---
if not st.session_state.authenticated:
    st.markdown("""
        <div style="text-align: center; padding: 40px;">
            <h2 style="color: #1d1d1f;">Secure GxP Environment</h2>
            <p style="color: #8e8e93; font-size: 1.2rem;">Automated CSV authoring for URS, FRS, and Traceability Matrix.</p>
            <div style="margin-top: 30px; padding: 20px; background: white; border-radius: 16px; border: 1px solid #e5e5ea; display: inline-block;">
                <p style="color: #007aff; font-weight: 600; margin:0;">🔐 Please sign in via the sidebar to begin.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    

[Image of a software validation life cycle V-model]


else:
    # AUTHENTICATED WORKSPACE
    uploaded_file = st.file_uploader("📂 Upload URS / SOP PDF", type="pdf")
    
    if uploaded_file and st.button(f"🚀 Execute Validation via {st.session_state.model_provider}"):
        with st.spinner(f"Analyzing requirements..."):
            with tempfile.NamedTemporaryFile(delete
