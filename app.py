import streamlit as st
import os
import datetime
import pandas as pd
import tempfile
import io

# --- 1. SAFE IMPORTS ---
try:
    from langchain_groq import ChatGroq
    GROQ_AVAILABLE = True
except: GROQ_AVAILABLE = False

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except: OPENAI_AVAILABLE = False

from langchain_community.document_loaders import PyPDFLoader

# --- 2. CONFIG & UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }

    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px;
        border-radius: 24px;
        color: white;
        margin-bottom: 30px;
        box-shadow: 0 10px 20px rgba(0,122,255,0.2);
        text-align: center;
    }
    .hero-banner h1 { font-size: 2.8rem; margin-bottom: 5px; font-weight: 600; }
    .hero-banner p { font-size: 1.2rem; opacity: 0.9; font-weight: 300; }

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

def extract_table(text):
    try:
        lines = [line for line in text.split('\n') if '|' in line]
        if len(lines) > 2:
            raw_data = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(raw_data), sep='|', skipinitialspace=True).dropna(axis=1, how='all')
            df.columns = [c.strip() for c in df.columns]
            df = df[~df.iloc[:,0].str.contains('---', na=False)]
            return df.replace('', pd.NA).ffill().bfill()
        return None
    except: return None

# --- 4. SIDEBAR ---
with st.sidebar:
    st.title("Admin Controls")
    if st.session_state.authenticated:
        st.success(f"Verified: **{st.session_state.user_name}**")
        available_engines = []
        if GROQ_AVAILABLE: available_engines.append("Llama 3.3 (Groq)")
        if OPENAI_AVAILABLE: available_engines.append("GPT-4o (OpenAI)")
        st.session_state.model_provider = st.radio("Active Intelligence:", available_engines)
        
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()
    else:
