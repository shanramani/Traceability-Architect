import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.5", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.5 (Smart RTM Merging)")

if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

def extract_table(text):
    try:
        lines = [line for line in text.split('\n') if '|' in line]
        if len(lines) > 2:
            raw_data = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(raw_data), sep='|', skipinitialspace=True).dropna(axis=1, how='all')
            df.columns = [c.strip() for c in df.columns]
            df = df[~df.iloc[:,0].str.contains('---', na=False)]
            
            # --- SMART MERGE LOGIC ---
            # Group by the first column (URS_ID) and take the first non
