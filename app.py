import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.1", layout="wide", page_icon="🏗️")

# Pharma-grade CSS for clean documents
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    .signature-box { border: 1px solid #d1d4d9; padding: 20px; border-radius: 5px; background-color: #ffffff; margin-top: 20px; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.1 (Audit-Ready Artifacts)")

if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

# --- 2. SIDEBAR ---
with st.sidebar:
    st.header("📝 Document Controls")
    proj_name = st.text_input("System Name", "BioLogistics v1.0")
    doc_id = st.text_input("Document ID", "FRS-2026-001")
    author = st.text_input("Author (CSV Lead)", "Shan")
    review_date = datetime.date.today().strftime("%d-%b-%Y")
    
    st.divider()
    status = st.selectbox("Document Status", ["Draft", "In Review", "Approved"])
    st.info(f"Generated on: {review_date}")

# --- 3. PROCESSING ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if
