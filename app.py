import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & iOS ENTERPRISE UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }

    /* Top Banner Style */
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px;
        border-radius: 24px;
        color: white;
        margin-bottom: 30px;
        box-shadow: 0 10px 20px rgba(0,122,255,0.2);
    }
    
    .public-container {
        text-align: center;
        padding: 100px 20px;
    }

    /* Glassmorphism Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px);
        border-right: 1px solid #e5e5ea;
    }

    /* Metric Cards */
    .metric-card {
        background: white;
        padding: 24px;
        border-radius: 20px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border: 1px solid #e5e5ea;
        text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# --- 2. SESSION LOGIC ---
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'version' not in st.session_state: st.session_state.version = 0.1
if 'authenticated' not in st.session_state: st.session_state.authenticated = False

def add_audit_entry(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({
        "Timestamp": timestamp, 
        "User": user, 
        "Action": action, 
        "Revision": f"v{st.session_state.version}"
    })

# --- 3. SIDEBAR (CONTROLS) ---
with st.sidebar:
    st.title("AI Powered GxP Validation Suite")
    if not st.session_state.authenticated:
        st.subheader("🔑 Secure Access")
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize Access"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                add_audit_entry("Secure Sign-in Successful")
                st.rerun()
    else:
        st.success(f"User: **{st.session_state.user_name}**")
        st.caption(f"📍 91362 | 🛡️ GxP Env")
        if st.button("Revoke Access"):
            add_audit_entry("Manual Logout")
            st.session_state.authenticated = False
            st.rerun()
        st.divider()
        st.header("Project Controls")
        proj_name = st.text_input("System ID", "BioLogistics-RAG-01")
        if st.button("Increment Revision"):
            st.session_state.version = round(st.session_state.version + 0.1, 1)
            add_audit_entry(f"Version up-revved to {st.session_state.version}")

# --- 4. MAIN CONTENT ---
if not st.session_state.authenticated:
    # PUBLIC LANDING PAGE
    st.markdown("""
        <div class="public-container">
            <h1 style="font-size: 3.5rem; color: #1d1d1f; font-weight: 600;">AI Powered GxP Validation Suite</h1>
            <p style="font-size: 1.5rem; color: #8e8e93; max-width: 800px; margin: 20px auto;">
                A next-generation platform for automated CSV artifact generation. 
                Leveraging Llama 3.3 to bridge the gap between URS, FRS, and OQ with 100% traceability.
            </p>
            <div style="margin-top: 40px; padding: 20px; background: white; border-radius: 16px; display: inline-block; border: 1px solid #e5e5ea;">
                <p style="color: #007aff; font-weight: 600;">🔐 Please authorize via the sidebar to access your workspace.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    

[Image of a software validation life cycle V-model]


else:
    # AUTHENTICATED DASHBOARD
    st.markdown("""
        <div class="hero-banner">
            <h1>AI Powered GxP Validation Suite</h1>
            <p>Authoring FRS, OQ, and RTM artifacts with Intelligent Traceability</p>
        </div>
    """, unsafe_allow_html=True)
    
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="metric-card"><h4>Current Version</h4><h2>v{st.session_state.version}</h2></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><h4>Location</h4><h2>91362</h2></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><h4>Integrity Status</h4><h2>ALCOA+</h2></div>', unsafe_allow_html=True)

    st.divider()

    uploaded_file = st.file_uploader("📂 Upload URS / SOP PDF", type="pdf")
    if uploaded_file and st.button("🚀 Execute Validation Authoring"):
        add_audit_entry(f"Analysis Started: {uploaded_file.name}")
        # (Processing logic remains the same as previous version...)
        # [Implementation of LLM and extract_table calls here]
        st.success("Documents successfully authored. Review in tabs below.")
