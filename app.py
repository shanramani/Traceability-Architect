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
except ImportError:
    GROQ_AVAILABLE = False

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

try:
    from langchain_anthropic import ChatAnthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False

from langchain_community.document_loaders import PyPDFLoader

# --- 2. UI CONFIG ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px; border-radius: 24px; color: white;
        margin-bottom: 30px; box-shadow: 0 10px 20px rgba(0,122,255,0.2);
        text-align: center;
    }
    .hero-banner h1 { font-size: 2.8rem; margin-bottom: 5px; font-weight: 600; }
    .hero-banner p { font-size: 1.2rem; opacity: 0.9; font-weight: 300; }
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px); border-right: 1px solid #e5e5ea;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'model_provider' not in st.session_state: st.session_state.model_provider = "Llama 3.3 (Groq)"
if 'user_name' not in st.session_state: st.session_state.user_name = ""

# --- 4. SIDEBAR ---
with st.sidebar:
    st.title("Admin Controls")
    if st.session_state.authenticated:
        st.success(f"Verified: **{st.session_state.user_name}**")
        
        available_engines = []
        if GROQ_AVAILABLE: available_engines.append("Llama 3.3 (Groq)")
        if OPENAI_AVAILABLE: available_engines.append("GPT-4o (OpenAI)")
        if ANTHROPIC_AVAILABLE: available_engines.append("Claude 3.5 (Anthropic)")
        
        if available_engines:
            st.session_state.model_provider = st.radio("Active Intelligence Engine:", available_engines)
        
        st.divider()
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()
    else:
        st.subheader("🔑 Secure Access")
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.warning("Please enter a User ID.")

# --- 5. SHARED HEADER ---
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
            <h2 style="color: #1d1d1f;">Secure GxP Cloud Environment</h2>
            <p style="color: #8e8e93; font-size: 1.2rem;">Automated Artifact Generation for Bio-Pharmaceutical Compliance.</p>
        </div>
    """, unsafe_allow_html=True)
else:
    uploaded_file = st.file_uploader("📂 Upload URS / SOP PDF", type="pdf")
    
    if uploaded_file and st.button(f"🚀 Execute Validation via {st.session_state.model_provider}"):
        with st.spinner(f"AI Engine ({st.session_state.model_provider}) is processing..."):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = "\n".join([p.page_content for p in loader.load()])
                
                # --- DYNAMIC ROUTING ---
                if "Llama" in st.session_state.model_provider:
                    llm = ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)
                elif "GPT-4o" in st.session_state.model_provider:
                    llm = ChatOpenAI(model="gpt-4o", api_key=st.secrets["OPENAI_API_KEY"], temperature=0)
                elif "Claude" in st.session_state.model_provider:
                    llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", api_key=st.secrets["ANTHROPIC_API_KEY"], temperature=0)
                
                prompt = f"Analyze: {full_text[:12000]}. Generate FRS, OQ, and RTM as Markdown tables."
                st.session_state.full_analysis = llm.invoke(prompt).content
            except Exception as e:
                st.error(f"Engine Configuration Error: {e}. Check your Streamlit Secrets.")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.full_analysis:
        st.divider()
        st.subheader(f"Validation Results: Generated by {st.session_state.model_provider}")
        st.markdown(st.session_state.full_analysis)
