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

# --- 2. CONFIG & UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

# (CSS Logic remains the same for iOS look)
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px; border-radius: 24px; color: white; margin-bottom: 30px;
    }
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95); backdrop-filter: blur(15px); border-right: 1px solid #e5e5ea;
    }
    </style>
""", unsafe_allow_html=True)

# --- 3. SESSION LOGIC ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None

# --- 4. SIDEBAR ENGINE ROUTER ---
with st.sidebar:
    st.title("AI Powered GxP Validation Suite")
    
    if st.session_state.authenticated:
        st.header("🤖 Engine Selection")
        
        # Build list of available engines based on installed libraries
        available_engines = []
        if GROQ_AVAILABLE: available_engines.append("Llama 3.3 (Groq)")
        if OPENAI_AVAILABLE: available_engines.append("GPT-4o (OpenAI)")
        if ANTHROPIC_AVAILABLE: available_engines.append("Claude 3.5 (Anthropic)")
        
        model_provider = st.radio("Select Intelligence Engine:", available_engines)
        
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()
    else:
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            st.session_state.user_name, st.session_state.authenticated = u, True
            st.rerun()

# --- 5. MAIN CONTENT ---
if not st.session_state.authenticated:
    st.markdown('<div style="text-align:center; padding:100px;"><h1>AI Powered GxP Validation Suite</h1><p>Please authorize via sidebar.</p></div>', unsafe_allow_html=True)
else:
    st.markdown(f'<div class="hero-banner"><h1>{model_provider} Active</h1><p>Site: 91362 | Version: v{st.session_state.get("version", 0.1)}</p></div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("📂 Upload URS PDF", type="pdf")
    
    if uploaded_file and st.button(f"🚀 Generate via {model_provider}"):
        with st.spinner(f"Processing with {model_provider}..."):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = "\n".join([p.page_content for p in loader.load()])
                
                # Logic Switcher
                if "Llama" in model_provider:
                    llm = ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)
                elif "GPT-4o" in model_provider:
                    llm = ChatOpenAI(model="gpt-4o", api_key=st.secrets["OPENAI_API_KEY"], temperature=0)
                elif "Claude" in model_provider:
                    llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", api_key=st.secrets["ANTHROPIC_API_KEY"], temperature=0)
                
                prompt = f"Analyze: {full_text[:10000]}. Generate FRS, OQ, and RTM as Markdown tables."
                st.session_state.full_analysis = llm.invoke(prompt).content
            except Exception as e:
                st.error(f"Configuration Error: {e}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.full_analysis:
        st.markdown(st.session_state.full_analysis)
