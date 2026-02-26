import streamlit as st
from langchain_groq import ChatGroq
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

# (CSS remains same as v4.2 for that sleek iOS look)
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
    .metric-card {
        background: white; padding: 20px; border-radius: 20px; border: 1px solid #e5e5ea; text-align: center;
    }
    </style>
""", unsafe_allow_html=True)

# --- 2. SESSION & ROUTING LOGIC ---
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'version' not in st.session_state: st.session_state.version = 0.1
if 'authenticated' not in st.session_state: st.session_state.authenticated = False

def add_audit_entry(action, model_used):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({
        "Timestamp": timestamp, 
        "User": user, 
        "Action": action, 
        "Model": model_used,
        "Revision": f"v{st.session_state.version}"
    })

# --- 3. SIDEBAR: MULTI-LLM SELECTOR ---
with st.sidebar:
    st.title("AI Powered GxP Validation Suite")
    
    if not st.session_state.authenticated:
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                add_audit_entry("Login", "N/A")
                st.rerun()
    else:
        st.success(f"Verified: **{st.session_state.user_name}**")
        
        st.divider()
        st.header("🤖 Engine Selection")
        # The core feature: Pick your "Brain"
        model_provider = st.radio(
            "Select Intelligence Engine:",
            ["Llama 3.3 (Groq)", "GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"],
            help="Switching engines allows for 'Independent Verification' of requirements."
        )
        
        st.divider()
        proj_name = st.text_input("System ID", "BioLogistics-RAG-01")
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()

# --- 4. MAIN CONTENT ---
if not st.session_state.authenticated:
    st.markdown('<div style="text-align:center; padding:100px;"><h1>AI Powered GxP Validation Suite</h1><p>Please authorize via sidebar.</p></div>', unsafe_allow_html=True)
else:
    st.markdown(f'<div class="hero-banner"><h1>{model_provider} Active</h1><p>Authoring artifacts with {model_provider} logic</p></div>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("📂 Upload URS PDF", type="pdf")
    
    if uploaded_file and st.button(f"🚀 Generate via {model_provider}"):
        add_audit_entry(f"Generation Start: {uploaded_file.name}", model_provider)
        
        with st.spinner(f"{model_provider} is thinking..."):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            try:
                # 1. Load PDF
                loader = PyPDFLoader(tmp_path)
                full_text = "\n".join([p.page_content for p in loader.load()])
                
                # 2. Route to correct LLM
                if "Llama" in model_provider:
                    llm = ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)
                elif "GPT-4o" in model_provider:
                    llm = ChatOpenAI(model="gpt-4o", api_key=st.secrets["OPENAI_API_KEY"], temperature=0)
                elif "Claude" in model_provider:
                    llm = ChatAnthropic(model="claude-3-5-sonnet-20240620", api_key=st.secrets["ANTHROPIC_API_KEY"], temperature=0)

                # 3. Prompt
                prompt = f"""Analyze: {full_text[:12000]}
                Generate 3 sections (FRS, OQ, RTM) separated by '---SECTION_SPLIT---'.
                Include FRS_ID and Test_ID mapping. Output as Markdown tables."""
                
                response = llm.invoke(prompt)
                st.session_state.full_analysis = response.content
                add_audit_entry("Generation Complete", model_provider)
                
            except Exception as e:
                st.error(f"Error: {e}. Check if API key for {model_provider} is set in Secrets.")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    # (Display tabs and analytics remain same as previous version)
    if st.session_state.full_analysis:
        st.markdown(st.session_state.full_analysis)
