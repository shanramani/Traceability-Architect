import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG ---
st.set_page_config(page_title="Traceability Architect Pro", layout="wide", page_icon="🧪")

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
    [data-testid="stSidebar"] { background: rgba(249, 249, 252, 0.95); backdrop-filter: blur(15px); }
    </style>
""", unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'model_provider' not in st.session_state: st.session_state.model_provider = "Llama 3.3 (Groq)"

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🧪 Admin Controls")
    if not st.session_state.authenticated:
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                st.rerun()
    else:
        st.success(f"Verified: **{st.session_state.user_name}**")
        st.session_state.model_provider = st.radio(
            "Select Intelligence Engine:",
            ["Llama 3.3 (Groq)", "GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"]
        )
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()

# --- 4. MAIN INTERFACE ---
st.markdown(f'<div class="hero-banner"><h1>Traceability Architect</h1><p>{st.session_state.model_provider} | Site: 91362</p></div>', unsafe_allow_html=True)

if not st.session_state.authenticated:
    st.info("🔐 Please sign in via the sidebar to begin.")
else:
    st.subheader("📂 Step 1: GAMP 5 Document Ingestion")
    uploaded_file = st.file_uploader("Upload SOP or URS PDF", type="pdf")

    if uploaded_file and st.button("🚀 Analyze & Generate GxP Matrix"):
        # --- FIX: DEFINE ACTIVE_KEY GLOBALLY WITHIN THIS BLOCK ---
        active_key = None
        if "Groq" in st.session_state.model_provider: active_key = st.secrets.get("GROQ_API_KEY")
        elif "OpenAI" in st.session_state.model_provider: active_key = st.secrets.get("OPENAI_API_KEY")
        elif "Claude" in st.session_state.model_provider: active_key = st.secrets.get("ANTHROPIC_API_KEY")

        if not active_key:
            st.error(f"Missing API Key for {st.session_state.model_provider} in Streamlit Secrets.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            try:
                loader = PyPDFLoader(tmp_path)
                pages = loader.load()
                full_text = " ".join([p.page_content for p in pages])
                
                model_map = {
                    "Llama 3.3 (Groq)": "groq/llama-3.3-70b-versatile",
                    "GPT-4o (OpenAI)": "openai/gpt-4o",
                    "Claude 3.5 (Anthropic)": "anthropic/claude-3-5-sonnet-20240620"
                }

                with st.spinner("Executing GAMP 5 Regulatory Analysis..."):
                    # Refined Power Prompt for RAG Grounding
                    prompt = (
                        f"Act as a GxP Validation Lead. Analyze: {full_text[:8000]}. "
                        f"Extract 5 requirements. Format exactly: "
                        f"ID | Requirement | Functional_Spec | GAMP_Test_Method | Risk | Reg_Citation. "
                        f"Include specific FDA 21 CFR Part 11 or GAMP 5 citations in 'Reg_Citation'. "
                        f"Separate with newlines."
                    )
                    
                    res = completion(
                        model=model_map[st.session_state.model_provider],
                        messages=[{"role": "user", "content": prompt}],
                        api_key=active_key
                    )
                    
                    lines = res.choices[0].message.content.strip().split('\n')
                    results = []
                    for line in lines:
                        if '|' in line:
                            p = line.split('|')
                            if len(p) >= 6:
                                results.append({
                                    "ID": p[0].strip(), "Requirement": p[1].strip(), 
                                    "Spec": p[2].strip(), "Test": p[3].strip(), 
                                    "Risk": p[4].strip(), "Citation": p[5].strip(), "Verified": False
                                })
                    st.session_state.master_df = pd.DataFrame(results)

            except Exception as e:
                st.error(f"Critical Error: {e}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    # Step 2: Editor & Export
    if st.session_state.master_df is not None:
        st.divider()
        st.subheader("🛠️ Step 2: Human-in-