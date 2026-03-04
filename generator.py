import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & 2026 MODERN STYLING ---
VERSION = "8.5"
st.set_page_config(page_title=f"Traceability Architect v{VERSION}", layout="wide", page_icon="⚖️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; color: #1f2937; }
    
    /* Center and constrain main container */
    .block-container { max-width: 900px; padding-top: 3rem; }

    /* Centered Header Block */
    .header-container {
        text-align: center;
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        padding: 2rem;
        border-radius: 20px;
        border: 1px solid #e2e8f0;
        margin-bottom: 2.5rem;
    }

    /* Professional Input Constraints */
    .stTextInput > div > div > input, .stSelectbox > div > div > div {
        max-width: 450px;
        margin: 0 auto;
        border-radius: 10px;
    }
    
    /* Center alignment for all centered-classed divs */
    .centered-content { display: flex; justify-content: center; flex-direction: column; align-items: center; }

    /* Modern Radio Group Styling */
    div[data-testid="stRadio"] > div {
        display: flex;
        flex-direction: row;
        justify-content: center;
        gap: 20px;
    }

    /* Premium Button */
    .stButton > button {
        background: #0f172a;
        color: white;
        border-radius: 8px;
        padding: 0.6rem 2.5rem;
        font-weight: 600;
        width: auto;
        margin: 0 auto;
        display: block;
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None
if 'engine' not in st.session_state: st.session_state.engine = "Groq (Llama 3.3)"

# --- 3. MODERN LANDING PAGE ---
def show_landing_page():
    st.markdown('<div class="header-container"><h1>🚀 Traceability Architect Pro</h1><p>GAMP 5 & 21 CFR Part 11 AI Compliance Suite</p></div>', unsafe_allow_html=True)
    
    _, col, _ = st.columns([0.1, 0.8, 0.1])
    with col:
        st.markdown("### 🔐 Auditor Authorization")
        u = st.text_input("User ID", placeholder="Enter username")
        p = st.text_input("Access Key", type="password", placeholder="Enter password")
        
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Authorize System"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()

# --- 4. AUDIT DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.title(f"🛡️ v{VERSION}")
        st.write(f"Auditor: **{st.session_state.user_name}**")
        st.write(f"📍 Site: **91362**") # [cite: 2025-12-28]
        st.divider()
        
        # RESTORED RADIO BUTTONS
        st.subheader("Intelligence Engine")
        st.session_state.engine = st.radio(
            "Select Backend Provider:",
            ["Groq (Llama 3.3)", "OpenAI (GPT-4o)", "Claude 3.5", "Gemini 1.5 Pro"]
        )
        
        st.divider()
        if st.button("End Session"):
            st.session_state.authenticated = False
            st.rerun()

    st.markdown("### 📂 Execute Compliance Audit")
    uploaded_file = st.file_uploader("Upload SOP or URS (PDF)", type="pdf")

    if uploaded_file and st.button("🚀 Run AI Analysis"):
        # Map Selection to Backend Code
        model_map = {
            "Groq (Llama 3.3)": ("groq/llama-3.3-70b-versatile", "GROQ_API_KEY"),
            "OpenAI (GPT-4o)": ("openai/gpt-4o", "OPENAI_API_KEY"),
            "Claude 3.5": ("anthropic/claude-3-5-sonnet-20240620", "ANTHROPIC_API_KEY"),
            "Gemini 1.5 Pro": ("gemini/gemini-1.5-pro", "GEMINI_API_KEY")
        }
        
        model_name, key_name = model_map[st.session_state.engine]
        api_key = st.secrets.get(key_name)
        
        if not api_key:
            st.error(f"Missing API Key for {st.session_state.engine} in Secrets.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = " ".join([p.page_content for p in loader.load()])
                
                with st.spinner(f"Auditing with {st.session_state.engine}..."):
                    prompt = (
                        f"Act as a GxP Lead. Analyze: {full_text[:7000]}. "
                        f"Extract 5 URS items. Map to FS and OQ. "
                        f"Flag gaps in §11.10(e) and §11.10(g). "
                        f"Return pipe-separated: URS_ID | URS_Text | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA_Score | Status."
                    )
                    
                    res = completion(model=model_name, messages=[{"role": "user", "content": prompt}], api_key=api_key)
                    raw_rows = [ [i.strip() for i in l.split('|')] for l in res.choices[0].message.content.strip().split('\n') if '|' in l ]
                    st.session_state.master_data = raw_rows
                    st.success("Audit Cycle Complete.")
            except Exception as e:
                st.error(f"Backend Error: {e}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.master_data:
        st.divider()
        df = pd.DataFrame(st.session_state.master_data, columns=["ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Ref", "ALCOA", "Status"])
        st.dataframe(df, use_container_width=True)
        
        # GAMP 5 Signature Block
        st.download_button("📥 Export Signed Workbook", "Dummy Data", file_name="GxP_Audit.xlsx")

# --- 5. ROUTING ---
if not st.session_state.authenticated:
    show_landing_page()
else:
    show_main_dashboard()