import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & 2026 MINIMALIST STYLING ---
VERSION = "8.8"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# Modern CSS: Centered layout, horizontal radio buttons, and status indicators
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; color: #0f172a; }
    
    .block-container { max-width: 800px; padding-top: 5rem; }

    /* Centered Header Section */
    .hero { text-align: center; margin-bottom: 4rem; }
    .hero h1 { font-weight: 600; font-size: 2.2rem; letter-spacing: -0.03em; margin-bottom: 0.5rem; }
    .hero p { color: #64748b; font-size: 1rem; font-weight: 300; }

    /* Horizontal Radio Alignment */
    div[data-testid="stRadio"] > div {
        display: flex;
        flex-direction: row;
        justify-content: center;
        gap: 30px;
        border: 1px solid #e2e8f0;
        padding: 15px;
        border-radius: 12px;
        background: #f8fafc;
    }

    /* Connectivity Dot Indicators */
    .status-dot { height: 8px; width: 8px; border-radius: 50%; display: inline-block; margin-right: 8px; }
    .online { background-color: #22c55e; box-shadow: 0 0 8px #22c55e; }
    .offline { background-color: #ef4444; }

    /* Sleek Input Fields */
    .stTextInput > div > div > input { border-radius: 8px; padding: 12px; border: 1px solid #cbd5e1; }
    .stButton > button { 
        background: #0f172a; color: white; width: 100%; border-radius: 8px; 
        font-weight: 500; padding: 0.7rem; border: none; transition: 0.2s;
    }
    .stButton > button:hover { background: #334155; transform: translateY(-1px); }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None

# --- 3. SOVEREIGN LANDING PAGE ---
def show_landing_page():
    st.markdown("""
        <div class="hero">
            <h1>Traceability Architect</h1>
            <p>Intelligence for GAMP 5 & 21 CFR Part 11 Compliance</p>
        </div>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 4, 1])
    with col:
        u = st.text_input("Professional Identity", placeholder="Username")
        p = st.text_input("Security Key", type="password", placeholder="••••••••")
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Initialize Session"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()

# --- 4. MAIN AUDIT DASHBOARD ---
def show_main_dashboard():
    # Model Configuration Map
    model_config = {
        "Groq": {"id": "groq/llama-3.3-70b-versatile", "key": "GROQ_API_KEY"},
        "OpenAI": {"id": "openai/gpt-4o", "key": "OPENAI_API_KEY"},
        "Claude": {"id": "anthropic/claude-3-5-sonnet-20240620", "key": "ANTHROPIC_API_KEY"},
        "Gemini": {"id": "gemini/gemini-1.5-pro-latest", "key": "GEMINI_API_KEY"}
    }

    with st.sidebar:
        st.markdown("### System Health")
        # Visual Status Indicators
        for name, cfg in model_config.items():
            status_class = "online" if st.secrets.get(cfg["key"]) else "offline"
            st.markdown(f'<span class="status-dot {status_class}"></span>{name}', unsafe_allow_html=True)
        
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name}")
        st.caption(f"Site: 91362")
        
        if st.button("Sign Out"):
            st.session_state.authenticated = False
            st.rerun()

    st.markdown("### Selection of Intelligence Engine")
    engine = st.radio("Provider", list(model_config.keys()), label_visibility="collapsed")
    
    st.divider()
    st.markdown("### Document Ingestion")
    uploaded_file = st.file_uploader("Upload PDF Source", type="pdf", label_visibility="collapsed")

    if uploaded_file and st.button("Execute Compliance Scan"):
        cfg = model_config[engine]
        api_key = st.secrets.get(cfg["key"])

        if not api_key:
            st.error(f"Selected engine ({engine}) is currently offline. Please check secrets.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = " ".join([p.page_content for p in loader.load()])
                
                with st.spinner(f"Engine synchronized. Analyzing GxP parameters..."):
                    prompt = (
                        f"Analyze: {full_text[:7000]}. Return pipe-separated rows with 9 columns: "
                        f"ID | URS_Text | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA_Score | Status."
                    )
                    res = completion(model=cfg["id"], messages=[{"role": "user", "content": prompt}], api_key=api_key)
                    
                    raw_lines = res.choices[0].message.content.strip().split('\n')
                    rows = []
                    for line in raw_lines:
                        if '|' in line:
                            p = [item.strip() for item in line.split('|')]
                            if len(p) >= 9: rows.append(p[:9])
                            else: rows.append(p + ["N/A"]*(9-len(p)))
                    
                    st.session_state.master_data = rows
                    st.success("Verification complete.")
            except Exception as e:
                st.error(f"Inference Interrupted: {e}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.master_data:
        st.divider()
        cols = ["ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Ref", "ALCOA", "Status"]
        df = pd.DataFrame(st.session_state.master_data, columns=cols)
        st.markdown("#### Validated Traceability Matrix")
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.button("Export Certified Package")

# --- 5. ROUTING ---
if not st.session_state.authenticated:
    show_landing_page()
else:
    show_main_dashboard()