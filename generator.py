import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import plotly.express as px

# --- 1. UI CONFIG ---
VERSION = "10.0"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# (Styling remains consistent with v9.9 for brand stability)
st.markdown("""<style>...</style>""", unsafe_allow_html=True) 

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []

def log_event(action):
    st.session_state.audit_trail.append({"Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "User": st.session_state.user_name, "Action": action})

# --- 3. MAIN DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.title(f"Sovereign v{VERSION}")
        engine = st.radio("Intelligence Provider", ["Gemini", "Groq"])
        st.divider()
        st.markdown("### 📂 System Context")
        # NEW: Context Uploader for System Manuals (SAP, LIMS, etc.)
        system_guide = st.file_uploader("Upload System Guide (e.g. SAP Manual)", type="pdf")
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name} | Site: 91362")
        if st.button("Logout"): st.session_state.authenticated = False; st.rerun()

    st.header("Context-Aware System Validation")
    sop_file = st.file_uploader("Upload Business SOP (The 'What')", type="pdf")

    if sop_file and st.button("🚀 Generate System-Specific Traceability"):
        api_key = st.secrets.get("GEMINI_API_KEY") if engine == "Gemini" else st.secrets.get("GROQ_API_KEY")
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp_sop:
            tmp_sop.write(sop_file.getvalue()); sop_path = tmp_sop.name
        
        # Load System Context if available
        system_text = "No specific system manual provided. Use generic GxP best practices."
        if system_guide:
            with tempfile.NamedTemporaryFile(delete=False) as tmp_sys:
                tmp_sys.write(system_guide.getvalue()); sys_path = tmp_sys.name
            system_text = " ".join([p.page_content for p in PyPDFLoader(sys_path).load()])[:10000]

        try:
            sop_text = " ".join([p.page_content for p in PyPDFLoader(sop_path).load()])[:8000]
            
            with st.spinner(f"Mapping SOP to System Context..."):
                prompt = (
                    f"CONTEXT A: Business SOP (Requirements): {sop_text}\n"
                    f"CONTEXT B: System Guide (Technical Capabilities): {system_text}\n\n"
                    "TASK: Generate a Traceability Matrix. The FRS (Functional Req) MUST be specific to the system in Context B. "
                    "Use technical terms from Context B (e.g., specific SAP T-Codes, LIMS Menu paths). "
                    "Return pipe-separated: URS_ID | URS_Desc | FS_ID | FS_Detail (System Specific) | OQ_ID | OQ_Protocol | Risk | Ref | Justification"
                )
                
                model_id = "gemini/gemini-1.5-pro" if engine == "Gemini" else "groq/llama-3.3-70b-versatile"
                res = completion(model=model_id, messages=[{"role":"user","content":prompt}], api_key=api_key)
                
                data = [l.split('|') for l in res.choices[0].message.content.strip().split('\n') if '|' in l]
                st.session_state.master_df = pd.DataFrame(data, columns=["URS_ID", "URS_Description", "FS_ID", "FS_Detail", "OQ_ID", "OQ_Protocol", "Risk", "Ref", "Justification"])
                log_event(f"Context-Aware Audit Complete via {engine}")
        except Exception as e: st.error(f"Error: {e}")

    # (Tabs 1-6 and Export logic remain identical to v9.9)
    if st.session_state.master_df is not None:
        # Display Tabs...
        pass

# (Auth logic remains same)