import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & VERSIONING ---
VERSION = "8.2"
st.set_page_config(page_title=f"Traceability Architect v{VERSION}", layout="wide", page_icon="⚖️")

if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None
if 'user_name' not in st.session_state: st.session_state.user_name = ""

# --- 2. LANDING PAGE ---
def show_landing_page():
    st.title("🚀 Traceability Architect Pro")
    st.markdown(f"**Version {VERSION}** | *Zero-Cost Development Mode*")
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("### ✅ SOP-to-RAG\nGround your work instructions in actual GxP data.")
    with col2:
        st.info("### ⚖️ Part 11 Scanning\nAutomated gap detection for 21 CFR §11.10.")
        
    st.divider()
    st.write("### 🔐 Auditor Access")
    u = st.text_input("User ID")
    p = st.text_input("Access Key", type="password")
    if st.button("Authorize"):
        if u: 
            st.session_state.user_name = u
            st.session_state.authenticated = True
            st.rerun()

# --- 3. MAIN AUDIT DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.title(f"🛡️ v{VERSION}")
        st.success(f"User: **{st.session_state.user_name}**")
        
        # --- NEW: MOCK MODE TOGGLE ---
        st.divider()
        st.write("🛠️ **Developer Tools**")
        mock_mode = st.toggle("Enable Mock Audit (No API Cost)", value=True)
        if mock_mode:
            st.warning("Running in Offline Mode")
        
        if st.button("Log Out"):
            st.session_state.authenticated = False
            st.rerun()

    st.header("📂 21 CFR Part 11 Audit")
    uploaded_file = st.file_uploader("Upload PDF", type="pdf")

    if uploaded_file and st.button("🚀 Run Audit"):
        if mock_mode:
            # Simulated GxP Response (Free)
            st.session_state.master_data = [
                ["URS-001", "System must log all data changes", "FS-101", "Audit Trail captures Timestamp/User", "OQ-201", "Verify log entry on record edit", "11.10(e)", "9.5", "Compliant"],
                ["URS-002", "Electronic Signatures required for approval", "FS-102", "E-Sig prompt on state change", "OQ-202", "Test signature manifestation", "11.50", "9.0", "Compliant"],
                ["URS-003", "Access restricted to authorized personnel", "PART11_MISSING", "GAP_MISSING", "GAP_MISSING", "GAP_MISSING", "11.10(g)", "2.0", "REGULATORY_GAP: No role-based access defined"]
            ]
            st.success("Mock Analysis Complete (No API Credits Used)")
        else:
            # Actual API Logic (Requires Credits)
            try:
                active_key = st.secrets.get("OPENAI_API_KEY")
                # ... [Previous LLM Call Logic] ...
                st.error("API call requires active credits.")
            except Exception as e:
                st.error(f"Error: {e}")

    # Display & Export
    if st.session_state.master_data:
        df = pd.DataFrame(st.session_state.master_data, columns=["URS_ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Part11_Ref", "ALCOA_Score", "Status"])
        st.data_editor(df, use_container_width=True)
        
        if st.button("💾 Export Excel"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Traceability Matrix')
            st.download_button("📥 Download", data=output.getvalue(), file_name=f"Mock_Audit_v{VERSION}.xlsx")

# --- 4. ROUTING ---
if not st.session_state.authenticated:
    show_landing_page()
else:
    show_main_dashboard()