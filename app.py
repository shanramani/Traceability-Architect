import streamlit as st
from langchain_groq import ChatGroq
import os
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect", layout="wide", page_icon="🏗️")

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered RTM & Validation Artifact Generator | GAMP 5 Framework")

# --- 2. INITIALIZE SESSION STATE ---
if 'rtm_data' not in st.session_state:
    st.session_state.rtm_data = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

# --- 3. SIDEBAR ---
with st.sidebar:
    st.header("🛠️ CSV Parameters")
    project_name = st.text_input("Project Name", "System Alpha Validation")
    id_prefix = st.text_input("Requirement Prefix", "URS-")
    st.divider()
    st.info("This tool transforms URS documents into a full Requirements Traceability Matrix (RTM).")

# --- 4. UPLOAD & EXTRACTION ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    if st.button("🚀 Generate Full Traceability Matrix"):
        with st.spinner("Processing GAMP 5 Logic (URS -> FS -> OQ)..."):
            # Load PDF
            loader = PyPDFLoader(tmp_path)
            pages = loader.load()
            full_text = "\n".join([p.page_content for p in pages])

            llm = get_llm()
            
            # THE "MASTER" PROMPT: Chain of Thought
            master_prompt = f"""
            You are a CSV Lead. Analyze the following URS text.
            1. Extract every testable requirement (using 'shall'/'must').
            2. For each requirement, generate a 'Functional Specification' (How it works).
            3. For each requirement, generate an 'OQ Test Script' step (How to verify it).
            
            Format the output ONLY as a Markdown Table with these columns:
            ID | Requirement | Functional Specification | OQ Test Step

            Use prefix {id_prefix} for IDs.
            
            TEXT:
            {full_text[:12000]}
            """
            
            response = llm.invoke(master_prompt)
            st.session_state.rtm_data = response.content

    os.remove(tmp_path)

# --- 5. DISPLAY & EXPORT ---
if st.session_state.rtm_data:
    st.subheader("📊 Draft Traceability Matrix")
    st.markdown(st.session_state.rtm_data)
    
    # Simple Export Logic (For Marketing/Demo)
    st.divider()
    st.subheader("📥 Export Artifacts")
    st.download_button(
        label="Download RTM as Text",
        data=st.session_state.rtm_data,
        file_name="Traceability_Matrix.txt",
        mime="text/plain"
    )
