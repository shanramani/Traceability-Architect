import streamlit as st
from langchain_groq import ChatGroq
import os
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.0", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    .reportview-container { background: #f5f7f9; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: #f0f2f6; border-radius: 4px 4px 0 0; gap: 1px; padding-top: 10px; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | GAMP 5 & 21 CFR Part 11 Compliant")

if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

# --- 2. SIDEBAR ---
with st.sidebar:
    st.header("🛠️ Project Metadata")
    proj_name = st.text_input("System Name", "Enterprise LIMS")
    author = st.text_input("CSV Lead", "Shan")
    st.divider()
    st.info("Upload your URS to generate the FRS, Test Protocol, and Trace Matrix.")

# --- 3. UPLOAD & PROCESSING ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    if st.button("🚀 Generate Validation Suite"):
        with st.spinner("Authoring FRS, Protocol, and RTM..."):
            loader = PyPDFLoader(tmp_path)
            pages = loader.load()
            full_text = "\n".join([p.page_content for p in pages])

            llm = get_llm()
            
            # The "Total Documentation" Prompt
            master_prompt = f"""
            You are a Senior CSV Consultant. Based on the URS provided, generate three distinct sections.
            
            SECTION 1: FUNCTIONAL REQUIREMENTS SPECIFICATION (FRS)
            For each requirement, provide a technical description of HOW the system achieves it.
            
            SECTION 2: OPERATIONAL QUALIFICATION (OQ) PROTOCOL
            For each requirement, provide a Step-by-Step Test Instruction and an Expected Result.
            
            SECTION 3: TRACEABILITY MATRIX (RTM)
            A markdown table mapping URS ID | URS Description | FRS Reference | Test Case ID.

            URS TEXT:
            {full_text[:12000]}
            
            Use professional, GxP-compliant language. Separate sections with '---SECTION_SPLIT---'.
            """
            
            response = llm.invoke(master_prompt)
            st.session_state.full_analysis = response.content

    os.remove(tmp_path)

# --- 4. THE DOCUMENT SUITE (TABS) ---
if st.session_state.full_analysis:
    # Split the AI response into the three buckets
    parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
    
    tab1, tab2, tab3 = st.tabs(["📑 Functional Specs (FRS)", "🧪 Test Protocol (OQ)", "🔗 Trace Matrix (RTM)"])
    
    with tab1:
        st.header(f"Functional Specification: {proj_name}")
        st.markdown(parts[0] if len(parts) > 0 else "Analysis failed.")
        
    with tab2:
        st.header("Operational Qualification Protocol")
        st.markdown(parts[1] if len(parts) > 1 else "Analysis failed.")
        
    with tab3:
        st.header("Requirements Traceability Matrix")
        st.markdown(parts[2] if len(parts) > 2 else "Analysis failed.")

    # Export Button
    st.divider()
    st.download_button("📥 Download Full Validation Draft", st.session_state.full_analysis, f"{proj_name}_Validation_Draft.txt")
