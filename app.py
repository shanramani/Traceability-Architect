import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.1", layout="wide", page_icon="🏗️")

# Pharma-grade CSS for professional look
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    .signature-box { border: 1px solid #d1d4d9; padding: 20px; border-radius: 5px; background-color: #ffffff; margin-top: 20px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.1 (Audit-Ready Artifacts)")

# Initialize session state so data persists during tab clicks
if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

# --- 2. SIDEBAR CONTROLS ---
with st.sidebar:
    st.header("📝 Document Controls")
    proj_name = st.text_input("System Name", "BioLogistics v1.0")
    doc_id = st.text_input("Document ID", "FRS-2026-001")
    author = st.text_input("Author (CSV Lead)", "Shan")
    review_date = datetime.date.today().strftime("%d-%b-%Y")
    
    st.divider()
    status = st.selectbox("Document Status", ["Draft", "In Review", "Approved"])
    st.info(f"Generated on: {review_date}")
    st.divider()
    st.write("📍 Location: Thousand Oaks / 91362 Hub")

# --- 3. UPLOAD & PROCESSING ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if uploaded_file:
    # Handle File Processing
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    if st.button("🚀 Generate Validation Suite"):
        with st.spinner("Executing GAMP 5 Logic (URS -> FRS -> OQ)..."):
            try:
                loader = PyPDFLoader(tmp_path)
                pages = loader.load()
                full_text = "\n".join([p.page_content for p in pages])
                llm = get_llm()
                
                master_prompt = f"""
                You are a Senior CSV Consultant specializing in GAMP 5. 
                Analyze the following URS text and generate three distinct sections for the project: {proj_name}.
                
                SECTION 1: FUNCTIONAL REQUIREMENTS SPECIFICATION (FRS)
                Provide a detailed technical description of HOW the system achieves each requirement.
                
                SECTION 2: OPERATIONAL QUALIFICATION (OQ) PROTOCOL
                Provide a Step-by-Step Test Instruction for each requirement, including 'Expected Result' and a 'Pass/Fail' column.
                
                SECTION 3: TRACEABILITY MATRIX (RTM)
                Create a markdown table mapping: URS ID | URS Description | FRS Reference | Test Case ID.

                URS TEXT: 
                {full_text[:15000]}
                
                Separate each section clearly using the exact string: '---SECTION_SPLIT---'
                """
                
                response = llm.invoke(master_prompt)
                st.session_state.full_analysis = response.content
                
            except Exception as e:
                st.error(f"Error during processing: {e}")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

# --- 4. THE DOCUMENT SUITE DISPLAY ---
if st.session_state.full_analysis:
    # Split the AI output into the three document sections
    parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
    
    tab1, tab2, tab3 = st.tabs(["📑 FRS Document", "🧪 Test Protocol", "🔗 Trace Matrix"])
    
    with tab1:
        st.subheader("Functional Requirements Specification")
        
        # Professional Signature Block
        st.markdown(f"""
        <div class="signature-box">
            <table style="width:100%; border:none;">
                <tr><td><b>Document ID:</b> {doc_id}</td><td><b>Status:</b> {status}</td></tr>
                <tr><td><b>Author:</b> {author}</td><td><b>Date:</b> {review_date}</td></tr>
            </table>
            <hr>
            <p style="font-size: 0.8em; color: gray;">
                <b>QA Approval Signature:</b> __________________________  &nbsp;&nbsp;&nbsp; <b>Date:</b> __________
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(parts[0] if len(parts) > 0 else "Analysis content missing.")
        
    with tab2:
        st.header("Operational Qualification (OQ) Protocol")
        st.markdown(parts[1] if len(parts) > 1 else "Analysis content missing.")
        
    with tab3:
        st.header("Requirements Traceability Matrix (RTM)")
        st.markdown(parts[2] if len(parts) > 2 else "Analysis content missing.")

    # Export Logic
    st.divider()
    st.download_button(
        label="📥 Download Full Validation Suite Draft",
        data=st.session_state.full_analysis,
        file_name=f"{proj_name}_Validation_Draft.txt",
        mime="text/plain"
    )
