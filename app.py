import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.4", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    .signature-box { border: 1px solid #d1d4d9; padding: 20px; border-radius: 5px; background-color: #ffffff; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.4 (PDF Upload & Demo Mode)")
if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def get_llm():
    return ChatGroq(
        model_name="llama-3.3-70b-versatile", 
        groq_api_key=st.secrets["GROQ_API_KEY"],
        temperature=0
    )

def extract_table(text):
    try:
        lines = [line for line in text.split('\n') if '|' in line]
        if len(lines) > 2:
            raw_data = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(raw_data), sep='|', skipinitialspace=True).dropna(axis=1, how='all')
            df.columns = [c.strip() for c in df.columns]
            df = df[~df.iloc[:,0].str.contains('---', na=False)]
            return df
        return None
    except:
        return None
        with st.sidebar:
        st.header("📝 Project Controls")
        proj_name = st.text_input("System Name", "BioLogistics v1.0")
        author = st.text_input("CSV Lead", "Shan")
        st.divider()
        if st.button("Load Sample Data"):
            st.session_state.full_analysis = """SECTION 1: FRS\n| ReqID | Functionality | Design Note |\n|---|---|---|\n| FRS-01| Login | LDAP |\n---SECTION_SPLIT---SECTION 2: OQ\n| TestID | Step | Result |\n|---|---|---|\n| OQ-01 | Login | Success |\n---SECTION_SPLIT---SECTION 3: RTM\n| URS | FRS | OQ |\n|---|---|---|"""
            st.success("Sample Loaded!")

uploaded_file = st.file_uploader("OR Upload URS PDF", type="pdf")

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name
    if st.button("🚀 Process PDF"):
        with st.spinner("Analyzing..."):
            loader = PyPDFLoader(tmp_path)
            pages = loader.load()
            full_text = "\n".join([p.page_content for p in pages])
            master_prompt = f"Generate FRS, OQ, RTM markdown tables for: {full_text[:10000]}. Separate with '---SECTION_SPLIT---'."
            response = get_llm().invoke(master_prompt)
            st.session_state.full_analysis = response.content
    os.remove(tmp_path)
if st.session_state.full_analysis:
    parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
    tab1, tab2, tab3 = st.tabs(["📑 FRS", "🧪 OQ", "🔗 RTM"])
    df_frs = extract_table(parts[0])
    df_oq = extract_table(parts[1])
    df_rtm = extract_table(parts[2])
    with tab1: st.markdown(parts[0])
    with tab2: st.markdown(parts[1])
    with tab3: st.markdown(parts[2])
    
    if any([df_frs is not None, df_oq is not None, df_rtm is not None]):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if df_frs is not None: df_frs.to_excel(writer, index=False, sheet_name='FRS')
            if df_oq is not None: df_oq.to_excel(writer, index=False, sheet_name='OQ')
            if df_rtm is not None: df_rtm.to_excel(writer, index=False, sheet_name='RTM')
        st.download_button("📊 Download Excel Workbook", output.getvalue(), "Validation.xlsx")
        
