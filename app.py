import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.6", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    .metadata-text { font-size: 0.8em; color: #6c757d; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.6")

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
            # Smart Merge Logic for RTM
            first_col = df.columns[0]
            df = df.replace('', pd.NA).ffill().bfill()
            df = df.groupby(first_col).last().reset_index()
            return df
        return None
    except:
        return None

# --- 2. SIDEBAR (RESTORED METADATA) ---
with st.sidebar:
    st.header("📝 Project Controls")
    proj_name = st.text_input("System Name", "BioLogistics v1.0")
    author = st.text_input("CSV Lead", "Shan")
    doc_status = st.selectbox("Document Status", ["Draft", "In-Review", "Approved"])
    
    st.divider()
    
    # Metadata Display
    curr_date = datetime.date.today().strftime("%d-%b-%Y")
    st.markdown(f"**Date:** {curr_date}")
    st.markdown(f"**Location:** Thousand Oaks, CA (91362)")
    st.markdown(f"**Environment:** GxP / GAMP 5")
    
    st.divider()
    
    if st.button("Load Sample Data"):
        sample_text = "SECTION 1: FRS\n| ReqID | Functionality | Design Note |\n|---|---|---|\n| FRS-01 | Auth | LDAP |\n\n---SECTION_SPLIT---\n\nSECTION 2: OQ\n| TestID | Instruction | Result |\n|---|---|---|\n| OQ-01 | Login | Success |\n\n---SECTION_SPLIT---\n\nSECTION 3: RTM\n| URS_ID | Description | FRS_Link | Test_Link |\n|---|---|---|---|\n| URS-01 | Login | FRS-01 | OQ-01 |"
        st.session_state.full_analysis = sample_text
        st.success("Sample Data Loaded!")

# --- 3. PROCESSING ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if uploaded_file:
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    if st.button("🚀 Process PDF"):
        with st.spinner("Analyzing requirements..."):
            try:
                loader = PyPDFLoader(tmp_path)
                pages = loader.load()
                full_text = "\n".join([p.page_content for p in pages])
                
                master_prompt = f"Analyze this URS and generate FRS, OQ, and RTM Markdown tables. Separate sections with '---SECTION_SPLIT---'. Ensure RTM rows are horizontally aligned. URS TEXT: {full_text[:12000]}"
                response = get_llm().invoke(master_prompt)
                st.session_state.full_analysis = response.content
            except Exception as e:
                st.error(f"Error: {e}")
            finally:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)

# --- 4. DISPLAY & EXCEL ---
if st.session_state.full_analysis:
    parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
    tab1, tab2, tab3 = st.tabs(["📑 FRS Document", "🧪 Test Protocol", "🔗 Trace Matrix"])
    
    df_frs = extract_table(parts[0]) if len(parts) > 0 else None
    df_oq = extract_table(parts[1]) if len(parts) > 1 else None
    df_rtm = extract_table(parts[2]) if len(parts) > 2 else None

    with tab1: st.markdown(parts[0])
    with tab2: st.markdown(parts[1])
    with tab3: st.markdown(parts[2])

    st.divider()
    if any([df_frs is not None, df_oq is not None, df_rtm is not None]):
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if df_frs is not None: df_frs.to_excel(writer, index=False, sheet_name='FRS')
            if df_oq is not None: df_oq.to_excel(writer, index=False, sheet_name='OQ')
            if df_rtm is not None: df_rtm.to_excel(writer, index=False, sheet_name='RTM')
        
        st.download_button(
            label="📊 Download Excel Workbook",
            data=output.getvalue(),
            file_name=f"{proj_name}_Validation.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
