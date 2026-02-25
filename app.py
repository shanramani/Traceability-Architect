import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & UI ---
st.set_page_config(page_title="Traceability Architect v2.7", layout="wide", page_icon="🏗️")

# Custom CSS for GxP feel
st.markdown("""
    <style>
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: #f0f2f6; border-radius: 4px 4px 0 0; }
    .stTabs [aria-selected="true"] { background-color: #007bff; color: white; font-weight: bold; }
    .audit-text { font-family: monospace; font-size: 0.85em; color: #d63384; }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ The Traceability Architect")
st.caption("AI-Powered CSV Document Suite | v2.7 (Audit Trail & Login)")

# Initialize session state for Audit Trail
if 'audit_trail' not in st.session_state:
    st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def add_audit_entry(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({
        "Timestamp": timestamp,
        "User": user,
        "Action": action
    })

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
            # Smart Merge Logic
            first_col = df.columns[0]
            df = df.replace('', pd.NA).ffill().bfill()
            df = df.groupby(first_col).last().reset_index()
            return df
        return None
    except:
        return None

# --- 2. SIDEBAR (AUTHENTICATION & CONTROLS) ---
with st.sidebar:
    st.header("🔐 System Access")
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        user_input = st.text_input("Username")
        pass_input = st.text_input("Password", type="password")
        if st.button("Sign In"):
            if user_input:
                st.session_state.user_name = user_input
                st.session_state.authenticated = True
                add_audit_entry("User Login Successful")
                st.rerun()
    else:
        st.success(f"Logged in: **{st.session_state.user_name}**")
        if st.button("Sign Out"):
            add_audit_entry("User Logout")
            st.session_state.authenticated = False
            st.rerun()

    st.divider()
    st.header("📝 Project Controls")
    proj_name = st.text_input("System Name", "BioLogistics v1.0")
    doc_status = st.selectbox("Document Status", ["Draft", "In-Review", "Approved"])
    
    st.divider()
    st.markdown(f"**Date:** {datetime.date.today().strftime('%d-%b-%Y')}")
    st.markdown(f"**Location:** Thousand Oaks, CA (91362)")

# --- 3. PROCESSING (RESTRICTED TO AUTH USERS) ---
if st.session_state.authenticated:
    uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

    if uploaded_file:
        if st.button("🚀 Process PDF"):
            add_audit_entry(f"PDF Processing Started: {uploaded_file.name}")
            with st.spinner("Analyzing requirements..."):
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name
                try:
                    loader = PyPDFLoader(tmp_path)
                    pages = loader.load()
                    full_text = "\n".join([p.page_content for p in pages])
                    master_prompt = f"Analyze this URS and generate FRS, OQ, and RTM Markdown tables. Separate sections with '---SECTION_SPLIT---'. URS TEXT: {full_text[:12000]}"
                    response = get_llm().invoke(master_prompt)
                    st.session_state.full_analysis = response.content
                    add_audit_entry("Validation Suite Generated via AI")
                except Exception as e:
                    st.error(f"Error: {e}")
                finally:
                    if os.path.exists(tmp_path): os.remove(tmp_path)

    # --- 4. DISPLAY & EXCEL ---
    if st.session_state.full_analysis:
        parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
        tab1, tab2, tab3, tab4 = st.tabs(["📑 FRS", "🧪 OQ", "🔗 RTM", "🕵️ Audit Trail"])
        
        df_frs = extract_table(parts[0])
        df_oq = extract_table(parts[1])
        df_rtm = extract_table(parts[2])

        with tab1: st.markdown(parts[0])
        with tab2: st.markdown(parts[1])
        with tab3: st.markdown(parts[2])
        with tab4:
            st.subheader("System Audit Log")
            st.table(st.session_state.audit_trail)

        st.divider()
        if any([df_frs is not None, df_oq is not None, df_rtm is not None]):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                if df_frs is not None: df_frs.to_excel(writer, index=False, sheet_name='FRS')
                if df_oq is not None: df_oq.to_excel(writer, index=False, sheet_name='OQ')
                if df_rtm is not None: df_rtm.to_excel(writer, index=False, sheet_name='RTM')
                # Export Audit Trail to Excel for Integrity
                pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='AUDIT_LOG')
            
            if st.download_button("📊 Download Excel Workbook", output.getvalue(), f"{proj_name}_Validation.xlsx"):
                add_audit_entry("Excel Workbook Downloaded")
else:
    st.warning("Please sign in from the sidebar to access the Traceability Architect.")
