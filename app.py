import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & iOS SLEEK UI ---
st.set_page_config(page_title="Traceability Architect", layout="wide", page_icon="🏗️")

# Custom CSS for iOS/Sleek look
st.markdown("""
    <style>
    /* Global Styles */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    
    html, body, [class*="css"]  {
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }

    /* Modern Scrollbar */
    ::-webkit-scrollbar { width: 8px; height: 8px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { 
        background: rgba(0, 0, 0, 0.1); 
        border-radius: 10px; 
    }
    ::-webkit-scrollbar-thumb:hover { background: rgba(0, 0, 0, 0.2); }

    /* Glassmorphism Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(255, 255, 255, 0.8);
        backdrop-filter: blur(10px);
        border-right: 1px solid rgba(255, 255, 255, 0.3);
    }

    /* Sleek Cards & Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background-color: #f2f2f7;
        padding: 5px;
        border-radius: 12px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 40px;
        border-radius: 8px;
        background-color: transparent;
        transition: all 0.3s ease;
        border: none;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff !important;
        box-shadow: 0px 3px 8px rgba(0,0,0,0.12);
        color: #007aff !important;
    }

    /* Buttons */
    div.stButton > button {
        border-radius: 12px;
        background-color: #007aff;
        color: white;
        border: none;
        padding: 0.5rem 1rem;
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        background-color: #0056b3;
        transform: scale(1.02);
    }

    /* Data Integrity Logs Styling */
    .audit-box {
        background-color: #ffffff;
        border-radius: 12px;
        padding: 15px;
        border: 1px solid #e5e5ea;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    </style>
""", unsafe_allow_html=True)

st.title("🏗️ Traceability Architect")
st.markdown("<p style='color: #8e8e93;'>AI-Powered CSV Document Suite | 21 CFR Part 11 MVP</p>", unsafe_allow_html=True)

# Initialize session state for Audit Trail
if 'audit_trail' not in st.session_state:
    st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state:
    st.session_state.full_analysis = None

def add_audit_entry(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({"Timestamp": timestamp, "User": user, "Action": action})

def get_llm():
    return ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)

def extract_table(text):
    try:
        lines = [line for line in text.split('\n') if '|' in line]
        if len(lines) > 2:
            raw_data = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(raw_data), sep='|', skipinitialspace=True).dropna(axis=1, how='all')
            df.columns = [c.strip() for c in df.columns]
            df = df[~df.iloc[:,0].str.contains('---', na=False)]
            first_col = df.columns[0]
            df = df.replace('', pd.NA).ffill().bfill()
            df = df.groupby(first_col).last().reset_index()
            return df
        return None
    except: return None

# --- 2. SIDEBAR (AUTHENTICATION) ---
with st.sidebar:
    st.header("🔐 Access Control")
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        user_input = st.text_input("Username")
        pass_input = st.text_input("Password", type="password")
        if st.button("Sign In"):
            if user_input:
                st.session_state.user_name = user_input
                st.session_state.authenticated = True
                add_audit_entry("Login Success")
                st.rerun()
    else:
        st.markdown(f"Welcome, **{st.session_state.user_name}**")
        st.caption(f"📍 Thousand Oaks, CA (91362)")
        if st.button("Sign Out"):
            add_audit_entry("Logout")
            st.session_state.authenticated = False
            st.rerun()

    st.divider()
    st.header("📝 Project")
    proj_name = st.text_input("System", "BioLogistics v1.0")
    doc_status = st.selectbox("Status", ["Draft", "In-Review", "Approved"])

# --- 3. PROCESSING ---
if st.session_state.authenticated:
    uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

    if uploaded_file:
        if st.button("🚀 Process Validation Suite"):
            add_audit_entry(f"Processed: {uploaded_file.name}")
            with st.spinner("Analyzing requirements..."):
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name
                try:
                    loader = PyPDFLoader(tmp_path)
                    pages = loader.load()
                    full_text = "\n".join([p.page_content for p in pages])
                    prompt = f"Extract FRS, OQ, RTM tables from: {full_text[:12000]}. Separate sections with '---SECTION_SPLIT---'."
                    response = get_llm().invoke(prompt)
                    st.session_state.full_analysis = response.content
                finally:
                    if os.path.exists(tmp_path): os.remove(tmp_path)

    # --- 4. DISPLAY ---
    if st.session_state.full_analysis:
        parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
        tab1, tab2, tab3, tab4 = st.tabs(["📑 FRS", "🧪 OQ", "🔗 RTM", "🕵️ Audit Log"])
        
        df_frs, df_oq, df_rtm = extract_table(parts[0]), extract_table(parts[1]), extract_table(parts[2])

        with tab1: st.markdown(parts[0])
        with tab2: st.markdown(parts[1])
        with tab3: st.markdown(parts[2])
        with tab4:
            st.markdown("### System Audit Trail")
            st.dataframe(pd.DataFrame(st.session_state.audit_trail), use_container_width=True)

        st.divider()
        if any([df_frs is not None, df_oq is not None, df_rtm is not None]):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                if df_frs is not None: df_frs.to_excel(writer, index=False, sheet_name='FRS')
                if df_oq is not None: df_oq.to_excel(writer, index=False, sheet_name='OQ')
                if df_rtm is not None: df_rtm.to_excel(writer, index=False, sheet_name='RTM')
                pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='AUDIT_LOG')
            
            st.download_button("📊 Download Excel Workbook", output.getvalue(), f"{proj_name}_Validation.xlsx")
else:
    st.info("🔒 Secure System. Please sign in to continue.")
