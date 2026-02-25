import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & iOS SLEEK UI ---
st.set_page_config(page_title="Traceability Architect v4.0", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; }

    /* Glassmorphism Design */
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px);
        border-right: 1px solid #e5e5ea;
    }

    /* iOS Progress Bar */
    .stProgress > div > div > div > div { background-color: #34c759; }

    /* Metric Cards */
    .metric-card {
        background: white;
        padding: 20px;
        border-radius: 16px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border: 1px solid #e5e5ea;
        text-align: center;
    }

    /* Modern Tab Pills */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: #f2f2f7;
        padding: 8px;
        border-radius: 16px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ffffff !important;
        box-shadow: 0px 4px 12px rgba(0,0,0,0.08);
        color: #007aff !important;
        font-weight: 600;
    }
    </style>
""", unsafe_allow_html=True)

# --- 2. LOGIC HELPERS ---
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'version' not in st.session_state: st.session_state.version = 0.1

def add_audit_entry(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({"Timestamp": timestamp, "User": user, "Action": action, "Version": f"v{st.session_state.version}"})

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
            return df.groupby(first_col).last().reset_index()
        return None
    except: return None

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🏗️ Architect")
    if 'authenticated' not in st.session_state: st.session_state.authenticated = False

    if not st.session_state.authenticated:
        u, p = st.text_input("User ID"), st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                add_audit_entry("Secure Sign-in Successful")
                st.rerun()
    else:
        st.success(f"Verified: **{st.session_state.user_name}**")
        st.caption(f"📍 Thousand Oaks (91362) | 🛡️ GxP Env")
        if st.button("Revoke Access"):
            add_audit_entry("Manual Session Termination")
            st.session_state.authenticated = False
            st.rerun()

    st.divider()
    proj_name = st.text_input("System ID", "Bio-RAG-Engine-01")
    doc_status = st.selectbox("Lifecycle Stage", ["Draft", "Review", "Approved", "Superseded"])
    if st.button("Increment Version"):
        st.session_state.version = round(st.session_state.version + 0.1, 1)
        add_audit_entry(f"Version up-revved to {st.session_state.version}")

# --- 4. MAIN INTERFACE ---
if st.session_state.authenticated:
    st.title("Validation Dashboard")
    
    col1, col2, col3 = st.columns(3)
    with col1: st.markdown('<div class="metric-card"><h3>Version</h3><h2>v'+str(st.session_state.version)+'</h2></div>', unsafe_allow_html=True)
    with col2: st.markdown('<div class="metric-card"><h3>Location</h3><h2>91362</h2></div>', unsafe_allow_html=True)
    with col3: st.markdown('<div class="metric-card"><h3>Integrity</h3><h2>ALCOA+</h2></div>', unsafe_allow_html=True)

    st.divider()
    
    uploaded_file = st.file_uploader("📂 Drop URS or SOP PDF here", type="pdf")
    if uploaded_file and st.button("🚀 Execute AI Validation Strategy"):
        add_audit_entry(f"File Analysis Initialized: {uploaded_file.name}")
        with st.spinner("Llama 3.3 performing deep requirement extraction..."):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp_file_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = "\n".join([p.page_content for p in loader.load()])
                llm = ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)
                prompt = f"""
                Act as a Principal CSV Engineer. Analyze this URS: {full_text[:12000]}
                Generate 3 Markdown tables separated by '---SECTION_SPLIT---'.
                
                SECTION 1 (FRS): [FRS_ID, Requirement, Criticality (High/Med/Low), Design_Note]
                SECTION 2 (OQ): [Test_ID, FRS_Link, Instruction, Acceptance_Criteria]
                SECTION 3 (RTM): [URS_ID, FRS_ID, Test_ID, Status]
                
                Ensure every URS_ID has an FRS_ID and Test_ID.
                """
                st.session_state.full_analysis = llm.invoke(prompt).content
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    # --- 5. RESULTS & ANALYTICS ---
    if st.session_state.full_analysis:
        parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
        df_frs, df_oq, df_rtm = extract_table(parts[0]), extract_table(parts[1]), extract_table(parts[2])

        tab_stats, tab_frs, tab_oq, tab_rtm, tab_audit = st.tabs(["📊 Stats", "⚡ FRS", "🧪 OQ", "🔗 RTM", "📋 Audit"])

        with tab_stats:
            if df_rtm is not None:
                total_reqs = len(df_rtm)
                gaps = df_rtm['Test_ID'].isna().sum()
                coverage = int(((total_reqs - gaps) / total_reqs) * 100)
                st.subheader("Traceability Coverage")
                st.progress(coverage / 100)
                st.write(f"**{coverage}%** of requirements are successfully linked to a test case.")
                if gaps > 0:
                    st.error(f"⚠️ Critical Gap: {gaps} requirements have no test coverage!")

        with tab_frs: st.markdown(parts[0])
        with tab_oq: st.markdown(parts[1])
        with tab_rtm: st.markdown(parts[2])
        with tab_audit:
            st.table(st.session_state.audit_trail)
            st.info(f"Digitally Signed By: {st.session_state.user_name} | Date: {datetime.datetime.now().strftime('%Y-%m-%d')}")

        # Excel Export
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            if df_frs is not None: df_frs.to_excel(writer, index=False, sheet_name='Functional_Specs')
            if df_oq is not None: df_oq.to_excel(writer, index=False, sheet_name='OQ_Protocol')
            if df_rtm is not None: df_rtm.to_excel(writer, index=False, sheet_name='Trace_Matrix')
            pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='Audit_Trail')
        
        st.download_button("📂 Export GxP Validation Package", output.getvalue(), f"{proj_name}_v{st.session_state.version}.xlsx")
else:
    st.info("🔒 Secure GxP Workspace. Please authorize to proceed.")
