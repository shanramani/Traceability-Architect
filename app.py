import streamlit as st
from langchain_groq import ChatGroq
import os
import datetime
import pandas as pd
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. CONFIG & iOS ENTERPRISE UI ---
st.set_page_config(page_title="AI GxP Validation Suite", layout="wide", page_icon="🏗️")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }

    /* Top Banner Style */
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px;
        border-radius: 24px;
        color: white;
        margin-bottom: 30px;
        box-shadow: 0 10px 20px rgba(0,122,255,0.2);
    }

    /* Glassmorphism Sidebar */
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px);
        border-right: 1px solid #e5e5ea;
    }

    /* Metric Cards */
    .metric-card {
        background: white;
        padding: 24px;
        border-radius: 20px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border: 1px solid #e5e5ea;
        text-align: center;
    }

    /* iOS Tab Pills */
    .stTabs [data-baseweb="tab-list"] {
        gap: 12px;
        background-color: #e5e5ea;
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

# --- 2. SESSION LOGIC ---
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []
if 'full_analysis' not in st.session_state: st.session_state.full_analysis = None
if 'version' not in st.session_state: st.session_state.version = 0.1

def add_audit_entry(action):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    user = st.session_state.get('user_name', 'System')
    st.session_state.audit_trail.append({
        "Timestamp": timestamp, 
        "User": user, 
        "Action": action, 
        "Revision": f"v{st.session_state.version}"
    })

def extract_table(text):
    try:
        lines = [line for line in text.split('\n') if '|' in line]
        if len(lines) > 2:
            raw_data = '\n'.join(lines)
            df = pd.read_csv(io.StringIO(raw_data), sep='|', skipinitialspace=True).dropna(axis=1, how='all')
            df.columns = [c.strip() for c in df.columns]
            df = df[~df.iloc[:,0].str.contains('---', na=False)]
            first_col = df.columns[0]
            return df.replace('', pd.NA).ffill().bfill().groupby(first_col).last().reset_index()
        return None
    except: return None

# --- 3. SIDEBAR (CONTROLS) ---
with st.sidebar:
    st.title("AI Powered GxP Validation Suite")
    if 'authenticated' not in st.session_state: st.session_state.authenticated = False

    if not st.session_state.authenticated:
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize Access"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                add_audit_entry("Secure Sign-in Successful")
                st.rerun()
    else:
        st.success(f"User: **{st.session_state.user_name}**")
        st.caption(f"📍 91362 | 🛡️ GxP Env")
        if st.button("Revoke Access"):
            add_audit_entry("Manual Logout")
            st.session_state.authenticated = False
            st.rerun()

    st.divider()
    proj_name = st.text_input("System ID", "BioLogistics-RAG-01")
    if st.button("Increment Revision"):
        st.session_state.version = round(st.session_state.version + 0.1, 1)
        add_audit_entry(f"Version up-revved to {st.session_state.version}")

# --- 4. DASHBOARD & ANALYSIS ---
if st.session_state.authenticated:
    # --- TOP BANNER ---
    st.markdown("""
        <div class="hero-banner">
            <h1>AI Powered GxP Validation Suite</h1>
            <p>Authoring FRS, OQ, and RTM artifacts with Llama 3.3 Intelligence</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Executive KPIs
    c1, c2, c3 = st.columns(3)
    with c1: st.markdown(f'<div class="metric-card"><h4>Current Version</h4><h2>v{st.session_state.version}</h2></div>', unsafe_allow_html=True)
    with c2: st.markdown(f'<div class="metric-card"><h4>Location</h4><h2>91362</h2></div>', unsafe_allow_html=True)
    with c3: st.markdown(f'<div class="metric-card"><h4>Integrity Status</h4><h2>ALCOA+</h2></div>', unsafe_allow_html=True)

    st.divider()

    uploaded_file = st.file_uploader("📂 Upload URS / SOP PDF", type="pdf")
    if uploaded_file and st.button("🚀 Execute Validation Authoring"):
        add_audit_entry(f"Analysis Started: {uploaded_file.name}")
        with st.spinner("Analyzing requirements..."):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                full_text = "\n".join([p.page_content for p in loader.load()])
                llm = ChatGroq(model_name="llama-3.3-70b-versatile", groq_api_key=st.secrets["GROQ_API_KEY"], temperature=0)
                prompt = f"""
                Act as a Principal CSV Engineer. Analyze: {full_text[:12000]}
                Generate 3 sections separated by '---SECTION_SPLIT---'.
                Section 1 (FRS): [FRS_ID, Requirement, Criticality (High/Med/Low), Design_Note]
                Section 2 (OQ): [Test_ID, FRS_Link, Instruction, Acceptance_Criteria]
                Section 3 (RTM): [URS_ID, FRS_ID, Test_ID, Trace_Status]
                """
                st.session_state.full_analysis = llm.invoke(prompt).content
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    # --- 5. RESULTS ---
    if st.session_state.full_analysis:
        parts = st.session_state.full_analysis.split('---SECTION_SPLIT---')
        df_frs, df_oq, df_rtm = extract_table(parts[0]), extract_table(parts[1]), extract_table(parts[2])

        t_stats, t_frs, t_oq, t_rtm, t_audit = st.tabs(["📊 Analytics", "⚡ FRS", "🧪 OQ", "🔗 RTM", "📋 Audit"])

        with t_stats:
            if df_rtm is not None:
                total = len(df_rtm)
                missing = df_rtm['Test_ID'].isna().sum()
                coverage = int(((total - missing) / total) * 100)
                st.subheader("Traceability Coverage Index")
                st.progress(coverage / 100)
                st.write(f"**{coverage}%** of User Requirements are linked to testable OQ scripts.")

        with t_frs: st.markdown(parts[0])
        with t_oq: st.markdown(parts[1])
        with t_rtm: st.markdown(parts[2])
        with t_audit:
            st.dataframe(pd.DataFrame(st.session_state.audit_trail), use_container_width=True)
            st.markdown(f"**Digital Sign-off:** `/s/ {st.session_state.user_name}`")

        # Excel Package
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            for df, name in zip([df_frs, df_oq, df_rtm], ['FRS', 'OQ', 'RTM']):
                if df is not None: df.to_excel(writer, index=False, sheet_name=name)
            pd.DataFrame(st.session_state.audit_trail).to_excel(writer, index=False, sheet_name='AUDIT_LOG')
        
        st.download_button("📂 Export GxP Validation Package", output.getvalue(), f"{proj_name}_v{st.session_state.version}.xlsx")
else:
    st.info("🔒 Authorized Access Required. Please sign in via sidebar.")
