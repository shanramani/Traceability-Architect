import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import plotly.express as px

# --- 1. UI CONFIG & 2026 STYLING ---
VERSION = "9.9"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; color: #0f172a; }
    .block-container { max-width: 1250px; padding-top: 2rem; }
    
    /* Modern Tab Styling */
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { 
        background-color: #f1f5f9; border-radius: 6px; padding: 10px 20px; font-weight: 500; 
    }
    .stTabs [aria-selected="true"] { background-color: #0f172a !important; color: white !important; }

    /* Buttons & Inputs */
    .stButton > button { 
        background: #0f172a; color: white; width: 100%; border-radius: 8px; font-weight: 500; 
    }
    </style>
    """, unsafe_allow_html=True)

# --- 2. SESSION STATE & AUDIT LOGGING ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'audit_trail' not in st.session_state: st.session_state.audit_trail = []

def log_event(action):
    st.session_state.audit_trail.append({
        "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
        "User": st.session_state.user_name, 
        "Action": action
    })

# Stable Model Config
model_config = {
    "Groq": {"id": "groq/llama-3.3-70b-versatile", "key": "GROQ_API_KEY"},
    "Gemini": {"id": "gemini/gemini-1.5-pro", "key": "GEMINI_API_KEY"}
}

# --- 3. MAIN DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.title(f"Sovereign v{VERSION}")
        engine = st.radio("Intelligence Provider", list(model_config.keys()))
        st.divider()
        st.caption(f"Operator: {st.session_state.user_name} | Site: 91362")
        if st.button("Logout"): 
            st.session_state.authenticated = False
            st.rerun()

    st.header("V-Model Validation & Gap Intelligence")
    uploaded_file = st.file_uploader("Upload Source Document (SOP/URS/FS)", type="pdf")

    if uploaded_file and st.button("🚀 Run Full Audit Cycle"):
        api_key = st.secrets.get(model_config[engine]["key"])
        if not api_key: st.error("API Key missing.")
        else:
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
            try:
                loader = PyPDFLoader(tmp_path)
                text = " ".join([p.page_content for p in loader.load()])
                with st.spinner("Analyzing V-Model Alignment..."):
                    prompt = (
                        f"Analyze: {text[:8000]}. Perform a GxP Gap Analysis. "
                        "Return pipe-separated rows: URS_ID | URS_Desc | FS_ID | FS_Detail | OQ_ID | OQ_Protocol | Risk | Ref | Justification. "
                        "Risk must be 'High', 'Medium', or 'Low'. "
                        "OQ_Protocol must be 'Step 1: [Action]; Expected Result: [Result].' "
                        "If a requirement is untestable or missing a spec, explain why in the Justification column."
                    )
                    
                    # Failover Logic
                    try:
                        res = completion(model=model_config[engine]["id"], messages=[{"role":"user","content":prompt}], api_key=api_key)
                    except Exception:
                        res = completion(model=model_config["Groq"]["id"], messages=[{"role":"user","content":prompt}], api_key=st.secrets.get("GROQ_API_KEY"))

                    data = [l.split('|') for l in res.choices[0].message.content.strip().split('\n') if '|' in l]
                    clean_data = [d[:9] if len(d)>=9 else d + ["N/A"]*(9-len(d)) for d in data]
                    st.session_state.master_df = pd.DataFrame(clean_data, columns=["URS_ID", "URS_Description", "FS_ID", "FS_Detail", "OQ_ID", "OQ_Protocol", "Risk", "Ref", "Justification"])
                    log_event(f"Audit Complete via {engine}")
            except Exception as e: st.error(f"Analysis Failed: {e}")
            finally: 
                if os.path.exists(tmp_path): os.remove(tmp_path)

    if st.session_state.master_df is not None:
        st.divider()
        df = st.session_state.master_df
        
        # --- THE MASTER TABS ---
        tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
            "📋 URS", "⚙️ FS", "🧪 OQ Protocols", "🔗 Trace Matrix", "⚠️ Gap & Risk", "📑 Audit Ledger"
        ])
        
        with tab1:
            st.subheader("User Requirements Specification")
            st.dataframe(df[["URS_ID", "URS_Description", "Risk", "Ref"]].drop_duplicates(), use_container_width=True, hide_index=True)
        
        with tab2:
            st.subheader("Functional Specification Mapping")
            st.dataframe(df[["URS_ID", "FS_ID", "FS_Detail"]], use_container_width=True, hide_index=True)
            
        with tab3:
            st.subheader("Operational Qualification Protocols")
            st.dataframe(df[["FS_ID", "OQ_ID", "OQ_Protocol"]], use_container_width=True, hide_index=True)
            
        with tab4:
            st.subheader("Master Traceability Matrix (URS → FS → OQ)")
            st.dataframe(df[["URS_ID", "FS_ID", "OQ_ID", "Risk"]], use_container_width=True, hide_index=True)
            
        with tab5:
            st.subheader("Gap Analysis & Risk Scorecard")
            col_a, col_b = st.columns([1, 1])
            with col_a:
                # Risk Pie Chart
                risk_counts = df['Risk'].value_counts().reset_index()
                fig = px.pie(risk_counts, values='count', names='Risk', title="Risk Profile Distribution",
                             color_discrete_map={'High':'#ef4444', 'Medium':'#f59e0b', 'Low':'#10b981'})
                st.plotly_chart(fig, use_container_width=True)
            with col_b:
                # Gap Logic
                gaps = df[(df['FS_ID'].str.contains('MISSING|N/A', na=False)) | (df['OQ_ID'].str.contains('MISSING|UNTESTABLE|N/A', na=False))]
                st.metric("Identified Gaps", len(gaps))
                st.dataframe(gaps[["URS_ID", "Justification"]], use_container_width=True, hide_index=True)
            
        with tab6:
            st.subheader("21 CFR Part 11.10(e) Audit Ledger")
            st.table(st.session_state.audit_trail)

        # Multi-Sheet Export
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Master_Trace_Matrix', index=False)
            pd.DataFrame(st.session_state.audit_trail).to_excel(writer, sheet_name='Audit_Trail', index=False)
        st.download_button("📥 Export Certified Workbook", data=output.getvalue(), file_name=f"GxP_Audit_Package_{datetime.date.today()}.xlsx")

# --- 4. AUTH ---
if not st.session_state.authenticated:
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.title("Traceability Architect")
        u = st.text_input("Auditor ID"); p = st.text_input("Key", type="password")
        if st.button("Authorize Session"): 
            st.session_state.user_name, st.session_state.authenticated = u, True
            log_event("Login Success"); st.rerun()
else: show_main_dashboard()