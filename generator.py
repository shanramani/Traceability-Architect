import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.24"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    return "Thousand Oaks, USA"

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

# (Styles remain exactly as you have them, omitted here for brevity)
st.markdown("""<style>...</style>""", unsafe_allow_html=True) 

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 3. AUTHENTICATION (Unchanged) ---
def show_login():
    left_space, center_content, right_space = st.columns([3, 4, 3])
    with center_content:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        st.markdown("<h1 style='text-align: center;'>🛡️ Validation Doc Assist</h1>", unsafe_allow_html=True)
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
        p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
        b_left, b_center, b_right = st.columns([1, 2, 1])
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                if u: 
                    st.session_state.user_name = u
                    st.session_state.authenticated = True
                    st.rerun()

# --- 4. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        
        # Change dropdown without losing file state
        engine_name = st.selectbox("Model", list(MODELS.keys()), 
                                   index=list(MODELS.keys()).index(st.session_state.selected_model), 
                                   label_visibility="collapsed")
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name
            st.rerun()
        
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.sidebar.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate Validation Package")
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    
    # FIX: Check file presence immediately
    is_ready = sop_file is not None
    
    # UI Enhancement: Info box next to analysis button
    col_btn, col_info = st.columns([1, 2])
    
    with col_btn:
        run_btn = st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready)
    
    with col_info:
        if is_ready:
            file_size_mb = len(sop_file.getvalue()) / (1024 * 1024)
            st.caption(f"📎 **Target:** {sop_file.name} ({file_size_mb:.2f} MB)")

    if run_btn:
        st.info(f"Analysis sequence initiated using {st.session_state.selected_model}...")
        
        with st.spinner("Executing GAMP-5 Analysis & Excel Workbook Generation..."):
            try:
                # 1. PDF EXTRACTION
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                    tmp_file.write(sop_file.getvalue())
                    tmp_path = tmp_file.name
                loader = PyPDFLoader(tmp_path)
                pages = loader.load()
                sop_content = "\n".join([page.page_content for page in pages])
                os.remove(tmp_path)

                # FIX: Capture accurate Local Session info
                current_user = st.session_state.user_name
                current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # 2. PROMPT
                model_id = MODELS[st.session_state.selected_model]
                system_prompt = f"You are a Principal Validation Engineer. Operator identity: {current_user}."
                user_prompt = f"""
                SOP CONTENT: {sop_content}
                
                TASK: Generate a validation package with 4 datasets separated by '|||'.
                Dataset 1 (FRS): ID, Requirement, Priority, GxP_Impact
                Dataset 2 (OQ): Test_ID, Req_Link, Test_Step, Expected_Result
                Dataset 3 (Traceability): Req_ID, Test_ID, Gap_Status (Flag '[GAP]' if missing)
                Dataset 4 (Audit Log): Action, User, Timestamp, Change_Description

                AUDIT LOG DATA (Strict Compliance):
                - Action: "SOP Analysis Execution"
                - User: {current_user}
                - Timestamp: {current_timestamp}
                - Change_Description: "Full validation package generated from {sop_file.name}."
                
                Output ONLY raw CSV for each, separated by |||.
                """

                # 3. AI CALL
                response = completion(model=model_id, messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}])
                datasets = response.choices[0].message.content.split("|||")

                # 4. EXCEL GENERATION
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    names = ["FRS", "OQ", "Traceability", "Audit_Log"]
                    for idx, data in enumerate(datasets):
                        if idx < len(names):
                            df = pd.read_csv(io.StringIO(data.strip()), quotechar='"', on_bad_lines='skip')
                            df.to_excel(writer, sheet_name=names[idx], index=False)

                st.success("Analysis Complete: Validation Workbook Generated.")
                st.download_button(label="📥 Download Validation Workbook (.xlsx)", data=output.getvalue(), 
                                   file_name=f"Validation_Package_{datetime.date.today()}.xlsx", 
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            except Exception as e:
                st.error(f"❌ Engineering Error: {str(e)}")

if not st.session_state.authenticated: show_login()
else: show_app()