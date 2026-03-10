import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import sqlite3

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.26"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    return "Thousand Oaks, USA"

# --- 2. DATABASE LOGIC ---
def log_audit_activity(user, action, object_type, object_id):
    try:
        conn = sqlite3.connect("validation_app.db")
        cursor = conn.cursor()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO audit_log (user, action, object_type, object_id, timestamp)
            VALUES (?, ?, ?, ?, ?)
        """, (user, action, object_type, object_id, timestamp))
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Database Logging Error: {e}")

def get_audit_history():
    try:
        conn = sqlite3.connect("validation_app.db")
        df = pd.read_sql_query("SELECT timestamp, user, action, object_id FROM audit_log ORDER BY id DESC LIMIT 10", conn)
        conn.close()
        return df
    except:
        return pd.DataFrame()

# --- 3. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

# Maintaining your exact CSS and styling elements
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #fcfcfd; }
    .top-banner { background-color: white; border: 1px solid #eef2f6; border-radius: 10px; padding: 12px 0px; text-align: center; margin-bottom: 5px; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); }
    .banner-text-inner { color: #475569; font-weight: 400; letter-spacing: 4px; text-transform: uppercase; font-size: 0.85rem; margin: 0; }
    div.stButton { width: 100% !important; display: flex !important; justify-content: center !important; }
    div.stButton > button[key="login_btn"] { width: 40% !important; margin: 0 auto !important; background: linear-gradient(135deg, #3b82f6, #2563eb) !important; color: white !important; height: 3.2rem !important; border-radius: 8px !important; border: none !important; font-weight: 600 !important; }
    div.stButton > button[key="run_analysis_btn"] { background: linear-gradient(135deg, #3b82f6, #2563eb) !important; color: white !important; padding: 0.75rem 3rem !important; font-size: 1.1rem !important; border-radius: 8px !important; box-shadow: 0 4px 15px rgba(37, 99, 235, 0.3); }
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sb-sub { color: white !important; font-weight: 700 !important; font-size: 0.95rem; }
    .sidebar-stats { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 4. AUTHENTICATION ---
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

# --- 5. MAIN APPLICATION ---
def show_app():
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        engine_name = st.selectbox("Model", list(MODELS.keys()), 
                                   index=list(MODELS.keys()).index(st.session_state.selected_model), 
                                   label_visibility="collapsed")
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
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
    
    is_ready = sop_file is not None
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.info(f"Analysis sequence initiated using {st.session_state.selected_model}...")
        
        with st.spinner("Executing GAMP-5 Analysis (Segmented Processing)..."):
            try:
                # 1. PDF EXTRACTION
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
                    tmp_file.write(sop_file.getvalue())
                    tmp_path = tmp_file.name
                loader = PyPDFLoader(tmp_path)
                pages = loader.load()
                os.remove(tmp_path)

                # REINFORCED CHUNKING LOGIC to prevent "Stream Ended"
                chunk_size = 8  # Smaller chunks are safer for large outputs
                all_results = {"FRS": [], "OQ": [], "Traceability": [], "Audit_Log": []}
                
                for i in range(0, len(pages), chunk_size):
                    chunk = pages[i:i+chunk_size]
                    sop_content = "\n".join([p.page_content for p in chunk])
                    st.write(f"⚙️ Processing Batch: Pages {i+1} to {min(i+chunk_size, len(pages))}...")
                    
                    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    model_id = MODELS[st.session_state.selected_model]
                    
                    user_prompt = f"""
                    SOP CONTENT (SEGMENT): {sop_content}
                    TASK: Parse into 4 datasets separated by '|||'.
                    Dataset 1 (FRS): ID, Requirement_Description, Priority, GxP_Impact
                    Dataset 2 (OQ): Test_ID, Requirement_Link, Test_Step, Expected_Result
                    Dataset 3 (Traceability): Req_ID, Test_ID, Gap_Analysis
                    Dataset 4 (Audit Log): Action, User, Timestamp, Description
                    
                    AUDIT LOG SPEC: User: {st.session_state.user_name}, Time: {current_time}, Action: 'Segment Analysis'
                    Format: Raw CSV only, separated by |||. Use double quotes for descriptions.
                    """

                    # Explicitly disabling streaming to prevent partial-packet crashes
                    response = completion(
                        model=model_id, 
                        messages=[{"role": "system", "content": "Principal Validation Engineer."}, {"role": "user", "content": user_prompt}],
                        timeout=400, 
                        stream=False
                    )
                    
                    datasets = response.choices[0].message.content.split("|||")
                    for idx, key in enumerate(["FRS", "OQ", "Traceability", "Audit_Log"]):
                        if idx < len(datasets):
                            all_results[key].append(datasets[idx].strip())

                # 4. CONSOLIDATED EXCEL GENERATION
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    for sheet_name, csv_list in all_results.items():
                        combined_csv = "\n".join(csv_list)
                        df = pd.read_csv(io.StringIO(combined_csv), quotechar='"', on_bad_lines='skip')
                        # Clean duplicate headers from intermediate chunks
                        df = df[df.iloc[:, 0].astype(str) != df.columns[0]]
                        df.to_excel(writer, sheet_name=sheet_name, index=False)

                # 5. DB PERMANENT LOGGING
                log_audit_activity(st.session_state.user_name, "Generated Validation Package", "SOP_PDF", sop_file.name)

                st.success("Analysis Complete: Validation Workbook Generated & Logged.")
                st.download_button(label="📥 Download Validation Workbook (.xlsx)", data=output.getvalue(), 
                                   file_name=f"Validation_Package_{datetime.date.today()}.xlsx", 
                                   mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            except Exception as e:
                st.error(f"❌ Engineering Error: {str(e)}")
                log_audit_activity(st.session_state.user_name, "Analysis Failed", "SYSTEM_ERR", str(e)[:100])

    # Display Permanent Audit Trail at the bottom
    st.markdown("---")
    st.subheader("📜 System History (from SQLite)")
    history_df = get_audit_history()
    if not history_df.empty:
        st.dataframe(history_df, use_container_width=True)

if not st.session_state.authenticated: show_login()
else: show_app()