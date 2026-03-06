import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io
import requests

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.24"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Persistent location context for 91362 [cite: 2025-12-28]
    return "Thousand Oaks, USA"

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'selected_model' not in st.session_state: st.session_state.selected_model = "Gemini 1.5 Pro"
if 'location' not in st.session_state: st.session_state.location = get_location()

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    
    .stApp { background-color: #fcfcfd; }
    
    /* BANNER & LOGIN */
    .top-banner {
        background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
        padding: 12px 0px; text-align: center; margin-bottom: 5px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569; font-weight: 400; letter-spacing: 4px;
        text-transform: uppercase; font-size: 0.85rem; margin: 0;
    }
    [data-testid="stTextInput"] { width: 50% !important; margin: 0 auto !important; }

    /* MODERN BLUE BUTTON ENGINE */
    /* Target Login and Sidebar Terminate and Active Run Analysis */
    div.stButton > button {
        border: none !important;
        transition: all 0.2s ease-in-out !important;
    } */



    /* 2. Style the Login Button to match that left edge */
    /* 1. Center the button's container horizontally */
    /* 1. Target the 'widget' container that holds the button */
    /* 1. Force the outer button container to take up 100% width of the page */
    div.stButton {
        width: 100% !important;
        display: flex !important;
        justify-content: center !important;
    }

    /* 2. Set the button to exactly 40% and use auto-margins to snap to center */
    div.stButton > button[key="login_btn"] {
        width: 40% !important;
        margin: 0 auto !important; 
        display: block !important;
        
        /* Branding Colors */
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important;
        height: 3.2rem !important;
        border-radius: 8px !important;
        border: none !important;
        font-weight: 600 !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    }

    /* RUN ANALYSIS - MODERN BLUE (When Active) */
    div.stButton > button[key="run_analysis_btn"] {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important;
        padding: 0.75rem 3rem !important; 
        font-size: 1.1rem !important;
        border-radius: 8px !important;
        box-shadow: 0 4px 15px rgba(37, 99, 235, 0.3);
    }

    /* HOVER EFFECTS */
    div.stButton > button:hover:not(:disabled) {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 20px rgba(37, 99, 235, 0.4) !important;
        filter: brightness(1.1);
    }

    /* DISABLED STATE */
    div.stButton > button:disabled {
        background: #e2e8f0 !important;
        color: #94a3b8 !important;
        cursor: not-allowed !important;
        transform: none !important;
        box-shadow: none !important;
    }

    /* SIDEBAR STYLING & KILL KEYBOARD_DOUBLE */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    [data-testid="stSidebar"] [data-testid="stHeader"], 
    [data-testid="stSidebarCollapseButton"],
    [title="keyboard_double_arrow_left"] { display: none !important; }
    
    .sb-title { color: white !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sb-sub { color: white !important; font-weight: 700 !important; font-size: 0.95rem; }
    .system-spacer { margin-top: 80px; }
    .sidebar-stats { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    /* Sidebar terminate width */
    div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }
    
    /* Login Centering */
    .login-center { display: flex; justify-content: center; width: 100%; }
    .login-center div.stButton > button { width: 50% !important; }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
    "GPT-4o": "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# --- 3. AUTHENTICATION ---

def show_login():
    # Keep the 30/40/30 split to maintain the centering that worked
    left_space, center_content, right_space = st.columns([3, 4, 3])

    with center_content:
        # 1. Put the banner back at the very top of the center column
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        
        # 2. Add the Title
        st.markdown("<h1 style='text-align: center;'>🛡️ Validation Doc Assist</h1>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        # 3. Text Inputs
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
        p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        # 4. The Button (centered and matching the 40% column width)
        # Create nested columns inside your 'center_content' to make the button 50% width
        # [1, 2, 1] means the button takes 2/4 (50%) of the 40% column
        b_left, b_center, b_right = st.columns([1, 2, 1])
        
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                if u: 
                    st.session_state.user_name = u
                    st.session_state.authenticated = True
                    st.rerun()

# --- 4. MAIN APPLICATION ---
def show_app():
    # Sidebar remains the same
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        engine_name = st.selectbox("Model", list(MODELS.keys()), 
                                   index=list(MODELS.keys()).index(st.session_state.selected_model), 
                                   label_visibility="collapsed")
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
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
    
    is_ready = sop_file is not None
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
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

                # 2. PROMPT FOR MULTI-SHEET LOGIC
                model_id = MODELS[st.session_state.selected_model]
                system_prompt = "You are a Principal Validation Engineer. You output structured CSV data for validation workbooks."
                user_prompt = f"""
                SOP CONTENT:
                {sop_content}

                TASK: Parse this SOP into 4 distinct datasets. 
                Use the delimiter '|||' between datasets.
                
                Dataset 1 (FRS): ID, Requirement_Description, Priority, GxP_Impact
                Dataset 2 (OQ): Test_ID, Requirement_Link, Test_Step, Expected_Result
                Dataset 3 (Traceability): Req_ID, Test_ID, Gap_Analysis (Flag [GAP] if requirement is missing a test or is untestable)
                Dataset 4 (Audit Log): Action, User, Timestamp, Change_Description (Generate a dummy log for this session)
                
                Format: Raw CSV only for each dataset, separated by |||.
                """

                # 3. AI CALL
                response = completion(
                    model=model_id,
                    messages=[{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}]
                )
                raw_output = response.choices[0].message.content

                # 4. EXCEL GENERATION (In-Memory)
                datasets = raw_output.split("|||")
                output = io.BytesIO()
                
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    sheet_names = ["FRS", "OQ", "Traceability", "Audit_Log"]
                    for i, data in enumerate(datasets):
                        if i < len(sheet_names):
                            df = pd.read_csv(io.StringIO(data.strip()), on_bad_lines='skip')
                            df.to_excel(writer, sheet_name=sheet_names[i], index=False)

                st.success("Analysis Complete: Validation Workbook Generated.")
                
                # 5. DOWNLOAD BUTTON
                st.download_button(
                    label="📥 Download Validation Workbook (.xlsx)",
                    data=output.getvalue(),
                    file_name=f"Validation_Package_{datetime.date.today()}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"❌ Engineering Error: {str(e)}")

if not st.session_state.authenticated: show_login()
else: show_app()