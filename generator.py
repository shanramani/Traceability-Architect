import streamlit as st
import os

# --- 1. PRO-GRADE UI & BRANDING ---
VERSION = "10.28"
st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    # Locked to Thousand Oaks, USA context [cite: 2025-12-28]
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
    
    /* LOGIN PAGE CENTERING */
    .login-container {
        display: flex; flex-direction: column; align-items: center; 
        width: 100%; margin-top: 10vh;
    }

    /* TOP BANNER */
    .top-banner {
        background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
        padding: 12px 0px; text-align: center; margin-bottom: 25px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        width: 320px;
    }
    .banner-text-inner {
        color: #475569; font-weight: 400; letter-spacing: 4px;
        text-transform: uppercase; font-size: 0.85rem; margin: 0;
    }

    /* INPUT BOXES (HALF WIDTH) */
    [data-testid="stTextInput"] { 
        width: 320px !important; 
        margin: 0 auto !important; 
    }

    /* THE CENTERED BUTTON ENGINE */
    div.stButton {
        display: flex;
        justify-content: center;
        width: 100%;
    }

    div.stButton > button[key="login_btn"] {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important;
        width: 320px !important;
        height: 3.2rem !important;
        border-radius: 8px !important;
        border: none !important;
        font-weight: 600 !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
        margin-top: 15px !important;
    }

    /* SIDEBAR: SLATE-CHARCOAL BACKGROUND */
    [data-testid="stSidebar"] { 
        background-color: #1e293b !important; 
        border-right: 1px solid #334155; 
    }
    
    /* HIDE KEYBOARD_DOUBLE */
    [data-testid="stSidebar"] [data-testid="stHeader"], 
    [data-testid="stSidebarCollapseButton"],
    [title="keyboard_double_arrow_left"] { display: none !important; }

    /* TERMINATE BUTTON CONTRAST */
    div.stButton > button[key="terminate_sidebar"] {
        width: 100% !important; 
        background-color: #2563eb !important; 
        color: white !important;
    }

    /* SIDEBAR TEXT */
    .sb-title { color: #f8fafc !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sidebar-stats { color: #f1f5f9 !important; font-weight: 400 !important; font-size: 0.85rem; }
    .system-spacer { margin-top: 80px; }
    </style>
    """, unsafe_allow_html=True)

# --- 3. AUTHENTICATION ---
def show_login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
    st.markdown("<h1 style='text-align: center;'>🛡️ Validation Doc Assist</h1>", unsafe_allow_html=True)
    u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
    p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
    
    if st.button("Initialize Secure Session", key="login_btn"):
        if u: 
            st.session_state.user_name = u
            st.session_state.authenticated = True
            st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)

# --- 4. MAIN APPLICATION ---
def show_app():
    MODELS = {
        "Gemini 1.5 Pro": "gemini/gemini-1.5-pro", 
        "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620", 
        "GPT-4o": "openai/gpt-4o",
        "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile" # Restored Groq [cite: 2026-02-11]
    }

    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub" style="color:#cbd5e1; font-weight:700;">🤖 Intelligence Engine</p>', unsafe_allow_html=True)
        
        engine_name = st.selectbox("Model", list(MODELS.keys()), 
                                   index=list(MODELS.keys()).index(st.session_state.selected_model), 
                                   label_visibility="collapsed")
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name; st.rerun()
        
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

        st.markdown('<p class="sb-sub" style="color:#cbd5e1; font-weight:700;">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {st.session_state.user_name}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        
        if st.button("Terminate Session", key="terminate_sidebar"):
            st.session_state.authenticated = False; st.rerun()

    st.title("Auto-Generate CSV Documents")
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    
    # Analysis logic persistence [cite: 2026-02-11]
    is_ready = sop_file is not None
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.success(f"Analysis initiated via {st.session_state.selected_model}.")

if not st.session_state.authenticated: 
    show_login()
else: 
    show_app()