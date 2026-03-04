import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
import tempfile
import io

# --- 1. UI CONFIG & CSS (Bringing back the look) ---
st.set_page_config(page_title="Traceability Architect", layout="wide", page_icon="🧪")

st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600&display=swap');
    html, body, [class*="css"]  { font-family: 'Inter', sans-serif; background-color: #f5f5f7; }
    .hero-banner {
        background: linear-gradient(90deg, #007aff 0%, #5856d6 100%);
        padding: 40px; border-radius: 24px; color: white;
        margin-bottom: 30px; box-shadow: 0 10px 20px rgba(0,122,255,0.2);
        text-align: center;
    }
    .hero-banner h1 { font-size: 2.8rem; margin-bottom: 5px; font-weight: 600; }
    .hero-banner p { font-size: 1.2rem; opacity: 0.9; font-weight: 300; }
    [data-testid="stSidebar"] {
        background: rgba(249, 249, 252, 0.95);
        backdrop-filter: blur(15px); border-right: 1px solid #e5e5ea;
    }
    </style>
""", unsafe_allow_html=True)

# --- 2. SESSION & KEYS ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'model_provider' not in st.session_state: st.session_state.model_provider = "Llama 3.3 (Groq)"

# Load Keys securely
for key in ["GROQ_API_KEY", "GEMINI_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]:
    val = st.secrets.get(key) or os.getenv(key)
    if val: os.environ[key] = val

# --- 3. SIDEBAR (Login & Engine Selection) ---
with st.sidebar:
    st.title("🧪 Admin Controls")
    if not st.session_state.authenticated:
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name, st.session_state.authenticated = u, True
                st.rerun()
    else:
        st.success(f"Verified: **{st.session_state.user_name}**")
        st.header("🤖 Engine Selection")
        st.session_state.model_provider = st.radio(
            "Select Intelligence Engine:",
            ["Llama 3.3 (Groq)", "GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"]
        )
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.rerun()

# --- 4. HEADER BANNER ---
st.markdown(f"""
    <div class="hero-banner">
        <h1>Traceability Architect</h1>
        <p>Intelligence Engine: {st.session_state.model_provider} | Site: 91362</p>
    </div>
""", unsafe_allow_html=True)

# --- 5. MAIN LOGIC ---
if not st.session_state.authenticated:
    st.markdown('<div style="text-align:center; padding:50px;"><h3>🔐 Secure Access Required</h3><p>Please authorize via the sidebar to access the GAMP 5 Engine.</p></div>', unsafe_allow_html=True)
else:
    # Input Data
    urs_input = [
        {"id": "URS-SEC-01", "text": "The system SHALL encrypt all PHI data at rest using AES-256."},
        {"id": "URS-COM-02", "text": "The system SHALL maintain an uneditable audit trail of all record changes."},
        {"id": "URS-FUN-03", "text": "The system SHOULD allow users to generate PDF reports of lab results."}
    ]
    
    st.write("### 1. Requirements Draft")
    st.json(urs_input)

    if st.button("🚀 Generate & Edit Traceability Matrix"):
        results = []
        progress = st.progress(0)
        
        # Mapping models for LiteLLM
        model_map = {
            "Llama 3.3 (Groq)": "groq/llama-3.3-70b-versatile",
            "GPT-4o (OpenAI)": "openai/gpt-4o",
            "Claude 3.5 (Anthropic)": "anthropic/claude-3-5-sonnet-20240620"
        }
        
        for i, item in enumerate(urs_input):
            with st.spinner(f"Analyzing {item['id']}..."):
                try:
                    res = completion(
                        model=model_map[st.session_state.model_provider],
                        messages=[{"role": "user", "content": f"Act as GxP Lead. Requirement: {item['text']}. Return pipe separated: Functional_Spec | Test_Step | Risk(H/M/L)"}]
                    )
                    parts = res.choices[0].message.content.split('|')
                    results.append({
                        "ID": item['id'],
                        "Requirement": item['text'],
                        "Functional_Spec": parts[0].strip() if len(parts)>0 else "Pending",
                        "Test_Steps": parts[1].strip() if len(parts)>1 else "Pending",
                        "Risk": parts[2].strip() if len(parts)>2 else "Med",
                        "Verified": False
                    })
                except Exception as e:
                    st.error(f"Error: {e}")
            progress.progress((i+1)/len(urs_input))
        
        st.session_state.master_df = pd.DataFrame(results)

    # The Interactive Editor
    if st.session_state.master_df is not None:
        st.write("### 2. Human-in-the-Loop Verification")
        edited_df = st.data_editor(st.session_state.master_df, use_container_width=True)
        
        # Sign-off & Export
        st.write("### 3. Final Approval")
        signer = st.text_input("Approver Name", value=st.session_state.user_name)
        if st.button("🖋️ Sign & Export"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                edited_df.to_excel(writer, index=False, sheet_name='Traceability_Matrix')
                pd.DataFrame([{"Signer": signer, "Date": datetime.datetime.now()}]).to_excel(writer, index=False, sheet_name='Audit_Trail')
            
            st.download_button("📥 Download Validated Report", data=output.getvalue(), file_name="RTM_Final.xlsx")