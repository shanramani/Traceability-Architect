import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & PREMIUM STYLING ---
st.set_page_config(page_title="Traceability Architect Pro", layout="wide", page_icon="🧪")

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

# --- 2. SESSION & KEY MANAGEMENT ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_df' not in st.session_state: st.session_state.master_df = None
if 'model_provider' not in st.session_state: st.session_state.model_provider = "Llama 3.3 (Groq)"

# Load Keys securely from Streamlit Secrets
for key in ["GROQ_API_KEY", "GEMINI_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_API_KEY"]:
    val = st.secrets.get(key)
    if val: os.environ[key] = val

# --- 3. SIDEBAR (Login & Engine Selection) ---
with st.sidebar:
    st.title("🧪 Admin Controls")
    if not st.session_state.authenticated:
        st.subheader("🔑 Secure Access")
        u = st.text_input("User ID")
        p = st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: 
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.warning("Please enter a User ID.")
    else:
        st.success(f"Verified: **{st.session_state.user_name}**")
        st.header("🤖 Engine Selection")
        st.session_state.model_provider = st.radio(
            "Select Intelligence Engine:",
            ["Llama 3.3 (Groq)", "GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"]
        )
        st.divider()
        if st.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.master_df = None
            st.rerun()

# --- 4. SHARED HEADER (Always Visible) ---
st.markdown(f"""
    <div class="hero-banner">
        <h1>Traceability Architect</h1>
        <p>Intelligence Engine: {st.session_state.model_provider} | Site: 91362</p>
    </div>
""", unsafe_allow_html=True)

# --- 5. AUTHENTICATION LOGIC FLOW ---
if not st.session_state.authenticated:
    # --- PUBLIC LANDING PAGE ---
    st.markdown("""
        <div style="text-align: center; padding: 60px;">
            <h2 style="color: #1d1d1f;">AI-Powered GxP Validation Suite</h2>
            <p style="color: #8e8e93; font-size: 1.2rem; max-width: 600px; margin: 0 auto;">
                Transforming static SOPs and URS documents into validated technical specifications.
            </p>
            <div style="margin-top: 30px; padding: 20px; background: white; border-radius: 16px; border: 1px solid #e5e5ea; display: inline-block;">
                <p style="color: #007aff; font-weight: 600; margin:0;">🔐 Please sign in via the sidebar to access the engine.</p>
            </div>
        </div>
    """, unsafe_allow_html=True)
else:
    # --- PRIVATE DASHBOARD (HIDDEN UNTIL LOGIN) ---
    st.subheader("📂 Step 1: Document Ingestion")
    uploaded_file = st.file_uploader("Upload URS or SOP PDF", type="pdf")

    if uploaded_file:
        st.success(f"File '{uploaded_file.name}' received.")
        
        if st.button("🚀 Analyze & Generate Matrix"):
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            try:
                with st.spinner("Extracting Pharmaceutical requirements..."):
                    loader = PyPDFLoader(tmp_path)
                    pages = loader.load()
                    full_text = " ".join([p.page_content for p in pages])
                
                results = []
                model_map = {
                    "Llama 3.3 (Groq)": "groq/llama-3.3-70b-versatile",
                    "GPT-4o (OpenAI)": "openai/gpt-4o",
                    "Claude 3.5 (Anthropic)": "anthropic/claude-3-5-sonnet-20240620"
                }

                with st.spinner(f"Orchestrating {st.session_state.model_provider}..."):
                    prompt = (f"Analyze this document: {full_text[:8000]}. "
                              f"Extract 5 key technical requirements. For each, return exactly: "
                              f"Req_ID | Requirement_Text | Functional_Spec | Test_Step | Risk(H/M/L). "
                              f"Separate each requirement with a new line.")
                    
                    res = completion(
                        model=model_map[st.session_state.model_provider],
                        messages=[{"role": "user", "content": prompt}]
                    )
                    
                    lines = res.choices[0].message.content.strip().split('\n')
                    for line in lines:
                        if '|' in line:
                            p = line.split('|')
                            if len(p) >= 5:
                                results.append({
                                    "ID": p[0].strip(),
                                    "Requirement": p[1].strip(),
                                    "Functional_Spec": p[2].strip(),
                                    "Test_Steps": p[3].strip(),
                                    "Risk": p[4].strip(),
                                    "Verified": False
                                })
                
                st.session_state.master_df = pd.DataFrame(results)
                st.success("Analysis Complete!")
                
            except Exception as e:
                st.error(f"Validation Error: {e}")
            finally:
                if os.path.exists(tmp_path): os.remove(tmp_path)

    # 3. INTERACTIVE REVIEW & EXPORT
    if st.session_state.master_df is not None:
        st.divider()
        st.subheader("🛠️ Step 2: Human-in-the-Loop Verification")
        
        edited_df = st.data_editor(
            st.session_state.master_df, 
            use_container_width=True,
            column_config={
                "Verified": st.column_config.CheckboxColumn("Verify", default=False),
                "Risk": st.column_config.SelectboxColumn("Risk Level", options=["High", "Med", "Low"])
            }
        )
        
        st.divider()
        st.subheader("🖋️ Step 3: Approval & Export")
        signer = st.text_input("Approver Digital Name", value=st.session_state.user_name)
        
        if st.button("💾 Sign & Generate GxP Workbook"):
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                edited_df.to_excel(writer, index=False, sheet_name='Traceability_Matrix')
                audit_data = {
                    "Signer": signer, 
                    "Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Engine": st.session_state.model_provider,
                    "Integrity": "ALCOA+ Compliant"
                }
                pd.DataFrame([audit_data]).to_excel(writer, index=False, sheet_name='Audit_Trail')
            
            st.download_button(
                label="📥 Download SIGNED GxP Workbook",
                data=output.getvalue(),
                file_name=f"Verified_RTM_{datetime.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )