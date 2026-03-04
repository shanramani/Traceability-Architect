import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG & VERSIONING ---
VERSION = "8.1"
st.set_page_config(page_title=f"Traceability Architect v{VERSION}", layout="wide", page_icon="⚖️")

# --- 2. SESSION STATE INITIALIZATION ---
# This prevents "losing stuff" when the app reruns
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None
if 'user_name' not in st.session_state: st.session_state.user_name = ""

# --- 3. LANDING PAGE & AUTHENTICATION ---
def show_landing_page():
    st.title("🚀 Traceability Architect Pro")
    st.markdown(f"**Version {VERSION}** | *AI-Grounded GxP Compliance*")
    
    col1, col2 = st.columns(2)
    with col1:
        st.info("### 1. SOP Conversion\nTurn static PDFs into interactive, AI-driven work instructions.")
        st.info("### 2. 21 CFR Part 11 Audit\nAutomatically scan for Audit Trail and Signature gaps.")
    with col2:
        st.info("### 3. Automated Traceability\nInstant URS ➔ FS ➔ OQ mapping for Validation packages.")
        
    st.divider()
    with st.container():
        st.write("### 🔐 Auditor Access")
        u = st.text_input("Auditor User ID", placeholder="Enter your ID")
        p = st.text_input("Secure Access Key", type="password")
        if st.button("Authorize Access"):
            if u and p: # In production, verify against a secure DB/Secrets
                st.session_state.user_name = u
                st.session_state.authenticated = True
                st.rerun()
            else:
                st.error("Invalid Credentials.")

# --- 4. MAIN AUDIT DASHBOARD ---
def show_main_dashboard():
    with st.sidebar:
        st.title(f"🛡️ v{VERSION}")
        st.success(f"User: **{st.session_state.user_name}**")
        st.session_state.model_provider = st.radio("Intelligence Engine:", ["GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"])
        
        # API Connection Health Check
        st.divider()
        st.write("📡 **API Health Status**")
        if st.secrets.get("OPENAI_API_KEY"):
            st.write("✅ OpenAI Key Detected")
        else:
            st.error("❌ OpenAI Key Missing")
            
        if st.button("Log Out"):
            st.session_state.authenticated = False
            st.session_state.master_data = None
            st.rerun()

    st.header("📂 21 CFR Part 11 Automated Audit")
    
    # Error Handling for the Quota Issue
    uploaded_file = st.file_uploader("Upload URS/SOP (PDF)", type="pdf")

    if uploaded_file and st.button("🚀 Execute Audit Scan"):
        try:
            active_key = st.secrets.get("OPENAI_API_KEY") if "OpenAI" in st.session_state.model_provider else st.secrets.get("ANTHROPIC_API_KEY")
            
            with tempfile.NamedTemporaryFile(delete=False) as tmp:
                tmp.write(uploaded_file.getvalue())
                tmp_path = tmp.name
            
            loader = PyPDFLoader(tmp_path)
            full_text = " ".join([p.page_content for p in loader.load()])
            
            with st.spinner("Analyzing for Regulatory Gaps..."):
                prompt = (
                    f"GxP Audit of: {full_text[:8000]}. "
                    f"Requirements to extract: 5 items. "
                    f"Check for: §11.10(e) Audit Trails, §11.10(g) Authority Checks, §11.50 Signatures. "
                    f"Assign ALCOA+ Score (1-10). "
                    f"Return ONLY pipe-separated: URS_ID | URS_Text | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA_Score | Observation."
                )
                
                model_map = {"GPT-4o (OpenAI)": "openai/gpt-4o", "Claude 3.5 (Anthropic)": "anthropic/claude-3-5-sonnet-20240620"}
                res = completion(model=model_map[st.session_state.model_provider], messages=[{"role": "user", "content": prompt}], api_key=active_key)
                
                raw_rows = [ [i.strip() for i in l.split('|')] for l in res.choices[0].message.content.strip().split('\n') if '|' in l ]
                st.session_state.master_data = raw_rows
                st.success("Analysis Complete.")

        except Exception as e:
            if "quota" in str(e).lower():
                st.error("🚨 **OpenAI Quota Exceeded**: Please check your billing at platform.openai.com. You may need to add credits.")
            else:
                st.error(f"Error: {e}")
        finally:
            if 'tmp_path' in locals() and os.path.exists(tmp_path): os.remove(tmp_path)

    # Display Results if Data Exists
    if st.session_state.master_data:
        df = pd.DataFrame(st.session_state.master_data, columns=["URS_ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Part11_Ref", "ALCOA_Score", "Observation"])
        
        st.divider()
        st.subheader("📊 Compliance Findings")
        st.data_editor(df, use_container_width=True)

        if st.button("💾 Export Signed Package"):
            output = io.BytesIO()
            sig_block = pd.DataFrame([{"Approver": st.session_state.user_name, "Site": "91362", "Date": datetime.datetime.now(), "Meaning": "Certification of Part 11 Compliance"}])
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Traceability Matrix')
                sig_block.to_excel(writer, index=False, startrow=len(df)+3, sheet_name='Traceability Matrix')
            st.download_button("📥 Download Excel", data=output.getvalue(), file_name=f"Part11_Audit_v{VERSION}.xlsx")

# --- 5. APP ROUTING ---
if not st.session_state.authenticated:
    show_landing_page()
else:
    show_main_dashboard()