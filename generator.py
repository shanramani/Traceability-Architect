import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
from langchain_community.document_loaders import PyPDFLoader
import tempfile
import io

# --- 1. UI CONFIG ---
st.set_page_config(page_title="Inspector's Choice - Part 11 Audit", layout="wide", page_icon="⚖️")

# --- 2. SESSION STATE ---
if 'authenticated' not in st.session_state: st.session_state.authenticated = False
if 'master_data' not in st.session_state: st.session_state.master_data = None

# --- 3. SIDEBAR ---
with st.sidebar:
    st.title("🛡️ 91362 Audit Readiness")
    if not st.session_state.authenticated:
        u, p = st.text_input("User ID"), st.text_input("Access Key", type="password")
        if st.button("Authorize"):
            if u: st.session_state.user_name, st.session_state.authenticated = u, True; st.rerun()
    else:
        st.success(f"Verified Auditor: **{st.session_state.user_name}**")
        st.session_state.model_provider = st.radio("Intelligence Engine:", ["GPT-4o (OpenAI)", "Claude 3.5 (Anthropic)"])
        if st.button("Logout"): st.session_state.authenticated = False; st.rerun()

# --- 4. MAIN INTERFACE ---
if not st.session_state.authenticated:
    st.info("🔐 Authorized Access Only: 21 CFR Part 11 Audit Suite")
else:
    st.subheader("📂 Step 1: Upload SOP/URS for " "Virtual 483" " Scanning")
    uploaded_file = st.file_uploader("Upload PDF", type="pdf")

    if uploaded_file and st.button("🚀 Execute Critical-Check Audit"):
        active_key = st.secrets.get("OPENAI_API_KEY") if "OpenAI" in st.session_state.model_provider else st.secrets.get("ANTHROPIC_API_KEY")
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(uploaded_file.getvalue()); tmp_path = tmp.name
        
        try:
            loader = PyPDFLoader(tmp_path)
            full_text = " ".join([p.page_content for p in loader.load()])
            
            with st.spinner("Scanning for Data Integrity Vulnerabilities..."):
                prompt = (
                    f"Act as a GxP Senior Auditor. Analyze: {full_text[:8000]}. "
                    f"1. Extract 5 key URS items and link to FS and OQ. "
                    f"2. FLAG MISSING CONTROLS for: "
                    f"   - §11.10(e) Audit Trails: Does it log 'Who, What, When, Why'? "
                    f"   - §11.10(g) Authority Checks: Are role-based permissions explicit? "
                    f"   - §11.50 Signature Manifestations: Is the 'Meaning' (Approval/Review) required? "
                    f"3. For each requirement, assign an ALCOA+ Score (1-10) based on how 'Attributable' and 'Accurate' the spec is. "
                    f"Return ONLY pipe-separated: URS_ID | URS_Text | FS_ID | FS_Spec | OQ_ID | OQ_Test | Part11_Ref | ALCOA_Score | Auditor_Observation."
                )
                
                model_map = {"GPT-4o (OpenAI)": "openai/gpt-4o", "Claude 3.5 (Anthropic)": "anthropic/claude-3-5-sonnet-20240620"}
                res = completion(model=model_map[st.session_state.model_provider], messages=[{"role": "user", "content": prompt}], api_key=active_key)
                
                raw_rows = [ [i.strip() for i in l.split('|')] for l in res.choices[0].message.content.strip().split('\n') if '|' in l ]
                st.session_state.master_data = raw_rows
                st.success("Analysis Complete.")

        except Exception as e: st.error(f"Error: {e}")
        finally: 
            if os.path.exists(tmp_path): os.remove(tmp_path)

    # --- 5. DASHBOARD & EXPORT ---
    if st.session_state.master_data:
        df = pd.DataFrame(st.session_state.master_data, columns=["URS_ID", "Requirement", "FS_ID", "FS_Spec", "OQ_ID", "OQ_Test", "Part11_Ref", "ALCOA_Score", "Observation"])
        
        # High Risk Filter (ALCOA Score < 7)
        high_risk = df[df['ALCOA_Score'].astype(float) < 7.0]
        
        st.divider()
        st.subheader("🚨 Data Integrity Risk Report (ALCOA+ Scale)")
        if not high_risk.empty:
            st.error(f"Critical Findings: {len(high_risk)} requirements have low integrity scores.")
            st.dataframe(high_risk[['URS_ID', 'Part11_Ref', 'ALCOA_Score', 'Observation']], use_container_width=True)
        else:
            st.success("SOP/URS meets basic integrity standards (All ALCOA Scores > 7).")

        st.subheader("🛠️ Step 2: Full Traceability Review")
        st.data_editor(df, use_container_width=True)

        if st.button("💾 Export Verified Audit Package"):
            output = io.BytesIO()
            sig_block = pd.DataFrame([{
                "Approver": st.session_state.user_name,
                "Site": "91362 - Thousand Oaks Hub",
                "Meaning": "Verification of Traceability and Part 11 Controls",
                "Date": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }])
            
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Traceability_Matrix')
                sig_block.to_excel(writer, index=False, startrow=len(df)+3, sheet_name='Traceability_Matrix')
                
                # Extracting specific Gaps for a separate tab
                gaps = df[df['URS_ID'] == 'PART11_GAP']
                gaps.to_excel(writer, index=False, sheet_name='Compliance_Gaps')

            st.download_button("📥 Download Signed Workbook", data=output.getvalue(), file_name=f"GxP_Audit_{datetime.date.today()}.xlsx")