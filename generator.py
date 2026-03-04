import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from litellm import completion
import streamlit as st
import io

# --- 1. CONFIG & KEYS ---
load_dotenv()
st.set_page_config(page_title="Traceability Architect Pro", page_icon="🧪", layout="wide")

# Load keys from Secrets (Cloud) or Env (Local)
for key in ["GROQ_API_KEY", "GEMINI_API_KEY", "OPENAI_API_KEY"]:
    val = st.secrets.get(key) or os.getenv(key)
    if val: os.environ[key] = val

# --- 2. THE ENGINE ---
def generate_matrix(urs_items):
    results = []
    progress_bar = st.progress(0)
    
    for i, item in enumerate(urs_items):
        req_id, req_text = item['id'], item['text']
        
        try:
            # Step 1: Brainstorming (Llama 3.3 via Groq)
            res_groq = completion(
                model="groq/llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": f"Draft 2 functional specs for: {req_text}"}]
            )
            logic = res_groq.choices[0].message.content

            # Step 2: GxP Formatting (Gemini 2.0 Flash)
            prompt = (f"Act as GAMP 5 Lead. Req: '{req_text}'. Logic: {logic}. "
                      f"Return: Functional_Requirement | Test_Steps | Risk(H/M/L)")
            
            res_gemini = completion(
                model="gemini/gemini-2.0-flash",
                messages=[{"role": "user", "content": prompt}]
            )
            
            parts = res_gemini.choices[0].message.content.split('|')
            
            results.append({
                "Requirement_ID": req_id,
                "User_Requirement": req_text,
                "Functional_Spec": parts[0].strip() if len(parts)>0 else "Error",
                "Test_Steps": parts[1].strip() if len(parts)>1 else "Error",
                "Risk_Level": parts[2].strip() if len(parts)>2 else "Low",
                "Verified_By_Human": False  # Commercial Feature: Status Tracking
            })
        except Exception as e:
            st.error(f"Error on {req_id}: {e}")
        
        progress_bar.progress((i + 1) / len(urs_items))
    return results

# --- 3. COMMERCIAL UI ---
st.title("🧪 Traceability Architect Pro")
st.caption("Commercial Version 6.0 | GAMP 5 & ALCOA+ Aligned")

# Sample Data Setup
if 'master_df' not in st.session_state:
    st.session_state.master_df = None

with st.expander("📝 1. Configure User Requirements", expanded=True):
    urs_input = [
        {"id": "URS-SEC-01", "text": "Encrypt PHI data at rest via AES-256."},
        {"id": "URS-AUD-02", "text": "Maintain immutable audit trail for all records."},
        {"id": "URS-SIG-03", "text": "Support 21 CFR Part 11 Electronic Signatures."}
    ]
    st.info("Currently processing 3 standard Bio-Pharma requirements.")

if st.button("🚀 Generate Draft Traceability Matrix"):
    with st.spinner("Orchestrating AI Validation Engines..."):
        data = generate_matrix(urs_input)
        st.session_state.master_df = pd.DataFrame(data)

# --- 4. THE INTERACTIVE WORKBENCH ---
if st.session_state.master_df is not None:
    st.divider()
    st.subheader("🛠️ 2. Review & Verify (Human-in-the-Loop)")
    st.markdown("Edit any cell below to refine the AI's output. Check the **Verified_By_Human** box for compliance.")

    # This is the "Commercial" feature: Editable UI
    edited_df = st.data_editor(
        st.session_state.master_df,
        column_config={
            "Verified_By_Human": st.column_config.CheckboxColumn("Verify Status", default=False),
            "Risk_Level": st.column_config.SelectboxColumn("Risk", options=["High", "Med", "Low"])
        },
        use_container_width=True,
        num_rows="dynamic"
    )

    # Export Logic
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Save Progress"):
            st.session_state.master_df = edited_df
            st.success("Changes saved to session memory.")

    with col2:
        # Professional Excel Export
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            edited_df.to_excel(writer, index=False, sheet_name='RTM_Final')
        
        st.download_button(
            label="📥 Download Validated RTM (Excel)",
            data=output.getvalue(),
            file_name=f"RTM_Export_{datetime.date.today()}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )