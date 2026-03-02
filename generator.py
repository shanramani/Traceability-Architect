import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from litellm import completion
import streamlit as st

# Load local .env if it exists
load_dotenv()

# --- UNIVERSAL SECRET BRIDGE ---
LLM_KEYS = {
    "OPENAI_API_KEY": "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY": "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY": "GEMINI_API_KEY",
    "GROQ_API_KEY": "GROQ_API_KEY"
}

def load_all_keys():
    for env_name, secret_name in LLM_KEYS.items():
        key_value = st.secrets.get(secret_name) or os.getenv(env_name)
        if key_value:
            os.environ[env_name] = key_value
            if "GEMINI" in env_name:
                os.environ["GOOGLE_API_KEY"] = key_value

load_all_keys()

# --- VALIDATION ENGINE ---
def process_urs_list(urs_items):
    results_for_excel = []
    progress_bar = st.progress(0)
    
    for i, item in enumerate(urs_items):
        req_id = item['id']
        req_text = item['text']
        
        st.write(f"🔍 Analyzing **{req_id}**...")

        try:
            # Step 1: Technical Brainstorming (Groq)
            res_groq = completion(
                model="groq/llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": f"Provide 2 functional specs and 2 test steps for: {req_text}"}]
            )
            ai_logic = res_groq.choices[0].message.content

            # Step 2: GxP Formatting (Gemini)
            prompt = (f"Act as a GAMP 5 Validation Lead. Analyze: '{req_text}'. Logic: {ai_logic}. "
                      f"Return exactly 3 values separated by a pipe '|': "
                      f"Functional_Requirement | Test_Steps | Risk_Level(High/Med/Low)")
            
            res_gemini = completion(
                model="gemini/gemini-2.0-flash", # Note: Gemini 2.0 is currently the stable high-speed flash
                messages=[{"role": "user", "content": prompt}]
            )
            
            parts = res_gemini.choices[0].message.content.split('|')
            
            results_for_excel.append({
                "Requirement ID": req_id,
                "User Requirement": req_text,
                "Functional Spec (FRS)": parts[0].strip(),
                "Test Steps": parts[1].strip(),
                "Risk Level": parts[2].strip(),
                "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            })

        except Exception as e:
            st.error(f"Error on {req_id}: {e}")
        
        progress_bar.progress((i + 1) / len(urs_items))

    return results_for_excel

# --- STREAMLIT UI ---
st.set_page_config(page_title="Traceability Architect", page_icon="🧪")
st.title("🧪 Traceability Architect")
st.subheader("GAMP 5 Automated Validation Engine")

# Sample URS Input Area
st.write("### 1. Define User Requirements")
urs_input = [
    {"id": "URS-SEC-01", "text": "The system SHALL encrypt all PHI data at rest using AES-256."},
    {"id": "URS-COM-02", "text": "The system SHALL maintain an uneditable audit trail of all record changes."},
    {"id": "URS-FUN-03", "text": "The system SHOULD allow users to generate PDF reports of lab results."}
]
st.json(urs_input)

if st.button("Generate Traceability Matrix"):
    with st.spinner("Orchestrating AI Models..."):
        final_data = process_urs_list(urs_input)
        df = pd.DataFrame(final_data)
        
        # Display Preview
        st.success("✅ Matrix Generated!")
        st.dataframe(df)

        # Download Button
        output_file = "Traceability_Matrix.xlsx"
        df.to_excel(output_file, index=False)
        with open(output_file, "rb") as file:
            st.download_button(
                label="📥 Download Excel RTM",
                data=file,
                file_name="Commercial_Traceability_Matrix.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

st.divider()
st.caption("Powered by Groq LPU™ and Google Gemini | GxP Compliant Design")