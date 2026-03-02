import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from litellm import completion

# 1. Load your secret keys
load_dotenv()

def process_urs_list(urs_items):
    """
    Processes a list of URS requirements and returns a structured list for Excel.
    """
    results_for_excel = []

    for item in urs_items:
        req_id = item['id']
        req_text = item['text']
        
        print(f"\n[AUDIT] Processing {req_id}...")

        try:
            # Step 1: Technical Brainstorming (Groq)
            print(f"[LOG] Step 1: Engineering Technical Controls for {req_id}...")
            res_groq = completion(
                model="groq/llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": f"Provide 2 functional specs and 2 test steps for: {req_text}"}]
            )
            ai_logic = res_groq.choices[0].message.content

            # Step 2: GxP Formatting & Risk Assessment (Gemini 2.5 Flash)
            print(f"[LOG] Step 2: Mapping to GxP Traceability Matrix...")
            prompt = (f"Act as a GAMP 5 Validation Lead. Analyze: '{req_text}'. Logic: {ai_logic}. "
                      f"Return exactly 3 values separated by a pipe '|': "
                      f"Functional_Requirement | Test_Steps | Risk_Level(High/Med/Low)")
            
            res_gemini = completion(
                model="gemini/gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parsing the AI response
            parts = res_gemini.choices[0].message.content.split('|')
            frs = parts[0].strip()
            tests = parts[1].strip()
            risk = parts[2].strip()

            # Append to our master list
            results_for_excel.append({
                "Requirement ID": req_id,
                "User Requirement": req_text,
                "Functional Spec (FRS)": frs,
                "Test Steps": tests,
                "Risk Level": risk,
                "System Status": "Validated-Draft",
                "Model Version": "Gemini 2.5 Flash",
                "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            })

        except Exception as e:
            print(f"[ERROR] Failed on {req_id}: {e}")

    return results_for_excel

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    # Your 'Complex URS' input - Professional Syntax
    my_urs = [
        {"id": "URS-SEC-01", "text": "The system SHALL encrypt all PHI data at rest using AES-256."},
        {"id": "URS-COM-02", "text": "The system SHALL maintain an uneditable audit trail of all record changes."},
        {"id": "URS-FUN-03", "text": "The system SHOULD allow users to generate PDF reports of lab results."}
    ]

    # Run the engine
    final_data = process_urs_list(my_urs)

    # Export to Excel
    df = pd.DataFrame(final_data)
    output_file = "Commercial_Traceability_Matrix.xlsx"
    df.to_excel(output_file, index=False)
    
    print(f"\n[SUCCESS] Commercial Demo File '{output_file}' is ready!")