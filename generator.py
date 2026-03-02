import os
import datetime
import pandas as pd
from dotenv import load_dotenv
from litellm import completion

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
            # Step 1: Brainstorming (Groq)
            res_groq = completion(
                model="groq/llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": f"Provide 2 functional specs and 2 test steps for: {req_text}"}]
            )
            ai_logic = res_groq.choices[0].message.content

            # Step 2: Formatting & Risk Assessment (Gemini 2.5 Flash)
            prompt = (f"Analyze this requirement: '{req_text}'. Logic: {ai_logic}. "
                      f"Return exactly 3 values separated by '|': "
                      f"Functional_Requirement | Test_Steps | Risk_Level(High/Med/Low)")
            
            res_gemini = completion(
                model="gemini/gemini-2.5-flash",
                messages=[{"role": "user", "content": prompt}]
            )
            
            # Parsing the AI response
            frs, tests, risk = res_gemini.choices[0].message.content.split('|')

            # Append to our master list
            results_for_excel.append({
                "Requirement ID": req_id,
                "User Requirement": req_text,
                "Functional Spec (FRS)": frs.strip(),
                "Test Steps": tests.strip(),
                "Risk Level": risk.strip(),
                "System Status": "Validated-Draft",
                "Timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
            })

        except Exception as e:
            print(f"[ERROR] Failed on {req_id}: {e}")

    return results_for_excel

# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    # This is your 'Complex URS' input
    my_urs = [
        {"id": "URS-SEC-01", "text": "The system SHALL encrypt all PHI data at rest using AES-256."},
        {"id": "URS-COM-02", "text": "The system SHALL maintain an uneditable audit trail of all record changes."},
        {"id": "URS-FUN-03", "text": "The system SHOULD allow users to generate PDF reports of lab results."}
    ]

    # Run the engine
    final_data = process_urs_list(my_urs)

    # Export to Excel
    df = pd.DataFrame(final_data)
    df.to_excel("Commercial_Traceability_Matrix.xlsx", index=False)
    print("\n[SUCCESS] Commercial Demo File 'Commercial_Traceability_Matrix.xlsx' is ready!")