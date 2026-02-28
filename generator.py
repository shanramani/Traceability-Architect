import os
import datetime
import pandas as pd # Ensure you ran: pip install pandas openpyxl
from dotenv import load_dotenv
from litellm import completion

load_dotenv()

def generate_validation_suite(req_id, requirement):
    """
    Commercial Grade Generator: 
    Uses Groq for logic and Gemini 2.5 for Regulatory Formatting.
    """
    print(f"\n[AUDIT TRAIL] Event: Generation Started | ID: {req_id}")
    
    try:
        # Step 1: Brainstorming (Groq - Llama 3.3)
        print("[PROCESS] Step 1: Extracting Technical Controls via Groq...")
        res_groq = completion(
            model="groq/llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": f"Provide 3 validation test steps for: {requirement}"}]
        )
        tech_steps = res_groq.choices[0].message.content

        # Step 2: Formal Mapping (Gemini 2.5 Flash)
        print("[PROCESS] Step 2: Formatting to GxP Standards via Gemini 2.5...")
        prompt = (f"Act as a CSV formatter. Create a table with 'Step_ID', 'Action', and 'Expected_Result' "
                  f"based on these steps: {tech_steps}. Return ONLY the table.")
        
        res_gemini = completion(
            model="gemini/gemini-2.5-flash", # Corrected to 2.5 Stable
            messages=[{"role": "user", "content": prompt}]
        )
        
        return res_gemini.choices[0].message.content

    except Exception as e:
        return f"System Error: {str(e)}"

def export_to_biotech_excel(req_id, requirement, ai_table):
    """
    Exports to a validated-style Excel Traceability Matrix.
    """
    # Create the data structure for the RTM (Requirements Traceability Matrix)
    rtm_data = {
        "Trace_ID": [f"TM-{req_id}-01"],
        "User_Requirement": [requirement],
        "Test_Protocol": ["Integrated AI-Generated Suite"],
        "Risk_Category": ["High (GxP Critical)"], 
        "Model_Used": ["Gemini 2.5 Flash"],
        "Timestamp": [datetime.datetime.now().strftime("%Y-%m-%d %H:%M")]
    }
    
    df = pd.DataFrame(rtm_data)
    filename = f"Traceability_Matrix_{req_id}.xlsx"
    
    # Save with professional formatting
    df.to_excel(filename, index=False)
    print(f"[COMPLIANCE] Artifact successfully exported to: {filename}")

# --- EXECUTION ---
if __name__ == "__main__":
    REQ_ID = "REQ-102"
    REQ_VAL = "The system must encrypt all PHI at rest to meet HIPAA and 21 CFR Part 11 standards."
    
    output = generate_validation_suite(REQ_ID, REQ_VAL)
    print("\n--- AI GENERATED VALIDATION TABLE ---")
    print(output)
    
    export_to_biotech_excel(REQ_ID, REQ_VAL, output)