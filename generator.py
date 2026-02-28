import os
from dotenv import load_dotenv
from litellm import completion
import litellm
litellm.set_verbose = True  # This shows you the "Behind the Scenes" of the AI call

# 1. Load your secret keys from the .env file
load_dotenv()

def generate_validation_script(requirement):
    print(f"\n[LOG] Processing Requirement: {requirement}")
    
    try:
        # 1. Step 1 stays the same (Groq is working!)
        print("[LOG] Step 1: Brainstorming technical steps with Groq...")
        response_groq = completion(
            model="groq/llama-3.3-70b-versatile", 
            messages=[{"role": "user", "content": f"List 3 technical test steps for: {requirement}"}]
        )
        steps = response_groq.choices[0].message.content

        # 2. SWITCH Step 2 to Gemini (Google) instead of OpenAI
        print("[LOG] Step 2: Formatting into GxP Template with Gemini...")
        prompt_gpt = f"Convert these steps into a formal Validation Table (Step, Procedure, Expected Result): {steps}"
        
        response_gemini = completion(
            model="gemini/gemini-2.5-flash", # Using Gemini instead of GPT
            messages=[{"role": "user", "content": prompt_gpt}]
        )
        return response_gemini.choices[0].message.content

    except Exception as e:
        return f"[ERROR] AI Generation failed: {str(e)}"
# --- TEST RUN ---
if __name__ == "__main__":
    my_requirement = "The system must require a 21 CFR Part 11 compliant digital signature before deleting any record."
    final_script = generate_validation_script(my_requirement)
    
    print("\n--- FINAL VALIDATION ARTIFACT ---")
    print(final_script)
