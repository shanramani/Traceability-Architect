import os
from dotenv import load_dotenv
from litellm import completion

# 1. Load your secret keys from the .env file
load_dotenv()

def generate_validation_script(requirement):
    print(f"\n📝 Processing Requirement: {requirement}")
    
    # 2. Use Groq (Fast) to brainstorm the "Test Steps"
    print("🚀 Step 1: Brainstorming steps with Groq...")
    response_groq = completion(
        model="groq/llama3-70b-8192", 
        messages=[{"role": "user", "content": f"List 3 technical test steps for this pharma software requirement: {requirement}"}]
    )
    steps = response_groq.choices[0].message.content

    # 3. Use ChatGPT (Precise) to format it into a GxP-compliant table
    print("✍️  Step 2: Formatting into GxP Template with ChatGPT...")
    prompt_gpt = f"""
    Convert these steps into a formal Validation Script table with columns: 
    Step # | Test Procedure | Expected Result | Pass/Fail.
    
    Steps: {steps}
    """
    
    response_gpt = completion(
        model="gpt-4o", 
        messages=[{"role": "user", "content": prompt_gpt}]
    )
    
    return response_gpt.choices[0].message.content

# --- TEST RUN ---
if __name__ == "__main__":
    my_requirement = "The system must require a 21 CFR Part 11 compliant digital signature before deleting any record."
    final_script = generate_validation_script(my_requirement)
    
    print("\n--- FINAL VALIDATION ARTIFACT ---")
    print(final_script)
