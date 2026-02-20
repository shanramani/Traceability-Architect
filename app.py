# --- 3. PROCESSING ---
uploaded_file = st.file_uploader("Upload URS PDF", type="pdf")

if uploaded_file:
    # This creates the temporary file so the PDF loader can read it
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        tmp_path = tmp_file.name

    if st.button("🚀 Generate Validation Suite"):
        with st.spinner("Executing GAMP 5 Logic..."):
            loader = PyPDFLoader(tmp_path)
            pages = loader.load()
            full_text = "\n".join([p.page_content for p in pages])
            llm = get_llm()
            
            master_prompt = f"""
            You are a Senior CSV Consultant. Generate three sections for {proj_name}.
            
            SECTION 1: FUNCTIONAL REQUIREMENTS SPECIFICATION (FRS)
            Detailed technical description of design features.
            
            SECTION 2: OPERATIONAL QUALIFICATION (OQ) PROTOCOL
            Test instructions with 'Step', 'Expected Result', and 'Pass/Fail' column.
            
            SECTION 3: TRACEABILITY MATRIX (RTM)
            Table mapping URS ID | Description | FRS Reference | Test ID.

            URS TEXT: {full_text[:12000]}
            Separate sections with '---SECTION_SPLIT---'.
            """
            response = llm.invoke(master_prompt)
            st.session_state.full_analysis = response.content
            
    # Clean up the temporary file after the button is pressed or ignored
    try:
        os.remove(tmp_path)
    except:
        pass
