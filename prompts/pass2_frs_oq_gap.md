{context_block}STRUCTURED URS TABLE (extracted in Pass 1):
{urs_csv}

{system_guidance}

CRITICAL RULE FOR FRS DESCRIPTIONS — READ CAREFULLY:
The FRS Requirement_Description MUST NEVER be a copy or light paraphrase of the URS text.
A URS describes WHAT the user needs (business language).
An FRS describes HOW the system implements it (engineering/technical language).

TRANSFORMATION EXAMPLES:
  URS: "The system must record sample identifiers for all tests."
  FRS: "The Sample Registration screen shall provide a mandatory alphanumeric Sample_ID
        field (max 50 chars). On Save, the system shall validate uniqueness against the
        sample master table and reject duplicates with error code ERR-SAM-001."

  URS: "Users must be able to search for batch records."
  FRS: "The Batch Record Search module shall expose filter criteria: Batch_Number
        (wildcard), Product_Code (dropdown), Date_Range (date-picker), and Status
        (multi-select). Results shall be paginated (25 rows/page) and sortable by
        any column. Search response time shall be ≤ 3 seconds for up to 10,000 records."

  URS: "The system shall support electronic signatures."
  FRS: "The e-Signature widget shall capture Signer_ID, Password (masked), Meaning
        (dropdown: Approved / Reviewed / Verified), and Timestamp (UTC, system-generated).
        Signature records shall be stored in the audit trail table as immutable entries
        per 21 CFR Part 11 §11.50 and shall not be editable or deletable by any user role."

Apply this transformation to EVERY URS row. If no system guide is provided, use the
inferred system type to determine appropriate field names, module names, and technical
constraints.

MANDATORY COMPLETENESS RULE:
You MUST generate exactly one FRS row for EVERY row in the URS table above.
Count the URS data rows (excluding the header). Your FRS dataset MUST have that exact count.
Do NOT skip, merge, or omit any URS requirement for any reason.
If a requirement is vague, still generate an FRS — note the vagueness in the description
and set Confidence < 0.70.
Number FRS IDs sequentially: FRS-001, FRS-002, FRS-003 ... one per URS row in order.

TASK: Generate exactly 3 CSV datasets separated by |||.
Output ONLY raw CSV rows — include the header row in EVERY dataset.
Wrap any comma-containing value in double-quotes.
Use N/A (not blank) for any field that is not applicable.

NOTE: Do NOT generate a Traceability dataset. Traceability is computed by the
application after your output is validated. Your job is FRS, OQ, and Gap only.

Dataset 1 (FRS): ID,Requirement_Description,Priority,Risk,GxP_Impact,Source_URS_Ref,Source_Text,Source_Page,Source_Section,Confidence,Confidence_Flag
  - ID: short code only — FRS-001, FRS-002, FRS-003. Max 7 characters. NEVER a sentence.
  - Requirement_Description: engineering implementation detail (single line, use semicolons
    instead of newlines, wrap in quotes if it contains commas).
  - Priority: Critical / High / Medium / Low
  - Risk: High / Medium / Low
    • High   = patient safety, data integrity, electronic records, audit trail
    • Medium = indirect quality, workflow, access control
    • Low    = cosmetic, reporting, preference
  - GxP_Impact: Direct / Indirect / None
  - Source_URS_Ref: URS Req_ID (e.g. URS-004)
  - Source_Text: exact quoted source text from URS (single line, max 100 chars)
  - Source_Page: e.g. Page 3
  - Source_Section: exact section heading/title from the System User Guide if used
    (e.g. "8.3 Login to DocuSign"); write "URS-derived" if no guide was uploaded.
  - Confidence: decimal 0.00–1.00
  - Confidence_Flag: write exactly "Review Required" if Confidence < 0.70, else leave blank

Dataset 2 (OQ): Test_ID,Requirement_Link,Requirement_Link_Type,Test_Type,Test_Step,Expected_Result,Pass_Fail_Criteria,Suggested_Evidence,Source,Confidence,Confidence_Flag
  - Test_ID: OQ-001, OQ-002 etc.
  - Requirement_Link: FRS-NNN (e.g. FRS-001)
  - Requirement_Link_Type: FRS
  - Test_Type: one of — Functional / Security / Data_Integrity / Negative_Test / Performance

  CRITICAL TEST_STEP RULES — apply in priority order:

  RULE A — Infrastructure/non-functional (availability, uptime, SLA, failover,
  high-availability, disaster recovery, scalability, performance under load):
    Write a TECHNICAL VERIFICATION PROCEDURE — no UI steps. E.g.:
    "Simulate primary node failure; measure time to failover; verify system available within RTO."
    Test_Type = Performance. Confidence ≤ 0.70.

  RULE B — Application feature WITH guide coverage:
    Use exact screen/field/button names from the guide.

  RULE C — Application feature WITHOUT guide coverage (or guide does not cover this screen):
    Prefix with [SCREEN UNVERIFIED] and write functionally correct but generic steps.
    Set Confidence = 0.60 so it auto-flags for human verification.
    NEVER invent specific menu paths not found in the guide.

  - Test_Step: apply rules above. Single line, semicolons between steps.
  - Expected_Result: single line outcome
  - Pass_Fail_Criteria: single line pass condition
  - Suggested_Evidence: specific artifact an FDA auditor requires — exact screen, log,
    config record, or error message. E.g. "Screenshot of 'Error: Password too short'
    from Login screen AND Security Policy export showing min_length=8."
  - Source: "Derived from URS-NNN"
  - Confidence: 0.00–1.00. Cap at 0.60 for RULE C. Cap at 0.70 for RULE A.
  - Confidence_Flag: "Review Required" if Confidence < 0.70, else blank
  - Rule: High-Risk FRS → ≥3 OQ rows. Medium → ≥2. Low → ≥1.

Dataset 3 (Gap_Analysis): Req_ID,Gap_Type,Description,Recommendation,Severity
  - Gap_Type: Untestable / No_Test_Coverage / Orphan_Test / Ambiguous / Duplicate /
    Non_Functional / Missing_Test
  - Only include rows where a gap exists in the URS requirements themselves.
  - Severity: Critical / High / Medium / Low
  - Description and Recommendation: single line each

CRITICAL OUTPUT RULES — MUST FOLLOW OR THE FILE WILL BE CORRUPT:
1. Every field value MUST fit on a single line — NO embedded newlines inside any value.
2. Commas inside a value MUST be wrapped in double-quotes: "value, with, commas"
3. The ONLY dataset separator is the exact token:  |||  (on its own line, nothing else).
4. Do NOT add extra ||| tokens inside dataset content.
5. FRS ID column must be a SHORT CODE ONLY: FRS-001, FRS-002, FRS-003 etc.
   The ID is NEVER a sentence. If you are writing more than 8 characters in the ID
   column, you are putting the description in the wrong column.

|||  ← this token on its own line separates each dataset
