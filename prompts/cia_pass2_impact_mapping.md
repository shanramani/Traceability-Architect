CHANGE TABLE (extracted in Pass 1):
{chg_csv}

EXISTING APPROVED FRS (first 4000 chars):
{frs_text}

EXISTING OQ TEST CASES (first 80 rows):
{oq_summary}

EXISTING TRACEABILITY MATRIX (first 80 rows):
{trc_summary}

TASK: For each change in the Change Table, identify ALL impacted rows in the
FRS and OQ. Output TWO datasets separated by |||

Dataset 1 (FRS_Impact): FRS_ID,Change_Driver,Impact_Status,Confidence_Level,Risk_Category,Rationale,Action_Required
- FRS_ID: the existing FRS row ID (e.g. FRS-007). Only include rows that are impacted.
- Change_Driver: the CHG-NNN ID that causes this impact
- Impact_Status: one of —
    Must_Update   — directly contradicted or made incorrect by this change
    Needs_Review  — possibly affected, human must verify
    Obsolete      — feature removed; this FRS row should be retired
    New_Required  — net-new functionality with no existing FRS row (use FRS_ID = "NEW")
- Confidence_Level: your confidence that this impact assessment is correct —
    High   — direct, unambiguous match between the change and this FRS row
             (e.g. change says "password min 8 to 12", FRS says "min password length = 8")
    Medium — clear semantic relationship but requires engineering judgement to confirm
             (e.g. change affects authentication module, FRS covers login behaviour)
    Low    — weak or inferred relationship, human must verify before acting
             (e.g. change may have downstream effects on this requirement)
- Risk_Category: CSA-aligned risk classification —
    GxP_Critical    — directly impacts patient safety, electronic records, audit trail,
                      electronic signatures, or regulatory submissions
    Data_Integrity  — impacts data accuracy, completeness, consistency, or retention
                      (ALCOA+ principles)
    Business        — impacts operational workflows, efficiency, or non-GxP functionality
    Cosmetic        — UI labels, formatting, display only, no functional impact
- Rationale: one sentence explaining why this row is impacted
- Action_Required: specific instruction for the validation engineer
  (e.g. "Update password length criterion from 8 to 12 characters in requirement description")

Dataset 2 (OQ_Impact): OQ_ID,Change_Driver,Impact_Status,Confidence_Level,Risk_Category,Rationale,Action_Required
- OQ_ID: the existing OQ test ID (e.g. OQ-012). Use "NEW" for net-new tests needed.
- Use the Traceability Matrix to propagate impact: if a FRS row is Must_Update,
  ALL OQ tests linked to that FRS row via the trace matrix are also Must_Update
  unless you have specific reason to exclude one.
- Impact_Status: same values as above
- Confidence_Level: same rules as above
- Risk_Category: same values as above — inherit from the linked FRS row where relationship is clear
- Rationale and Action_Required: same as above

IMPORTANT: Only include rows that are actually impacted. Do NOT list Unaffected rows.
|||
