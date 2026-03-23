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

Dataset 1 (FRS_Impact): FRS_ID,Change_Driver,Impact_Status,Rationale,Action_Required
- FRS_ID: the existing FRS row ID (e.g. FRS-007). Only include rows that are impacted.
- Change_Driver: the CHG-NNN ID that causes this impact
- Impact_Status: one of —
    Must_Update   — directly contradicted or made incorrect by this change
    Needs_Review  — possibly affected, human must verify
    Obsolete      — feature removed; this FRS row should be retired
    New_Required  — net-new functionality with no existing FRS row (use FRS_ID = "NEW")
- Rationale: one sentence explaining why this row is impacted
- Action_Required: specific instruction for the validation engineer
  (e.g. "Update password length criterion from 8 to 12 characters in requirement description")

Dataset 2 (OQ_Impact): OQ_ID,Change_Driver,Impact_Status,Rationale,Action_Required
- OQ_ID: the existing OQ test ID (e.g. OQ-012). Use "NEW" for net-new tests needed.
- Use the Traceability Matrix to propagate impact: if a FRS row is Must_Update,
  ALL OQ tests linked to that FRS row via the trace matrix are also Must_Update
  unless you have specific reason to exclude one.
- Impact_Status: same values as above
- Rationale and Action_Required: same as above

IMPORTANT: Only include rows that are actually impacted. Do NOT list Unaffected rows.
|||
