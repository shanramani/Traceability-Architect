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

REASONING RULES — apply carefully before classifying each row:

RULE 1 — RETIREMENT / REMOVAL:
If the Change Spec retires a URS requirement, removes a feature, or discontinues a
functional area, ALL FRS rows derived from that URS/feature must be marked Obsolete.
Do not mark them Needs_Review — if the requirement is gone, the FRS is gone.

RULE 2 — MODIFICATION:
If the Change Spec modifies how something works (changes a protocol, replaces a
component, changes a configuration value), the FRS rows describing the OLD behaviour
must be marked Must_Update with explicit Action_Required describing the delta.

RULE 3 — NET-NEW:
If the Change Spec introduces something that has NO equivalent in the existing FRS,
create a New_Required row with FRS_ID = "NEW" and describe what the new FRS must cover.

RULE 4 — INDIRECT IMPACT (think carefully):
If a change removes something that other requirements DEPENDED ON (e.g. a hardware
interface, an ID generation mechanism, an integration), ask: "Does any other existing
FRS row assume the old behaviour?" If yes, mark it Needs_Review even if the Change
Spec doesn't mention it explicitly.
Example: If a change introduces auto-generated unique IDs, any FRS about duplicate
prevention should be Needs_Review — the duplicate check may now be redundant.

RULE 5 — OQ IMPACT (via traceability):
For OQ rows: the system will automatically propagate FRS impact to linked OQ via the
traceability matrix. You only need to flag OQ rows where the Change Spec EXPLICITLY
mentions a test scenario, or where your semantic reasoning identifies a direct impact
that the trace matrix would miss (e.g. OQ-LAB-04 testing duplicate prevention when
auto-GUID makes that test potentially redundant).

Dataset 1 (FRS_Impact): FRS_ID,Change_Driver,Impact_Status,Confidence_Level,Risk_Category,Rationale,Action_Required
- FRS_ID: the existing FRS row ID (e.g. FRS-007). Only include rows that are impacted.
  Use "NEW" for net-new requirements.
- Change_Driver: the CHG-NNN ID that causes this impact
- Impact_Status: one of —
    Must_Update   — directly contradicted or made incorrect by this change
    Needs_Review  — possibly affected, human must verify
    Obsolete      — feature/requirement removed; this FRS row should be retired
    New_Required  — net-new functionality with no existing FRS row (use FRS_ID = "NEW")
- Confidence_Level:
    High   — direct, unambiguous match between the change and this FRS row
    Medium — clear semantic relationship but requires engineering judgement
    Low    — weak or inferred relationship, human must verify
- Risk_Category:
    GxP_Critical    — patient safety, electronic records, audit trail, e-signatures
    Data_Integrity  — data accuracy, completeness, consistency, retention (ALCOA+)
    Business        — operational workflows, non-GxP functionality
    Cosmetic        — UI labels, formatting, display only
- Rationale: one sentence explaining WHY this row is impacted
- Action_Required: specific instruction (e.g. "Replace COM port reference with TCP/IP
  address and port fields; add encryption protocol field per new security requirement")

Dataset 2 (OQ_Impact): OQ_ID,Change_Driver,Impact_Status,Confidence_Level,Risk_Category,Rationale,Action_Required
- OQ_ID: the existing OQ test ID. Use "NEW" for net-new tests needed.
- Focus on OQ rows where your semantic reasoning finds impact beyond trace propagation.
- Impact_Status, Confidence_Level, Risk_Category: same rules as FRS
- Rationale and Action_Required: same as above

IMPORTANT: Only include rows that are actually impacted. Do NOT list Unaffected rows.
|||
