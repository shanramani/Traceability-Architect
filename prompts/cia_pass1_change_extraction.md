CHANGE SPECIFICATION DOCUMENT:
{change_spec_text}

TASK: Extract every discrete change from this document into a single CSV.
Output ONLY the CSV — include the header row.

CSV columns:
Change_ID,Change_Type,Affected_Area,Description,Impact_Scope

Rules:
- Change_ID: sequential CHG-NNN (e.g. CHG-001)
- Change_Type: one of —
    New_Feature         (brand new capability added)
    Modified_Behaviour  (existing behaviour changed)
    Removed_Feature     (existing capability removed)
    Config_Change       (configuration/parameter change)
    Performance_Change  (SLA, response time, capacity change)
    Security_Change     (auth, access control, encryption change)
    Bug_Fix             (defect corrected, may affect existing tests)
- Affected_Area: which functional area or module is affected
  (e.g. "Authentication", "Audit Trail", "Reporting")
- Description: one sentence, precise description of what changed
- Impact_Scope: Critical / High / Medium / Low
    Critical = patient safety, electronic records, audit trail, e-signatures
    High     = data integrity, access control, core workflows
    Medium   = secondary features, notifications, UI changes
    Low      = cosmetic, labels, minor config
