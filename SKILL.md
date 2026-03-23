# SKILL: Validation Doc Assist — GxP Validation Package Generator

**Version:** 37.0  
**Prompt Version:** v19.0-esignature-test-type-r3c  
**Governed by:** 21 CFR Part 11, GAMP 5  
**Stack:** Streamlit · LiteLLM · SQLite · openpyxl · pdfplumber

---

## 1. WHAT THIS TOOL DOES

Converts a User Requirements Specification (URS) PDF into a complete, audit-ready
GxP validation package in under 5 minutes. Outputs a signed, SHA-256 hashed Excel
workbook containing:

| Sheet | Contents |
|---|---|
| Cover | Executive summary, KPIs, model metadata |
| URS_Extraction | Structured requirement table extracted from URS |
| FRS | Functional Requirements Specification (engineering language) |
| OQ | Operational Qualification test cases with evidence guidance |
| Traceability | End-to-end URS → FRS → OQ coverage map |
| Gap_Analysis | AI-detected gaps (LLM + 7 deterministic rules R0–R6+R3d) |
| Det_Validation | Deterministic rule results with rule codes |
| Signature | 21 CFR Part 11 §11.200 e-signature block, SHA-256 hash |

Also supports **Change Impact Analysis (CIA)** mode — see Section 6.

---

## 2. INPUTS

### New Validation Mode

| Input | Format | Required | Slot |
|---|---|---|---|
| URS | PDF (text-based, OCR-searchable) | ✅ Yes | Main panel uploader |
| System User Guide / SOP | PDF | Optional | Sidebar uploader |

**URS acceptance criteria (three-stage gate):**
- Stage 0: Minimum 300 chars extractable text, at least 1 page
- Stage 1: Heuristic — must score ≥3 positive URS signals; must contain ≥2 "shall"/"must" statements; must not trigger negative signals (SOP patterns, personal doc patterns)
- Stage 2: LLM preflight — binary YES/NO classification (only if Stage 1 passes)

**Sidebar (System Guide) acceptance criteria:**
- Runs URS mis-file check first — rejects docs containing "the system shall", URS-NNN IDs, or "REQ ID" headers with redirect message to main panel
- Then checks for SOP/personal document negative signals

### Change Impact Analysis Mode

| Input | Format | Required |
|---|---|---|
| Change Specification | PDF | ✅ Yes |
| Existing Approved FRS | PDF | ✅ Yes |
| Existing OQ Test Cases | .xlsx or .csv | ✅ Yes |
| Existing Traceability Matrix | .xlsx or .csv | ✅ Yes |

**Note:** No URS upload in CIA mode. The Traceability Matrix is the prerequisite —
it carries URS Req IDs already linked to FRS and OQ rows.

---

## 3. AI PIPELINE

### New Validation — Three-Pass Architecture

```
PDF → extract_pages() → chunks (CHUNK_SIZE=8 pages)
         │
         ▼
    PASS 1 (per chunk)
    prompt: pass1_urs_extraction.md
    output: URS CSV — Req_ID, Requirement_Description, Category,
                      Testable, Source_Text, Source_Page, Confidence
         │
         ▼  (chunks merged, deduped, renumbered URS-001…URS-NNN)
         │
    PASS 2 (batched, PASS2_CHUNK rows per call, default 40)
    prompt: pass2_frs_oq_gap.md
    output: 3 datasets separated by |||
            Dataset 1: FRS
            Dataset 2: OQ
            Dataset 3: Gap_Analysis (LLM-detected gaps only)
         │
         ▼  (optional, only if sys_context loaded)
    PASS 3 — Cross-source gap analysis
    prompt: build_cross_source_gap_prompt()
    output: additional Gap_Analysis rows from URS↔Guide comparison
         │
         ▼
    POST-PROCESSING (Python, deterministic, no LLM)
    - _renumber_frs_ids()     — sequential FRS-001…FRS-NNN
    - _renumber_oq_ids()      — sequential OQ-001…OQ-NNN
    - _remap_oq_links()       — fix OQ→FRS links after renumber
    - _build_traceability()   — compute Coverage_Status per URS row
    - run_deterministic_validation() — R0–R6 + R3d rules
```

### Change Impact Analysis — Two-Pass Architecture

```
Change Spec PDF + FRS PDF + OQ xlsx/csv + Trace xlsx/csv
         │
         ▼
    CIA PASS 1
    prompt: cia_pass1_change_extraction.md
    output: CHG-NNN table — Change_ID, Change_Type, Affected_Area,
                             Description, Impact_Scope
         │
         ▼
    CIA PASS 2
    prompt: cia_pass2_impact_mapping.md
    output: 2 datasets separated by |||
            Dataset 1: FRS_Impact
            Dataset 2: OQ_Impact
         │
         ▼
    TRACE PROPAGATION (Python, deterministic)
    - Build FRS→OQ lookup from trace matrix
    - Must_Update FRS rows auto-propagate to all linked OQ rows
    - Newly flagged OQ rows labelled "Trace-propagated"
```

---

## 4. PROMPT FILES

All prompts live in `./prompts/` and are loaded once at startup via `_load_prompt()`.
Variables are injected at call time using Python `.format()`.

| File | Used by | Key variables |
|---|---|---|
| `system_prompt.md` | All calls (system role) | — |
| `preflight_classifier.md` | `validate_urs_document()` Stage 2 | `{text}` |
| `pass1_urs_extraction.md` | `build_pass1_prompt()` | `{chunk_index}` `{total_chunks}` `{chunk_text}` |
| `pass2_frs_oq_gap.md` | `build_pass2_prompt()` | `{context_block}` `{urs_csv}` `{system_guidance}` |
| `cia_pass1_change_extraction.md` | `build_cia_pass1_prompt()` | `{change_spec_text}` |
| `cia_pass2_impact_mapping.md` | `build_cia_pass2_prompt()` | `{chg_csv}` `{frs_text}` `{oq_summary}` `{trc_summary}` |

**To edit a prompt:** open the relevant `.md` file, edit in plain text, restart the
Streamlit app. No Python changes required. Commit the `.md` file separately from
code changes so prompt iterations have their own git history.

**Fallback behaviour:** If a `.md` file is missing at startup, `_load_prompt()` logs
a warning and returns an empty string. The app continues running — but the affected
pipeline will produce empty output. Always verify all 6 files exist on deployment.

---

## 5. OUTPUT SCHEMAS

### FRS Columns
```
ID | Requirement_Description | Priority | Risk | GxP_Impact |
Source_URS_Ref | Source_Text | Source_Page | Source_Section |
Confidence | Confidence_Flag | AI_Review_Status
```
- `Source_Section`: exact guide heading if guide provided; "URS-derived" otherwise
- `Confidence_Flag`: "Review Required" if Confidence < 0.70

### OQ Columns
```
Test_ID | Requirement_Link | Requirement_Link_Type | Test_Type |
Test_Step | Expected_Result | Pass_Fail_Criteria | Suggested_Evidence |
Source | Confidence | Confidence_Flag | AI_Review_Status
```
- `Test_Step` rules: RULE A (infrastructure → technical procedure), RULE B (guide covers it → exact steps), RULE C (no guide coverage → `[SCREEN UNVERIFIED]` prefix, Confidence=0.60)
- `Suggested_Evidence`: specific FDA auditor artifact — never vague

### Gap_Analysis Columns
```
Req_ID | Gap_Type | Description | Recommendation | Severity | Rule | AI_Review_Status
```

**Valid Gap_Types:**
```
Untestable | No_Test_Coverage | Orphan_Test | Ambiguous | Duplicate |
Missing_FRS | Non_Functional | Missing_Test | Non_Testable_Requirement
```

### CIA FRS_Impact Columns
```
FRS_ID | Change_Driver | Impact_Status | Rationale | Action_Required
```

**Valid Impact_Status values:**
```
Must_Update | Needs_Review | Obsolete | New_Required
```

---

## 6. DETERMINISTIC RULES (R0–R6 + R3d)

These run entirely in Python after AI passes complete. No LLM involved.

| Rule | Name | What it detects | Severity |
|---|---|---|---|
| R0 | Missing FRS | URS req with no linked FRS row | High |
| R1 | Orphan Test | OQ test with no matching FRS | Medium |
| R2 | HITL Flag | Low-confidence FRS or OQ rows | Medium |
| R3 | Weak Req | Vague URS language (legacy) | Medium |
| R3b | Ambiguous | Ambiguous FRS descriptions | Medium |
| R3c | Non-Functional | Infrastructure/SLA reqs in FRS | Medium |
| R3d | Non-Testable | URS rows with weak verbs (25-term dict) | **High** |
| R4 | Coverage Count | High-risk FRS with <3 OQ tests | High |
| R5 | Duplicate | FRS rows with >0.80 Jaccard overlap | Low |
| R6 | Manual Action | OQ steps requiring human-only execution | Medium |

**R3d weak verb remediation dictionary** (25 terms):
`user-friendly, user friendly, easy, intuitive, fast, quickly, seamless, simple,
flexible, robust, scalable, reliable, convenient, smooth, modern, efficient,
should, may, appropriate, adequate, sufficient, ...`
Each maps to a concrete measurable remediation suggestion.

---

## 7. DOCUMENT TYPE DETECTION (`detect_tabular_doc_type`)

Used in CIA mode to fingerprint uploaded xlsx/csv files before accepting them.

**Detection priority:**
1. **Traceability** — has URS col + FRS col + Test_ID col in same sheet
2. **OQ** — has `test_step` or `expected_result` column, or ≥3 OQ-NNN IDs in first column
3. **FRS** — has `requirement_description` column or ≥3 FRS-NNN IDs, no test_step
4. **URS** — ≥3 URS-NNN IDs in first column
5. **Unknown** — accepted with warning

**Cross-slot consistency check:** After all four CIA files are uploaded, Python
compares OQ IDs in the trace matrix against OQ IDs in the OQ file. Zero overlap
triggers a version mismatch warning.

---

## 8. SECURITY MODEL

- **Auth:** bcrypt-only password hashing. Role-based access: Admin / QA / Validator.
- **Session:** 15-minute inactivity timeout (21 CFR Part 11). All three logout paths use `st.session_state.clear()` — full scorched-earth wipe.
- **Upload gate:** 10MB max, PDF magic bytes check (`b'%PDF'`), then content heuristics, then LLM preflight.
- **Prompt injection defence:** SYSTEM_PROMPT instructs LLM to treat all document content as untrusted data regardless of wording.
- **Audit log:** Every analysis, login, logout, error, and signature event written to `audit_log` SQLite table with timestamp, user, IP, and SHA-256 doc hash.
- **E-signature:** 21 CFR Part 11 §11.200 — password re-verification, meaning selection, SHA-256 hash of output, `signature_log` table entry with `sig_id`.

---

## 9. CONFIGURATION CONSTANTS

```python
VERSION                 = "37.0"
PROMPT_VERSION          = "v19.0-esignature-test-type-r3c"
TEMPERATURE             = 0.2        # Low for deterministic outputs
CHUNK_SIZE              = 8          # Pages per Pass 1 chunk
PASS2_CHUNK             = 40         # URS rows per Pass 2 batch (user-tunable: 20/40/60)
SESSION_TIMEOUT_MINUTES = 15
MAX_UPLOAD_BYTES        = 10_485_760 # 10 MB
```

---

## 10. SUPPORTED AI MODELS

Configured in the `MODELS` dict. Currently supported:

| Display Name | LiteLLM model string |
|---|---|
| Gemini 1.5 Pro | `gemini/gemini-1.5-pro-latest` |
| Gemini 1.5 Flash | `gemini/gemini-1.5-flash-latest` |
| Claude 3.5 Sonnet | `anthropic/claude-3-5-sonnet-...` |
| GPT-4o | `openai/gpt-4o` |

User selects via sidebar radio button. Selection persists in `st.session_state.selected_model`.

---

## 11. KNOWN CONSTRAINTS AND EDGE CASES

| Constraint | Impact | Workaround |
|---|---|---|
| Scanned/image-only PDFs | Pass 1 extracts nothing — OCR required | Warn user via Parser Quality Indicator; user must OCR the PDF first |
| User Guide capped at 6 pages / 4000 chars | Guide content beyond page 6 not visible to LLM | FRS rows for those screens will fall back to RULE C ([SCREEN UNVERIFIED]) |
| OQ IDs reset between Pass 2 batches | Fixed by `_renumber_oq_ids()` post-processing | Already handled — no user action needed |
| FRS with no `shall` language | URS gate may reject valid FRS-style docs | Use main panel for URS only; CIA mode accepts FRS as PDF without heuristic gate |
| CIA OQ/Trace file >80 rows | Only first 80 rows sent to LLM in Pass 2 | Python trace propagation runs on full file regardless |
| `.md` prompt file missing on deploy | `_load_prompt()` returns empty string, pipeline produces empty output | Always deploy `./prompts/` directory alongside `app_v37.py` |

---

## 12. DEPLOYMENT CHECKLIST

```
✅ app_v37.py
✅ prompts/
    ✅ system_prompt.md
    ✅ preflight_classifier.md
    ✅ pass1_urs_extraction.md
    ✅ pass2_frs_oq_gap.md
    ✅ cia_pass1_change_extraction.md
    ✅ cia_pass2_impact_mapping.md
✅ requirements.txt  (streamlit, litellm, pdfplumber, openpyxl,
                      langchain-community, pypdf, bcrypt, pandas)
✅ validation_app.db (auto-created on first run via db_migrate())
✅ API keys set as environment variables (GEMINI_API_KEY, ANTHROPIC_API_KEY, etc.)
```

---

## 13. ADDING A NEW ANALYSIS MODE

The sidebar mode selector lives in `show_app()`. To add a new mode (e.g. Gap Audit):

1. Add the mode label to the `st.radio` options list in `show_app()`
2. Remove `(coming soon)` suffix from the label
3. Write a `show_gap_audit(user, role, model_id)` function following the same
   pattern as `show_change_impact()`
4. Add a new prompt `.md` file in `./prompts/`
5. Add `_load_prompt("your_new_prompt.md")` to the loader block in Section 1b
6. Add the routing branch in the `_mode` if/elif block in `show_app()`
7. Add session state defaults for any new file slots to `_defaults`

Planned modes (in priority order):
- **Gap Audit** — run R0–R6+R3d against uploaded existing FRS/OQ
- **Periodic Review Acceleration** — coverage report for existing validated docs
- **Delta Generation** — generate FRS/OQ only for net-new requirements

---

*This SKILL.md is the authoritative reference for anyone extending, deploying,
or prompting against Validation Doc Assist v37. Keep it in sync with the codebase.*
