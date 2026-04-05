# VALINTEL.AI — Handoff Summary
## Session Date: 2026-04-05

---

## Project
VALINTEL.AI — GxP Validation Intelligence Platform
Repo: https://github.com/shanramani/Traceability-Architect.git (branch: main)
Deploy: Streamlit Cloud (generator.py = the app)
Current version: app_v61.py

---

## Working Files
- App: `generator.py` (Streamlit reads this — always copy v61 → generator before pushing)
- Local versioned copy: `app_v61.py` (666k, local is source of truth)
- Test input: `valintel_sample_audit_log_template.xlsx`
- AT outputs: `AuditTrail_today.xlsx`, `AuditTrail_yesterday.xlsx`
- Validation outputs: `Validation_Package_no Sys guide subset.xlsx`, `Validation_Package_Full Sys guide.xlsx`

---

## Git Deploy Pattern
```bash
copy app_v61.py generator.py
git add generator.py app_v61.py
git commit -m "v61: description"
git pull origin main --rebase
git push origin main
```
Then reboot app on Streamlit Cloud.

---

## What Was Done This Session

### Bug Fix 1 — Traceability Matrix Broken (FIXED ✅)
**Root cause:** `Source_URS_Ref` stored as `"URS-001 (CAL01)"` but lookup code compared
against `"URS-001"` — mismatch caused all URS rows to show "Missing FRS" even when FRS existed.
Cascaded into: 0% traceability, phantom FRS-009 to FRS-016, 8 phantom OQ placeholders,
16 phantom Det_Validation findings.

**Fixed in 4 locations in generator.py / app_v61.py:**
- `_fill_missing_frs` — frs_urs_refs set now strips `(SRC_ID)` annotation
- `_build_traceability` — frs_by_urs dict now keys on base URS ID
- Deterministic validation R0 check — same normalisation
- URS accountability audit log check — same normalisation

**Fix pattern:** `re.sub(r'\s*\(.*?\)\s*$', '', ref).strip()` applied to all Source_URS_Ref lookups.

**Confirmed working:** Validation_Package_Full Sys guide.xlsx shows all 8 URS fully covered,
0 phantom rows, traceability 100% basic coverage.

---

### Bug Fix 2 — Audit Trail 988/1000 Rows Showing High (FIXED ✅)
**Root cause:** New v61 copy-paste rationale sub-check had `_CP_THRESHOLD = 3`.
Test data had 970 rows with identical comment `"Standard value entry per SOP-01"` on RESULTS table.
970 >> 3 → all flagged Rule 1 HIGH → `_apply_tier_override` elevated all to High.

**Fix in generator.py / app_v61.py:**
- `_CP_THRESHOLD` raised from 3 → 20
- SOP/STP/WOI reference whitelist regex added (`_SOP_REF_PAT`) — comments referencing
  a procedure are excluded from copy-paste flagging regardless of count

**Confirmed working:** AuditTrail_today.xlsx shows 976 Low / 12 Medium / 8 High / 4 Critical.

---

### Validation Output Assessment — No Sys Guide
- Traceability fix confirmed: all 8 URS fully covered ✅
- Coverage went from 0% → 100% basic after fix ✅
- Gap analysis: only 2 gaps generated (thin — run with sys guide for demo)
- OQ tests: all [SCREEN UNVERIFIED] at 0.60 confidence — expected without sys guide

---

### Validation Output Assessment — Full Sys Guide (ThermoFisher LIMS manual PDF)
- Traceability: all 8 URS fully covered ✅
- 50 FRS total — 8 real URS-derived + 42 XFRS cross-source rows from Pass 3
- XFRS rows inflating coverage denominator → dashboard shows 16% instead of 100% 🔴
- OQ still all [SCREEN UNVERIFIED] even with sys guide — sys_context chain bottleneck (fix queue #2)
- Gap analysis: 21 gaps — strong ✅
- FRS-009 blank phantom row still present 🔴
- 85 Det_Validation issues — 82 are noise from XFRS rows with no OQ tests 🔴

---

### Audit Trail Output Assessment (post copy-paste fix)
- Risk distribution correct: 976 Low / 12 Medium / 8 High / 4 Critical ✅
- 4 Critical findings all genuine:
  - Rule 7: dba_prod modified AUDIT_TRAIL table (SYS-001)
  - Rule 6: analyst_y delete+recreate on RES-8888 (both legs captured and linked)
  - Rule 3: admin_sys direct INSERT on BATCH_RELEASE/BATCH-999
- Events for Review sheet: excellent quality — regulation citations, Why/What/Recommended
  Action, system-proposed dispositions, reviewer sign-off section ✅
- Summary narrative: professional and client-ready ✅
- Remaining issues: see fix queue below

---

### Architecture / Product Discussions

**Rules vs Dimensions:**
- Dimensions (temporal, privilege, record, gap, del_recreate) are internal scoring mechanics —
  never mention to client, they feed the composite score silently
- 13 active rules, internal numbering has gaps (8, 9, 15 retired/merged) — gaps visible in output

**Rule → Regulation Mapping (use in presentations):**

| Rule | Name | 21 CFR Part 11 | EU Annex 11 |
|------|------|----------------|-------------|
| 1  | Vague Rationale | §11.10(e) | Clause 9 |
| 2  | Contemporaneous Burst | §211.68 | Clause 9 |
| 3  | Admin/GxP Conflict | §11.10(d) | Clause 12 |
| 4  | Change Control Drift | §820.70(b) | Clause 10 |
| 5  | Failed Login → Data Manipulation | §11.10(d) | Clause 12 |
| 6  | Record Reconstruction Pattern | §11.10(e) | Clause 9 |
| 7  | Audit Trail Integrity Event | §11.10(e) | Clause 9 |
| 8  | Privileged User on GxP Data | §11.10(d) | Clause 12 |
| 9  | Audit Trail Timestamp Gap | §11.10(e) | Clause 9 |
| 10 | Off-Hours / Holiday Activity | §11.10(d) | Clause 12 |
| 11 | Timestamp Reversal | §11.10(e) | Clause 9 |
| 12 | Service / Shared Account GxP Action | §11.10(d) | Clause 12 |
| 13 | Dormant Account Sudden Activity | §11.10(d) | Clause 12 |

**Why 13 rules:** Each maps to a distinct failure mode from FDA 483 observations and
MHRA/EU GMP inspection findings. Not arbitrary — minimum set for full coverage of FDA
2018 Data Integrity Guidance failure categories with no overlap.

**"Doesn't the LIMS prevent these?"** Key client answer:
The LIMS records what happened. It was never designed to judge whether what happened
was appropriate. That judgment layer is what VALINTEL adds.

**Delta + CIA modules:** Agreed to combine into single Change Control Package module.
Remove Delta Generation from sidebar. Build Monday. See fix queue #9.

---

## Module Map (do not touch unless specifically asked)
- Audit Trail Review: `at_score_events()`, `at_build_excel()`, `at_generate_justifications()`
- Change Impact Analysis: `run_cia_analysis()`, `show_change_impact()`, `build_cia_excel()`
- New Validation: `run_segmented_analysis()`, `build_pass2_single_prompt()`, `_summarise_sys_context()`
- URS Gate: `validate_urs_document()` — pages[:5] scan window
- Traceability builder: `_build_traceability()` — Python-built, not AI
- Missing row guards: `_fill_missing_frs()`, `_fill_missing_oq()`
- Confidence flags: `_apply_confidence_flags()`

## 12 Active Scoring Rules — AT Module (lines ~5522+)
score_record, score_del_recreate, score_gap,
score_rule1_vague_rationale, score_rule2_burst, score_rule3_admin_conflict,
score_rule4_drift, score_rule5_failed_login, score_rule12_timestamp_reversal,
score_rule13_service_account, score_rule14_dormant_account, score_rule16_first_time_behavior

## Key Constants (AT module)
- `_AT_VAGUE_TERMS` — set of vague words triggering Rule 1
- `_AT_GXP_TABLES` — GxP table names for Rule 1/3/4
- `_AT_BIZ_START=7`, `_AT_BIZ_END=20` — business hours window
- `_AT_WEEKENDS={5,6}` — weekend days
- `_AT_TOP_N=20` — top events shown in review sheet
- `_CP_THRESHOLD=20` — copy-paste detection threshold (raised from 3 this session)
- `_SOP_REF_PAT` — regex whitelist for SOP/STP/WOI references in copy-paste check

## WEIGHTS dict (composite score)
score_temporal:0.06, score_privilege:0.09, score_record:0.08,
score_del_recreate:0.10, score_gap:0.06, score_rule1:0.07,
score_rule2:0.07, score_rule3:0.10, score_rule4:0.06,
score_rule5:0.08, score_rule12:0.09, score_rule13:0.09,
score_rule14:0.07, score_rule16:0.07

---

## Fix Queue

### Tomorrow (2026-04-06)
1. **Source Document label** — shows `async_job` instead of actual filename on Dashboard and Summary sheets
2. **sys guide ingestion chain** — currently only reads 6 pages, sys_summary capped at 500 chars;
   raise page limit to 100, increase all char caps in chain, make summariser specifically extract
   screen names / module names / navigation paths from guide text:
   - `sys_pages[:6]` → `[:100]`
   - `_summarise_sys_context` input `[:3000]` → `[:50000]`
   - `_summarise_sys_context` output cap `500` → `3000`
   - `build_pass2_prompt` sys_context `[:3000]` → `[:6000]`
   - `_make_system_prompt` sys_context `[:4000]` → `[:8000]`
3. **XFRS rows excluded from coverage KPI denominator** — 42 cross-source FRS rows from Pass 3
   inflate FRS count to 50, making dashboard show 16% coverage when all 8 real URS are covered
4. **FRS-009 blank phantom row** — NaN row still appearing in FRS sheet, needs filter
5. **SOP-referenced entries still escalating via Rule 10** — RES-1000 "Standard value entry per
   SOP-01" no longer fires Rule 1 (copy-paste fix worked) but is now caught by Rule 10 and still
   shown as High — false positive, needs suppression for SOP-referenced comments
6. **Rule 10 label/tier contradiction** — Primary Rule label says [MEDIUM] but Risk Level shows
   High for some rows — label and tier must agree
7. **Out-of-period events** — do not score or escalate; set Risk Level = "Out of Period" label only;
   exclude from Events for Review sheet entirely
8. **Renumber rule display labels 1–13 sequentially** — internal code variable names stay as-is
   (score_rule14 etc.) but all output-facing strings renumbered to remove gaps from retired rules
   (currently gaps at 9, 15 visible to client)

### Monday (2026-04-07)
9. **Change Control Package module** — combine Delta Generation + Change Impact Analysis:
   - Remove Delta Generation as standalone sidebar item
   - Single module: upload old doc + new doc → delta engine identifies changed/added/removed
     requirements semantically → CIA propagates impact through FRS → OQ → Traceability →
     single output package ready for change control record or regulatory submission
   - CIA also remains accessible as standalone entry point (user already knows what changed)

---

## Shan Ramani Background (for interview coaching context)
Resume: 15yr GxP/CSV, LabVantage at Kite/Gilead, Novartis D365/Azure, Amgen, Pfizer
Target role: LIMS Integration & Validation Engineer
Key strengths: LabVantage validated at Kite, 40-instrument integration experience,
21 CFR Part 11, EU Annex 11, GAMP 5 Cat 4, ALCOA+, IQ/OQ/PQ full lifecycle
