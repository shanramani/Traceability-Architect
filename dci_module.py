"""
VALINTEL.AI — DCI (Deviation & CAPA Investigation) Module
============================================================

Per DCI Spec v1.3. 14 rules across 4 engines, keyword-match only, no LLM
in detection path.

Engines:
    A — RCA Recurrence      Rules 1-3
    B — Weak Investigation  Rules 4-8
    C — CAPA Effectiveness  Rules 9-11
    D — SLA / Aging Risk    Rules 12-14

Regulatory coverage:
    21 CFR Part 820.100  |  ICH Q10 §3.2  |  EU GMP Annex 11, Clause 10
    |  FDA CAPA Guidance (2014)  |  FDA CSA Final Guidance (Sep 2021)

This module imports UI helpers from generator.py at load time. Circular
imports are avoided via a deferred-import pattern in generator.py's
dispatcher (it imports from dci_module only when the user clicks the
DCI module button).
"""
import io
import hashlib
import datetime as _dt

import pandas as pd
import streamlit as st

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ── Helpers from generator.py — LAZY import pattern ───────────────────────
# We MUST NOT do `from generator import ...` at module top level.
# Reason: when Streamlit first imports dci_module (via the deferred import
# in generator.py's dispatcher), generator.py is still mid-execution. A
# top-level `from generator import X` would re-enter generator, which
# re-runs `st.set_page_config()` — but widgets have already rendered, so
# Streamlit raises StreamlitSetPageConfigMustBeFirstCommandError.
#
# Solution: defer the import inside a cached accessor. First call actually
# imports (generator.py is fully loaded by then). Subsequent calls return
# cached references.
_gen_helpers = None

def _gen():
    """
    Lazy-load helpers from generator.py — looking up the ALREADY-RUNNING
    generator module via sys.modules, NEVER using `import generator`.

    Why this matters in Streamlit:
      - Streamlit runs generator.py as the entry script. Python stores it
        under the module name "__main__" (or an ephemeral name), NOT under
        "generator".
      - Doing `import generator` would cause Python to find generator.py
        on the path, see no cached entry under that exact name, and RUN
        the file top-to-bottom AGAIN. That re-executes `st.set_page_config()`
        which Streamlit forbids past the first call.
      - The fix: walk sys.modules to find the module object that contains
        the helpers we need. That object is already loaded and in memory.
    """
    global _gen_helpers
    if _gen_helpers is not None:
        return _gen_helpers

    import sys as _sys

    # Strategy: find the module object that defines the functions we need.
    # Generator.py is guaranteed to be loaded (we're running from it) but
    # its module name depends on how Streamlit invoked it. Search both
    # the obvious candidates and the full module registry.
    _candidate_names = ["__main__", "generator", "streamlit.generator"]
    _gen_mod = None
    for _name in _candidate_names:
        _m = _sys.modules.get(_name)
        if _m is not None and hasattr(_m, "log_audit") and hasattr(_m, "_scroll_top"):
            _gen_mod = _m
            break

    # Fallback: scan all loaded modules for one whose __file__ ends with
    # generator.py. Handles any naming convention Streamlit might use.
    if _gen_mod is None:
        for _name, _m in list(_sys.modules.items()):
            try:
                _f = getattr(_m, "__file__", "") or ""
            except Exception:
                continue
            if _f.endswith("generator.py") and hasattr(_m, "log_audit"):
                _gen_mod = _m
                break

    if _gen_mod is None:
        # sys.modules didn't contain a match. We deliberately do NOT fall back
        # to `import generator` — that would re-execute generator.py and
        # re-trigger st.set_page_config, which is exactly the bug this
        # function exists to avoid. Raise a clear error instead.
        raise RuntimeError(
            "dci_module._gen() could not locate the already-loaded generator "
            "module in sys.modules. Candidates searched: "
            f"{_candidate_names}. Loaded modules count: {len(_sys.modules)}. "
            "This usually means dci_module.py was loaded before generator.py "
            "finished its top-level execution, or the two files are not in "
            "the same folder. Verify deployment layout."
        )

    _gen_helpers = {
        "log_audit":                      _gen_mod.log_audit,
        "_render_validator_verdict":      _gen_mod._render_validator_verdict,
        "_scroll_top":                    _gen_mod._scroll_top,
        "_cols_lower":                    _gen_mod._cols_lower,
        "_matching_cols":                 _gen_mod._matching_cols,
        "_find_col":                      _gen_mod._find_col,
        "_verdict_from_results":          _gen_mod._verdict_from_results,
        "_VALINTEL_SHEET_FINGERPRINTS":   _gen_mod._VALINTEL_SHEET_FINGERPRINTS,
        "_VALINTEL_COLUMN_FINGERPRINTS":  _gen_mod._VALINTEL_COLUMN_FINGERPRINTS,
        "_AT_VOCAB_COLUMNS":              _gen_mod._AT_VOCAB_COLUMNS,
        "_UAR_VOCAB_COLUMNS":             _gen_mod._UAR_VOCAB_COLUMNS,
        "_dim_event_category":            getattr(_gen_mod, "_dim_event_category", None),
    }
    return _gen_helpers


# ═══════════════════════════════════════════════════════════════════════════
#  Regulatory reference
# ═══════════════════════════════════════════════════════════════════════════
_REG_DCI = ("21 CFR Part 820.100  |  ICH Q10 §3.2  "
            "|  EU GMP Annex 11, Clause 10  |  FDA CAPA Guidance (2014)  "
            "|  FDA CSA Final Guidance (Sep 2021)")


# ═══════════════════════════════════════════════════════════════════════════
#  Required columns + column alias map
# ═══════════════════════════════════════════════════════════════════════════
_DCI_REQUIRED_COLS = {
    "record_id", "record_type", "deviation_category",
    "system_name", "open_date", "close_date",
    "rca_text", "capa_text", "assigned_to",
    "approved_by", "status", "sla_days",
}

_DCI_COLUMN_ALIASES = {
    # record_id
    "record_id": "record_id", "recordid": "record_id", "id": "record_id",
    "deviation_id": "record_id", "deviation id": "record_id",
    "capa_id": "record_id", "capa id": "record_id",
    "nc_id": "record_id", "event_id": "record_id", "case_id": "record_id",
    # record_type
    "record_type": "record_type", "recordtype": "record_type",
    "type": "record_type", "record type": "record_type",
    # deviation_category
    "deviation_category": "deviation_category", "category": "deviation_category",
    "cause_category": "deviation_category", "root_cause_category": "deviation_category",
    "rca_category": "deviation_category",
    # system_name
    "system_name": "system_name", "system": "system_name", "systemname": "system_name",
    "application": "system_name", "gxp_system": "system_name",
    # open_date
    "open_date": "open_date", "opendate": "open_date",
    "date_opened": "open_date", "opened_date": "open_date",
    "occurrence_date": "open_date", "report_date": "open_date",
    # close_date
    "close_date": "close_date", "closedate": "close_date",
    "date_closed": "close_date", "closed_date": "close_date",
    "closure_date": "close_date",
    # rca_text
    "rca_text": "rca_text", "rca": "rca_text", "root_cause": "rca_text",
    "root cause": "rca_text", "root_cause_text": "rca_text",
    "investigation": "rca_text", "investigation_summary": "rca_text",
    # capa_text
    "capa_text": "capa_text", "capa": "capa_text",
    "corrective_action": "capa_text", "corrective action": "capa_text",
    "action_taken": "capa_text", "capa_description": "capa_text",
    # assigned_to
    "assigned_to": "assigned_to", "assignee": "assigned_to",
    "owner": "assigned_to", "assigned": "assigned_to",
    "investigator": "assigned_to", "responsible": "assigned_to",
    # approved_by
    "approved_by": "approved_by", "approver": "approved_by",
    "approved": "approved_by", "closed_by": "approved_by",
    "reviewer": "approved_by",
    # status
    "status": "status", "state": "status", "current_status": "status",
    "record_status": "status",
    # sla_days
    "sla_days": "sla_days", "sla": "sla_days", "due_days": "sla_days",
    "target_days": "sla_days", "closure_sla": "sla_days",
}

# Cross-module rejection detection
_DCI_VOCAB_COLUMNS = {
    "record_id", "recordid", "deviation_id", "deviationid",
    "capa_id", "capaid", "nc_id", "case_id", "caseid",
    "deviation_category", "cause_category", "root_cause_category",
    "rca_text", "rca", "root_cause", "investigation",
    "capa_text", "capa", "corrective_action", "action_taken",
    "open_date", "opendate", "date_opened", "occurrence_date",
    "close_date", "closedate", "date_closed", "closure_date",
    "sla_days", "sla", "due_days", "target_days",
    "assigned_to", "assignee", "investigator", "approved_by", "approver",
}


# ═══════════════════════════════════════════════════════════════════════════
#  DCI vocabularies (per Spec v1.3 §4.1)
# ═══════════════════════════════════════════════════════════════════════════
# Reviewer Round 2 guardrails locked here:
#   - Keep _DCI_SPECIFIC_CAUSE_TERMS SMALL (10 terms). Do NOT expand.
#   - Match semantics: case-insensitive substring via Python `in` operator.
#     No regex, no stemming, no NLP.

_DCI_VAGUE_RCA_TERMS = {
    "human error", "operator error", "mistake", "carelessness",
    "lack of attention", "oversight", "misunderstanding",
    "forgot", "forgotten", "negligence",
}

_DCI_SPECIFIC_CAUSE_TERMS = {
    "instrument",     # "instrument failure", "instrument malfunction"
    "calibration",    # "calibration drift", "out of calibration"
    "sop",            # "SOP deviation", "SOP step missed"
    "method",         # "method error", "method not followed"
    "equipment",      # "equipment failure", "equipment malfunction"
    "software",       # "software glitch", "software bug"
    "procedure",      # "procedure not followed", "procedure outdated"
    "reagent",        # "reagent expired", "reagent contamination"
    "specification",  # "out of specification", "specification misread"
    "component",      # "component failure", "component worn"
}

_DCI_TRAINING_ONLY_TERMS = {
    "retrain", "retraining", "refresher", "reminder",
    "toolbox talk", "additional training", "training session",
    "awareness", "briefing",
}

_DCI_CAPA_ACTION_TERMS = {
    "sop", "procedure", "method", "specification",
    "engineering", "design", "hardware", "software",
    "equipment", "requalification", "revalidation",
    "preventive maintenance",
}


# ═══════════════════════════════════════════════════════════════════════════
#  Input file validator (per Spec v1.3 §3)
# ═══════════════════════════════════════════════════════════════════════════
def _validate_dci_input_file(raw_bytes: bytes, file_name: str, df: pd.DataFrame,
                              all_sheet_names: list = None) -> tuple:
    """
    DCI input file validator. 8 invariants (3 fatal, 5 normal).

    Returns (ok, severity, title, results, evidence).
    severity: "hard_reject" | "warn" | "ok"
    """
    results  = []
    evidence = []

    def _has_dci_col(canonical: str) -> tuple:
        """Returns (has_col, actual_column_name_found)."""
        aliases_for_canonical = {
            alias for alias, canon in _DCI_COLUMN_ALIASES.items()
            if canon == canonical
        }
        cols_norm = _gen()["_cols_lower"](df)
        hit = cols_norm & aliases_for_canonical
        if hit:
            return True, sorted(hit)[0]
        return False, ""

    # ── DCI-F1: Not a VALINTEL output ─────────────────────────────────────
    _sheet_hits = set()
    if all_sheet_names:
        _sheet_hits = ({str(s).strip().lower() for s in all_sheet_names}
                       & _gen()["_VALINTEL_SHEET_FINGERPRINTS"])
    _col_hits = _gen()["_cols_lower"](df) & _gen()["_VALINTEL_COLUMN_FINGERPRINTS"]
    _f1_passed = not _sheet_hits and not _col_hits
    _f1_detail = "Not a VALINTEL output"
    if not _f1_passed:
        if _sheet_hits:
            _d = f"VALINTEL-only sheets detected: {', '.join(sorted(_sheet_hits))}"
            _f1_detail = _d
            evidence.append(_d)
        if _col_hits:
            _d = f"VALINTEL-only columns detected: {', '.join(sorted(_col_hits))}"
            _f1_detail = _d if _f1_passed else _f1_detail + " · " + _d
            evidence.append(_d)
    results.append({
        "id": "DCI-F1", "name": "Not a VALINTEL output",
        "severity_class": "fatal",
        "passed": _f1_passed, "detail": _f1_detail,
    })

    # ── DCI-F2: Has record_id column ──────────────────────────────────────
    _f2_has, _f2_col = _has_dci_col("record_id")
    _f2_detail = (f"record_id column found: '{_f2_col}'"
                  if _f2_has else
                  "No record identifier column found. Required to uniquely "
                  "identify deviations/CAPAs.")
    if not _f2_has:
        evidence.append("Missing: record_id (or alias: deviation_id, capa_id, "
                        "nc_id, case_id, event_id)")
    results.append({
        "id": "DCI-F2", "name": "Has record_id column (or alias)",
        "severity_class": "fatal",
        "passed": _f2_has, "detail": _f2_detail,
    })

    # ── DCI-F3: Has BOTH rca_text AND capa_text ───────────────────────────
    _f3_rca_has, _f3_rca_col   = _has_dci_col("rca_text")
    _f3_capa_has, _f3_capa_col = _has_dci_col("capa_text")
    _f3_passed = _f3_rca_has and _f3_capa_has
    if _f3_passed:
        _f3_detail = (f"Both RCA ('{_f3_rca_col}') and CAPA ('{_f3_capa_col}') "
                      "columns present.")
    else:
        _missing = []
        if not _f3_rca_has:  _missing.append("rca_text")
        if not _f3_capa_has: _missing.append("capa_text")
        _f3_detail = ("Both RCA text and CAPA text columns are required. "
                      f"Missing: {', '.join(_missing)}.")
        evidence.append(_f3_detail)
    results.append({
        "id": "DCI-F3", "name": "Has both rca_text AND capa_text columns",
        "severity_class": "fatal",
        "passed": _f3_passed, "detail": _f3_detail,
    })

    # ── DCI-N1: Has open_date ─────────────────────────────────────────────
    _n1_has, _n1_col = _has_dci_col("open_date")
    _n1_detail = (f"open_date column found: '{_n1_col}'" if _n1_has else
                  "No record open date column found — SLA/aging rules "
                  "(12/13/14) will be unavailable.")
    if not _n1_has:
        evidence.append("Missing: open_date (or alias: opendate, date_opened, "
                        "occurrence_date, report_date)")
    results.append({
        "id": "DCI-N1", "name": "Has open_date column (or alias)",
        "severity_class": "normal",
        "passed": _n1_has, "detail": _n1_detail,
    })

    # ── DCI-N2: Has status column ─────────────────────────────────────────
    _n2_has, _n2_col = _has_dci_col("status")
    _n2_detail = (f"status column found: '{_n2_col}'" if _n2_has else
                  "No status column found — closure-based rules will be "
                  "unavailable.")
    if not _n2_has:
        evidence.append("Missing: status (or alias: state, current_status, "
                        "record_status)")
    results.append({
        "id": "DCI-N2", "name": "Has status column (or alias)",
        "severity_class": "normal",
        "passed": _n2_has, "detail": _n2_detail,
    })

    # ── DCI-N3: AT vocabulary does NOT dominate ───────────────────────────
    _at_cols  = _gen()["_matching_cols"](df, _gen()["_AT_VOCAB_COLUMNS"])
    _dci_cols = _gen()["_matching_cols"](df, _DCI_VOCAB_COLUMNS)
    _n3_dominated = (len(_at_cols) >= 3 and len(_at_cols) > len(_dci_cols))
    _n3_passed = not _n3_dominated
    if _n3_passed:
        _n3_detail = ("No audit-trail vocabulary dominance "
                      f"(AT cols: {len(_at_cols)}, DCI cols: {len(_dci_cols)}).")
    else:
        _n3_detail = ("File appears to be an audit trail log "
                      f"(AT vocabulary columns: {', '.join(_at_cols)}). "
                      "Upload via AT module instead.")
        evidence.append(_n3_detail)
    results.append({
        "id": "DCI-N3", "name": "No AT vocabulary dominance",
        "severity_class": "normal",
        "passed": _n3_passed, "detail": _n3_detail,
    })

    # ── DCI-N4: UAR vocabulary does NOT dominate ──────────────────────────
    _uar_cols = _gen()["_matching_cols"](df, _gen()["_UAR_VOCAB_COLUMNS"])
    _n4_dominated = (len(_uar_cols) >= 3 and len(_uar_cols) > len(_dci_cols))
    _n4_passed = not _n4_dominated
    if _n4_passed:
        _n4_detail = ("No user-access vocabulary dominance "
                      f"(UAR cols: {len(_uar_cols)}, DCI cols: {len(_dci_cols)}).")
    else:
        _n4_detail = ("File appears to be a user access list "
                      f"(UAR vocabulary columns: {', '.join(_uar_cols)}). "
                      "Upload via UAR module instead.")
        evidence.append(_n4_detail)
    results.append({
        "id": "DCI-N4", "name": "No UAR vocabulary dominance",
        "severity_class": "normal",
        "passed": _n4_passed, "detail": _n4_detail,
    })

    # ── DCI-N5: Record-centric structure ──────────────────────────────────
    _n5_passed = True
    _n5_detail = "Record uniqueness check skipped (no record_id column)."
    if _f2_has and _f2_col and len(df) > 0:
        try:
            _rid_col_actual = _gen()["_find_col"](df, {_f2_col})
            _rid_vals = df[_rid_col_actual].astype(str)
            _unique_ratio = len(set(_rid_vals)) / max(len(_rid_vals), 1)
            _n5_passed = _unique_ratio >= 0.50
            if _n5_passed:
                _n5_detail = (f"Record uniqueness: {_unique_ratio:.0%} "
                              f"({len(set(_rid_vals))}/{len(_rid_vals)}).")
            else:
                _n5_detail = (f"Low record_id uniqueness ({_unique_ratio:.0%}) — "
                              "snapshot data assumed. If this is an audit-history "
                              "export, collapse to latest state per record first.")
                evidence.append(_n5_detail)
        except Exception:
            _n5_detail = "Record uniqueness check skipped (column read error)."
    results.append({
        "id": "DCI-N5", "name": "Record-centric structure (>=50% unique)",
        "severity_class": "normal",
        "passed": _n5_passed, "detail": _n5_detail,
    })

    # ── Verdict ───────────────────────────────────────────────────────────
    _severity = _gen()["_verdict_from_results"](results)
    _title = ""
    if _severity == "hard_reject":
        _title = ("File rejected — does not match "
                  "Deviation & CAPA Investigation source pattern")
    elif _severity == "warn":
        _title = "File accepted with a warning"

    return (_severity != "hard_reject", _severity, _title, results, evidence)


# ═══════════════════════════════════════════════════════════════════════════
#  Rule config, metadata, tier priority
# ═══════════════════════════════════════════════════════════════════════════
_DCI_RULE_DEFAULTS = {
    "dci_r1_on":  True,   "dci_r2_on":  True,   "dci_r3_on":  True,
    "dci_r4_on":  True,   "dci_r5_on":  True,   "dci_r6_on":  True,
    "dci_r7_on":  True,   "dci_r8_on":  True,
    "dci_r9_on":  True,   "dci_r10_on": True,   "dci_r11_on": False,
    "dci_r12_on": True,   "dci_r13_on": True,   "dci_r14_on": False,
}

_DCI_CFG_SCORE_MAP = {
    "dci_r1_on":  ["score_dci_rule1_recurring_category"],
    "dci_r2_on":  ["score_dci_rule2_repeat_category_hivol"],
    "dci_r3_on":  ["score_dci_rule3_repeat_system"],
    "dci_r4_on":  ["score_dci_rule4_short_rca"],
    "dci_r5_on":  ["score_dci_rule5_vague_rca"],
    "dci_r6_on":  ["score_dci_rule6_missing_rca"],
    "dci_r7_on":  ["score_dci_rule7_missing_capa"],
    "dci_r8_on":  ["score_dci_rule8_weak_capa"],
    "dci_r9_on":  ["score_dci_rule9_repeat_post_closure"],
    "dci_r10_on": ["score_dci_rule10_reopened_capa"],
    "dci_r11_on": ["score_dci_rule11_short_close"],
    "dci_r12_on": ["score_dci_rule12_overdue"],
    "dci_r13_on": ["score_dci_rule13_near_breach"],
    "dci_r14_on": ["score_dci_rule14_no_activity"],
}

_DCI_RULE_META = [
    # (rule_num, name, severity, tier, engine, default, cfg_key)
    (1,  "Recurring Category",                "Medium",   "T1", "A", True,  "dci_r1_on"),
    (2,  "Repeat Category High Volume",       "High",     "T1", "A", True,  "dci_r2_on"),
    (3,  "Repeat System",                     "High",     "T1", "A", True,  "dci_r3_on"),
    (4,  "Short RCA Narrative",               "High",     "T1", "B", True,  "dci_r4_on"),
    (5,  "Vague RCA (generic cause only)",    "Critical", "T1", "B", True,  "dci_r5_on"),
    (6,  "Missing RCA",                       "Critical", "T1", "B", True,  "dci_r6_on"),
    (7,  "Missing CAPA",                      "Critical", "T1", "B", True,  "dci_r7_on"),
    (8,  "Weak CAPA (training-only)",         "High",     "T1", "B", True,  "dci_r8_on"),
    (9,  "Repeat Deviation Post-Closure",     "Critical", "T1", "C", True,  "dci_r9_on"),
    (10, "Re-opened CAPA",                    "High",     "T1", "C", True,  "dci_r10_on"),
    (11, "Short Close Cycle",                 "Medium",   "T2", "C", False, "dci_r11_on"),
    (12, "Overdue",                           "High",     "T1", "D", True,  "dci_r12_on"),
    (13, "Near-Breach",                       "Medium",   "T1", "D", True,  "dci_r13_on"),
    (14, "No Activity",                       "Medium",   "T2", "D", False, "dci_r14_on"),
]

_DCI_SEVERITY_SCORE = {"Critical": 9.0, "High": 7.0, "Medium": 6.0}

_DCI_RULE_TIER_PRIORITY = [
    ("score_dci_rule6_missing_rca",             9.0, "Critical"),
    ("score_dci_rule7_missing_capa",            9.0, "Critical"),
    ("score_dci_rule5_vague_rca",               9.0, "Critical"),
    ("score_dci_rule9_repeat_post_closure",     9.0, "Critical"),
    ("score_dci_rule2_repeat_category_hivol",   7.0, "High"),
    ("score_dci_rule3_repeat_system",           7.0, "High"),
    ("score_dci_rule4_short_rca",               7.0, "High"),
    ("score_dci_rule8_weak_capa",               7.0, "High"),
    ("score_dci_rule10_reopened_capa",          7.0, "High"),
    ("score_dci_rule12_overdue",                7.0, "High"),
    ("score_dci_rule1_recurring_category",      6.0, "Medium"),
    ("score_dci_rule11_short_close",            6.0, "Medium"),
    ("score_dci_rule13_near_breach",            6.0, "Medium"),
    ("score_dci_rule14_no_activity",            6.0, "Medium"),
]

_DCI_RULE_DISPLAY_NAMES = {
    "score_dci_rule1_recurring_category":     "Rule 1 — Recurring Category",
    "score_dci_rule2_repeat_category_hivol":  "Rule 2 — Repeat Category (High Volume)",
    "score_dci_rule3_repeat_system":          "Rule 3 — Repeat System",
    "score_dci_rule4_short_rca":              "Rule 4 — Short RCA Narrative",
    "score_dci_rule5_vague_rca":              "Rule 5 — Vague RCA (generic cause only)",
    "score_dci_rule6_missing_rca":            "Rule 6 — Missing RCA",
    "score_dci_rule7_missing_capa":           "Rule 7 — Missing CAPA",
    "score_dci_rule8_weak_capa":              "Rule 8 — Weak CAPA (training-only)",
    "score_dci_rule9_repeat_post_closure":    "Rule 9 — Repeat Deviation Post-Closure",
    "score_dci_rule10_reopened_capa":         "Rule 10 — Re-opened CAPA",
    "score_dci_rule11_short_close":           "Rule 11 — Short Close Cycle",
    "score_dci_rule12_overdue":               "Rule 12 — Overdue",
    "score_dci_rule13_near_breach":           "Rule 13 — Near-Breach",
    "score_dci_rule14_no_activity":           "Rule 14 — No Activity",
}


# ═══════════════════════════════════════════════════════════════════════════
#  Helpers
# ═══════════════════════════════════════════════════════════════════════════
def _dci_normalize_status(status) -> str:
    """Canonicalize status to one of: closed | reopened | open | other."""
    if status is None or (isinstance(status, float) and pd.isna(status)):
        return "other"
    s = str(status).strip().lower()
    if s in ("", "nan", "none"):
        return "other"
    if s in ("closed", "complete", "completed"):
        return "closed"
    if s in ("re-opened", "reopened", "re_opened", "reopen"):
        return "reopened"
    if s in ("open", "in-progress", "in progress", "in_progress",
             "investigating", "pending"):
        return "open"
    return "other"


def _dci_parse_date(v):
    """Parse to pd.Timestamp; return pd.NaT on failure."""
    if v is None:
        return pd.NaT
    try:
        return pd.to_datetime(v, errors="coerce")
    except Exception:
        return pd.NaT


# ═══════════════════════════════════════════════════════════════════════════
#  ENGINE A — RCA Recurrence (Rules 1-3)
# ═══════════════════════════════════════════════════════════════════════════
def _dci_rule1_recurring_category(df):
    """Rule 1 — Recurring Category. Same deviation_category in >=3 records
    within 180-day sliding window. Medium severity."""
    n = len(df)
    scores    = [0.0] * n
    rationales = [""]  * n
    if "deviation_category" not in df.columns or "open_date" not in df.columns:
        return pd.Series(scores, index=df.index), pd.Series(rationales, index=df.index)

    cats  = df["deviation_category"].astype(str).str.strip().str.lower()
    dates = pd.to_datetime(df["open_date"], errors="coerce")

    from collections import defaultdict
    cat_index = defaultdict(list)
    for i, (c, d) in enumerate(zip(cats, dates)):
        if c and c not in ("nan", "none", "") and pd.notna(d):
            cat_index[c].append((i, d))

    for cat, entries in cat_index.items():
        if len(entries) < 3:
            continue
        entries.sort(key=lambda x: x[1])
        dates_arr = [e[1] for e in entries]
        for start_i in range(len(entries)):
            end_i = start_i
            while (end_i + 1 < len(entries) and
                   (dates_arr[end_i + 1] - dates_arr[start_i]).days <= 180):
                end_i += 1
            if end_i - start_i + 1 >= 3:
                n_in_window = end_i - start_i + 1
                window_start = dates_arr[start_i].strftime("%Y-%m-%d")
                window_end   = dates_arr[end_i].strftime("%Y-%m-%d")
                for j in range(start_i, end_i + 1):
                    row_idx = entries[j][0]
                    if scores[row_idx] == 0.0:
                        scores[row_idx] = _DCI_SEVERITY_SCORE["Medium"]
                        rationales[row_idx] = (
                            f"Deviation category '{cat}' appeared in "
                            f"{n_in_window} records between {window_start} and "
                            f"{window_end} — possible recurring systemic cause. "
                            "ICH Q10 §3.2 requires effective CAPA to prevent "
                            "recurrence."
                        )
                break
    return (pd.Series(scores, index=df.index),
            pd.Series(rationales, index=df.index))


def _dci_rule2_repeat_category_hivol(df):
    """Rule 2 — Repeat Category High Volume. Same deviation_category in >=5
    records within 90-day window. High severity."""
    n = len(df)
    scores    = [0.0] * n
    rationales = [""]  * n
    if "deviation_category" not in df.columns or "open_date" not in df.columns:
        return pd.Series(scores, index=df.index), pd.Series(rationales, index=df.index)

    cats  = df["deviation_category"].astype(str).str.strip().str.lower()
    dates = pd.to_datetime(df["open_date"], errors="coerce")

    from collections import defaultdict
    cat_index = defaultdict(list)
    for i, (c, d) in enumerate(zip(cats, dates)):
        if c and c not in ("nan", "none", "") and pd.notna(d):
            cat_index[c].append((i, d))

    for cat, entries in cat_index.items():
        if len(entries) < 5:
            continue
        entries.sort(key=lambda x: x[1])
        dates_arr = [e[1] for e in entries]
        for start_i in range(len(entries)):
            end_i = start_i
            while (end_i + 1 < len(entries) and
                   (dates_arr[end_i + 1] - dates_arr[start_i]).days <= 90):
                end_i += 1
            if end_i - start_i + 1 >= 5:
                n_in_window = end_i - start_i + 1
                for j in range(start_i, end_i + 1):
                    row_idx = entries[j][0]
                    if scores[row_idx] == 0.0:
                        scores[row_idx] = _DCI_SEVERITY_SCORE["High"]
                        rationales[row_idx] = (
                            f"Deviation category '{cat}' appeared in "
                            f"{n_in_window} records within 90 days — "
                            "high-volume recurrence indicates prior CAPA "
                            "ineffective. 21 CFR 820.100(a)(2), ICH Q10 §3.2.3."
                        )
                break
    return (pd.Series(scores, index=df.index),
            pd.Series(rationales, index=df.index))


def _dci_rule3_repeat_system(df):
    """Rule 3 — Repeat System. Same system_name has >=3 deviations within
    60-day window. High severity."""
    n = len(df)
    scores    = [0.0] * n
    rationales = [""]  * n
    if "system_name" not in df.columns or "open_date" not in df.columns:
        return pd.Series(scores, index=df.index), pd.Series(rationales, index=df.index)

    systems = df["system_name"].astype(str).str.strip().str.lower()
    dates   = pd.to_datetime(df["open_date"], errors="coerce")

    from collections import defaultdict
    sys_index = defaultdict(list)
    for i, (s, d) in enumerate(zip(systems, dates)):
        if s and s not in ("nan", "none", "") and pd.notna(d):
            sys_index[s].append((i, d))

    for sys_name, entries in sys_index.items():
        if len(entries) < 3:
            continue
        entries.sort(key=lambda x: x[1])
        dates_arr = [e[1] for e in entries]
        for start_i in range(len(entries)):
            end_i = start_i
            while (end_i + 1 < len(entries) and
                   (dates_arr[end_i + 1] - dates_arr[start_i]).days <= 60):
                end_i += 1
            if end_i - start_i + 1 >= 3:
                n_in_window = end_i - start_i + 1
                for j in range(start_i, end_i + 1):
                    row_idx = entries[j][0]
                    if scores[row_idx] == 0.0:
                        scores[row_idx] = _DCI_SEVERITY_SCORE["High"]
                        rationales[row_idx] = (
                            f"System '{sys_name}' had {n_in_window} deviations "
                            "in 60 days — concentrated issue pattern. Annex 11 "
                            "§10 requires change management to address systemic "
                            "deficiencies."
                        )
                break
    return (pd.Series(scores, index=df.index),
            pd.Series(rationales, index=df.index))


# ═══════════════════════════════════════════════════════════════════════════
#  ENGINE B — Weak Investigation (Rules 4-8)
# ═══════════════════════════════════════════════════════════════════════════
def _dci_rule4_short_rca(row):
    """Rule 4 — Short RCA Narrative. Fires on Closed records with non-blank
    RCA text shorter than 50 chars. High severity."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    rca = str(row.get("rca_text", "")).strip()
    if not rca or rca.lower() in ("nan", "none"):
        return 0.0, ""
    if len(rca) < 50:
        return _DCI_SEVERITY_SCORE["High"], (
            f"RCA text is only {len(rca)} characters — insufficient detail "
            "to document root cause analysis per 21 CFR 820.100(b)(4) and "
            "ICH Q10 §3.2.2."
        )
    return 0.0, ""


def _dci_rule5_vague_rca(row):
    """Rule 5 — Vague RCA (generic cause only). Per Spec v1.3 §4.3.
    Fires when: status=Closed AND rca not blank AND vague_term_present
    AND NOT specific_cause_present. Substring match, case-insensitive."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    rca = str(row.get("rca_text", "")).strip().lower()
    if not rca or rca in ("nan", "none"):
        return 0.0, ""

    vague_present    = any(v in rca for v in _DCI_VAGUE_RCA_TERMS)
    specific_present = any(s in rca for s in _DCI_SPECIFIC_CAUSE_TERMS)

    if vague_present and not specific_present:
        matched_vague = [v for v in _DCI_VAGUE_RCA_TERMS if v in rca]
        return _DCI_SEVERITY_SCORE["Critical"], (
            f"RCA attributes cause to '{matched_vague[0]}' without specific "
            "equipment, procedural, material, or process factor. "
            "FDA CAPA Guidance (2014) requires root-cause identification "
            "beyond human-factor attribution."
        )
    return 0.0, ""


def _dci_rule6_missing_rca(row):
    """Rule 6 — Missing RCA. Closed with blank rca_text. Critical."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    rca = row.get("rca_text")
    is_blank = (
        rca is None
        or (isinstance(rca, float) and pd.isna(rca))
        or str(rca).strip().lower() in ("", "nan", "none")
    )
    if is_blank:
        return _DCI_SEVERITY_SCORE["Critical"], (
            "Record closed with no root cause analysis recorded. "
            "21 CFR 820.100(b)(2) requires investigation of the cause "
            "of nonconformities."
        )
    return 0.0, ""


def _dci_rule7_missing_capa(row):
    """Rule 7 — Missing CAPA. Closed with blank capa_text. Critical."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    capa = row.get("capa_text")
    is_blank = (
        capa is None
        or (isinstance(capa, float) and pd.isna(capa))
        or str(capa).strip().lower() in ("", "nan", "none")
    )
    if is_blank:
        return _DCI_SEVERITY_SCORE["Critical"], (
            "Record closed with no corrective/preventive action recorded. "
            "21 CFR 820.100(a)(3) requires identification of action needed "
            "to correct and prevent recurrence."
        )
    return 0.0, ""


def _dci_rule8_weak_capa(row):
    """Rule 8 — Weak CAPA (training-only). Per Spec v1.3 §4.3.
    Fires when: status=Closed AND capa not blank AND training term present
    AND NOT action term present."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    capa = str(row.get("capa_text", "")).strip().lower()
    if not capa or capa in ("nan", "none"):
        return 0.0, ""

    training_present = any(t in capa for t in _DCI_TRAINING_ONLY_TERMS)
    action_present   = any(a in capa for a in _DCI_CAPA_ACTION_TERMS)

    if training_present and not action_present:
        return _DCI_SEVERITY_SCORE["High"], (
            "CAPA addresses the issue only via training (no procedural, "
            "engineering, or material change). Training-only CAPA for "
            "non-training-root-cause deviations typically indicates weak "
            "corrective action. ICH Q10 §3.2.3."
        )
    return 0.0, ""


# ═══════════════════════════════════════════════════════════════════════════
#  ENGINE C — CAPA Effectiveness (Rules 9-11)
# ═══════════════════════════════════════════════════════════════════════════
def _dci_rule9_repeat_post_closure(df):
    """Rule 9 — Repeat Deviation Post-Closure. Same record_type +
    system_name + deviation_category re-opens within 90 days of prior
    closure. Critical severity."""
    n = len(df)
    scores    = [0.0] * n
    rationales = [""]  * n

    required = {"record_type", "system_name", "deviation_category",
                "open_date", "close_date", "status"}
    if not required.issubset(df.columns):
        return pd.Series(scores, index=df.index), pd.Series(rationales, index=df.index)

    rtypes   = df["record_type"].astype(str).str.strip().str.lower()
    syss     = df["system_name"].astype(str).str.strip().str.lower()
    cats     = df["deviation_category"].astype(str).str.strip().str.lower()
    opens    = pd.to_datetime(df["open_date"],  errors="coerce")
    closes   = pd.to_datetime(df["close_date"], errors="coerce")
    statuses = df["status"].apply(_dci_normalize_status)
    rids     = df.get("record_id", pd.Series([""] * n)).astype(str)

    from collections import defaultdict
    key_index = defaultdict(list)
    for i in range(n):
        if not (rtypes.iloc[i] and syss.iloc[i] and cats.iloc[i]):
            continue
        if rtypes.iloc[i] in ("nan", "none", ""):
            continue
        key = (rtypes.iloc[i], syss.iloc[i], cats.iloc[i])
        key_index[key].append((
            i, opens.iloc[i], closes.iloc[i],
            statuses.iloc[i], rids.iloc[i]
        ))

    for key, entries in key_index.items():
        if len(entries) < 2:
            continue
        entries.sort(key=lambda x: (x[1] if pd.notna(x[1]) else pd.Timestamp.max))
        for i_cur in range(1, len(entries)):
            cur = entries[i_cur]
            cur_idx, cur_open = cur[0], cur[1]
            if pd.isna(cur_open):
                continue
            for i_prev in range(i_cur):
                prev = entries[i_prev]
                prev_idx, prev_open, prev_close, prev_status, prev_rid = prev
                if prev_status != "closed" or pd.isna(prev_close):
                    continue
                delta = (cur_open - prev_close).days
                if 0 <= delta <= 90:
                    if scores[cur_idx] == 0.0:
                        scores[cur_idx] = _DCI_SEVERITY_SCORE["Critical"]
                        rationales[cur_idx] = (
                            f"Same {key[0]} recurred for {key[1]} in "
                            f"category '{key[2]}' {delta} days after previous "
                            f"closure ({prev_rid} closed "
                            f"{prev_close.strftime('%Y-%m-%d')}). Indicates "
                            "prior CAPA was ineffective. 21 CFR 820.100(a)(2), "
                            "ICH Q10 §3.2.3."
                        )
                    break
    return (pd.Series(scores, index=df.index),
            pd.Series(rationales, index=df.index))


def _dci_rule10_reopened_capa(row):
    """Rule 10 — Re-opened CAPA. Status normalizes to 'reopened'. High."""
    status = _dci_normalize_status(row.get("status"))
    if status == "reopened":
        return _DCI_SEVERITY_SCORE["High"], (
            "Record status is 'Re-opened' — prior closure was invalid or "
            "CAPA ineffective. Per 21 CFR 820.100(a)(7), re-opening requires "
            "documented re-investigation."
        )
    return 0.0, ""


def _dci_rule11_short_close(row):
    """Rule 11 — Short Close Cycle. Closed with <3 days between open and
    close. Default OFF. Medium severity."""
    status = _dci_normalize_status(row.get("status"))
    if status != "closed":
        return 0.0, ""
    od = _dci_parse_date(row.get("open_date"))
    cd = _dci_parse_date(row.get("close_date"))
    if pd.isna(od) or pd.isna(cd):
        return 0.0, ""
    days = (cd - od).days
    if days < 3:
        return _DCI_SEVERITY_SCORE["Medium"], (
            f"Record closed within {days} day(s) of opening. Investigations "
            "typically require multi-day evidence gathering; rapid closure "
            "may indicate insufficient review. ICH Q10 §3.2.2."
        )
    return 0.0, ""


# ═══════════════════════════════════════════════════════════════════════════
#  ENGINE D — SLA / Aging Risk (Rules 12-14)
# ═══════════════════════════════════════════════════════════════════════════
def _dci_rule12_overdue(row, today=None):
    """Rule 12 — Overdue. open_date+sla_days < today AND status != Closed.
    High severity."""
    if today is None:
        today = pd.Timestamp.now().normalize()
    status = _dci_normalize_status(row.get("status"))
    if status == "closed":
        return 0.0, ""
    od = _dci_parse_date(row.get("open_date"))
    if pd.isna(od):
        return 0.0, ""
    sla_raw = row.get("sla_days")
    try:
        sla = float(sla_raw)
        if pd.isna(sla):
            return 0.0, ""
    except (TypeError, ValueError):
        return 0.0, ""
    due_date = od + pd.Timedelta(days=sla)
    if today > due_date:
        days_overdue = (today - due_date).days
        return _DCI_SEVERITY_SCORE["High"], (
            f"Record open for {days_overdue} day(s) past SLA "
            f"({int(sla)}-day window). Overdue investigations breach "
            "21 CFR 820.100(b)(5) timeliness and Annex 11 §10 "
            "change-management timing."
        )
    return 0.0, ""


def _dci_rule13_near_breach(row, today=None):
    """Rule 13 — Near-Breach. Within 7 days of SLA AND still open.
    Medium severity."""
    if today is None:
        today = pd.Timestamp.now().normalize()
    status = _dci_normalize_status(row.get("status"))
    if status == "closed":
        return 0.0, ""
    od = _dci_parse_date(row.get("open_date"))
    if pd.isna(od):
        return 0.0, ""
    sla_raw = row.get("sla_days")
    try:
        sla = float(sla_raw)
        if pd.isna(sla):
            return 0.0, ""
    except (TypeError, ValueError):
        return 0.0, ""
    due_date = od + pd.Timedelta(days=sla)
    days_until = (due_date - today).days
    if 0 <= days_until <= 7:
        return _DCI_SEVERITY_SCORE["Medium"], (
            f"Record within {days_until} day(s) of SLA breach and still open. "
            "Near-breach status per FDA CAPA Guidance (2014) — proactive "
            "escalation recommended."
        )
    return 0.0, ""


def _dci_rule14_no_activity(row, today=None):
    """Rule 14 — No Activity (snapshot fallback). Open >=30 days with no
    close_date. Default OFF. Medium severity."""
    if today is None:
        today = pd.Timestamp.now().normalize()
    status = _dci_normalize_status(row.get("status"))
    if status == "closed":
        return 0.0, ""
    od = _dci_parse_date(row.get("open_date"))
    if pd.isna(od):
        return 0.0, ""
    cd = _dci_parse_date(row.get("close_date"))
    if pd.notna(cd):
        return 0.0, ""
    days_open = (today - od).days
    if days_open >= 30:
        return _DCI_SEVERITY_SCORE["Medium"], (
            f"Record open {days_open} days with no closure. Without "
            "status-change history, sustained-inactivity detection uses "
            "duration proxy per ICH Q10 §3.2.2."
        )
    return 0.0, ""


# ═══════════════════════════════════════════════════════════════════════════
#  Orchestrator
# ═══════════════════════════════════════════════════════════════════════════
def dci_score_records(df, rule_config=None):
    """Score every DCI record across 14 rules. Returns sorted DataFrame
    with individual scores, rule flags, Risk_Tier, Risk_Score,
    Primary_Rule, All_Rules_Fired, Detection_Basis."""
    if rule_config is None:
        rule_config = dict(_DCI_RULE_DEFAULTS)

    df = df.copy()
    today = pd.Timestamp.now().normalize()

    # Vectorized rules
    s1, r1 = _dci_rule1_recurring_category(df)
    s2, r2 = _dci_rule2_repeat_category_hivol(df)
    s3, r3 = _dci_rule3_repeat_system(df)
    s9, r9 = _dci_rule9_repeat_post_closure(df)
    df["score_dci_rule1_recurring_category"]    = s1
    df["rule1_rationale"]                        = r1
    df["score_dci_rule2_repeat_category_hivol"] = s2
    df["rule2_rationale"]                        = r2
    df["score_dci_rule3_repeat_system"]         = s3
    df["rule3_rationale"]                        = r3
    df["score_dci_rule9_repeat_post_closure"]   = s9
    df["rule9_rationale"]                        = r9

    # Per-row rules
    per_row_rules = [
        ("score_dci_rule4_short_rca",       "rule4_rationale",   _dci_rule4_short_rca,      False),
        ("score_dci_rule5_vague_rca",       "rule5_rationale",   _dci_rule5_vague_rca,      False),
        ("score_dci_rule6_missing_rca",     "rule6_rationale",   _dci_rule6_missing_rca,    False),
        ("score_dci_rule7_missing_capa",    "rule7_rationale",   _dci_rule7_missing_capa,   False),
        ("score_dci_rule8_weak_capa",       "rule8_rationale",   _dci_rule8_weak_capa,      False),
        ("score_dci_rule10_reopened_capa",  "rule10_rationale",  _dci_rule10_reopened_capa, False),
        ("score_dci_rule11_short_close",    "rule11_rationale",  _dci_rule11_short_close,   False),
        ("score_dci_rule12_overdue",        "rule12_rationale",  _dci_rule12_overdue,       True),
        ("score_dci_rule13_near_breach",    "rule13_rationale",  _dci_rule13_near_breach,   True),
        ("score_dci_rule14_no_activity",    "rule14_rationale",  _dci_rule14_no_activity,   True),
    ]
    for score_col, rat_col, fn, needs_today in per_row_rules:
        scores, rats = [], []
        for _, row in df.iterrows():
            try:
                s, r = fn(row, today=today) if needs_today else fn(row)
            except Exception:
                s, r = 0.0, ""
            scores.append(s)
            rats.append(r)
        df[score_col] = scores
        df[rat_col]   = rats

    # Zero out disabled rules
    for cfg_key, score_cols in _DCI_CFG_SCORE_MAP.items():
        if not rule_config.get(cfg_key, True):
            for sc in score_cols:
                if sc in df.columns:
                    df[sc] = 0.0

    # Aggregate to Risk_Tier
    _TIER_RANK = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}

    def _priority_tier_and_rule(row):
        best_tier  = "Low"
        best_score = 0.0
        best_col   = ""
        for score_col, threshold, base_tier in _DCI_RULE_TIER_PRIORITY:
            val = float(row.get(score_col, 0))
            if val >= threshold:
                if _TIER_RANK.get(base_tier, 3) < _TIER_RANK.get(best_tier, 3):
                    best_tier  = base_tier
                    best_score = val
                    best_col   = score_col
                elif (_TIER_RANK.get(base_tier, 3) == _TIER_RANK.get(best_tier, 3)
                      and val > best_score):
                    best_score = val
                    best_col   = score_col
        return best_tier, round(best_score, 2), best_col

    _triples = [_priority_tier_and_rule(row) for _, row in df.iterrows()]
    df["Risk_Tier"]    = [t[0] for t in _triples]
    df["Risk_Score"]   = [t[1] for t in _triples]
    df["Primary_Rule"] = [_DCI_RULE_DISPLAY_NAMES.get(t[2], "—") for t in _triples]

    def _rules_fired(row):
        fired = []
        for score_col, threshold, _ in _DCI_RULE_TIER_PRIORITY:
            if float(row.get(score_col, 0)) >= threshold:
                fired.append(_DCI_RULE_DISPLAY_NAMES.get(score_col, score_col))
        return "; ".join(fired) if fired else ""

    def _detection_basis(row):
        parts = []
        for rn in range(1, 15):
            rat = str(row.get(f"rule{rn}_rationale", "")).strip()
            if rat:
                parts.append(rat)
        return "  ||  ".join(parts) if parts else ""

    df["All_Rules_Fired"] = df.apply(_rules_fired, axis=1)
    df["Detection_Basis"] = df.apply(_detection_basis, axis=1)

    df["_tier_rank"] = df["Risk_Tier"].map(_TIER_RANK).fillna(3).astype(int)
    df = df.sort_values(
        ["_tier_rank", "Risk_Score"], ascending=[True, False]
    ).drop(columns=["_tier_rank"]).reset_index(drop=True)

    return df


# ═══════════════════════════════════════════════════════════════════════════
#  Excel builder — 6 sheets per Spec v1.3 §6
# ═══════════════════════════════════════════════════════════════════════════
def dci_build_excel(scored_df, system_name, r_start, r_end, fname,
                     config_hash="", operator_user="", model_used="",
                     rule_config=None):
    """Build 6-sheet GxP evidence workbook for DCI findings."""
    if rule_config is None:
        rule_config = dict(_DCI_RULE_DEFAULTS)

    output = io.BytesIO()
    wb     = Workbook()

    C_NAVY      = "1E3A5F"
    C_WHITE     = "FFFFFF"
    C_AMBER     = "D97706"
    C_RED       = "C0392B"
    C_ORANGE    = "EA580C"
    C_GREY      = "F1F5F9"
    C_MID       = "94A3B8"
    C_LIGHT     = "EFF6FF"
    C_DARK_TEXT = "2C3E50"

    _TIER_COLORS = {
        "Critical": (C_RED,    C_WHITE),
        "High":     (C_ORANGE, C_WHITE),
        "Medium":   (C_AMBER,  C_WHITE),
        "Low":      (C_GREY,   C_DARK_TEXT),
    }

    bdr = Border(
        left=Side(style="thin", color="D1D5DB"),
        right=Side(style="thin", color="D1D5DB"),
        top=Side(style="thin", color="D1D5DB"),
        bottom=Side(style="thin", color="D1D5DB"),
    )

    def _fill(hex_color):
        return PatternFill("solid", fgColor=hex_color)

    def _hdr(ws, row, col, val, width=None, bg=C_NAVY, fg=C_WHITE, size=9, wrap=False):
        c = ws.cell(row=row, column=col, value=val)
        c.font = Font(name="Calibri", bold=True, size=size, color=fg)
        c.fill = _fill(bg)
        c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=wrap)
        c.border = bdr
        if width:
            ws.column_dimensions[get_column_letter(col)].width = width
        return c

    def _cell(ws, row, col, val, bold=False, bg=None, fg=C_DARK_TEXT,
              size=9, wrap=False, align="left"):
        c = ws.cell(row=row, column=col, value=val)
        c.font = Font(name="Calibri", bold=bold, size=size, color=fg)
        c.fill = _fill(bg) if bg else PatternFill()
        c.alignment = Alignment(horizontal=align, vertical="top", wrap_text=wrap)
        c.border = bdr
        return c

    n_total    = len(scored_df)
    n_critical = int((scored_df["Risk_Tier"] == "Critical").sum()) if n_total else 0
    n_high     = int((scored_df["Risk_Tier"] == "High").sum())     if n_total else 0
    n_medium   = int((scored_df["Risk_Tier"] == "Medium").sum())   if n_total else 0
    n_hc       = n_critical + n_high

    # ── Sheet 1 — Summary ───────────────────────────────────────────────
    ws1 = wb.active
    ws1.title = "Summary"
    ws1.sheet_view.showGridLines = False

    ws1.cell(row=1, column=1, value="VALINTEL.AI — Deviation & CAPA Investigation"
    ).font = Font(name="Calibri", bold=True, size=14, color=C_NAVY)
    ws1.cell(row=2, column=1,
             value=f"System: {system_name}   ·   Review Period: {r_start} → {r_end}"
    ).font = Font(name="Calibri", size=10, color=C_DARK_TEXT)
    ws1.cell(row=3, column=1,
             value=f"Source File: {fname}"
    ).font = Font(name="Calibri", size=9, italic=True, color=C_MID)

    ws1.row_dimensions[1].height = 22
    ws1.column_dimensions["A"].width = 26
    ws1.column_dimensions["B"].width = 60

    _hdr(ws1, 5, 1, "KPI", width=26)
    _hdr(ws1, 5, 2, "Value", width=60)

    kpi_rows = [
        ("Records Analyzed",       str(n_total)),
        ("Critical Findings",      str(n_critical)),
        ("High Findings",          str(n_high)),
        ("Medium Findings",        str(n_medium)),
        ("Records Requiring Review", f"{n_hc} (Critical + High)"),
    ]
    for i, (k, v) in enumerate(kpi_rows, 6):
        _cell(ws1, i, 1, k, bold=True, bg=C_LIGHT)
        _cell(ws1, i, 2, v)
        ws1.row_dimensions[i].height = 16

    _hdr(ws1, 12, 1, "Top Rules Fired")
    _hdr(ws1, 12, 2, "Count")
    if n_total:
        rule_counts = {}
        for score_col, threshold, _ in _DCI_RULE_TIER_PRIORITY:
            if score_col in scored_df.columns:
                cnt = int((scored_df[score_col] >= threshold).sum())
                if cnt > 0:
                    disp = _DCI_RULE_DISPLAY_NAMES.get(score_col, score_col)
                    rule_counts[disp] = cnt
        top3 = sorted(rule_counts.items(), key=lambda x: -x[1])[:3]
        for i, (rname, cnt) in enumerate(top3, 13):
            _cell(ws1, i, 1, rname)
            _cell(ws1, i, 2, str(cnt), align="center")
            ws1.row_dimensions[i].height = 15
    else:
        _cell(ws1, 13, 1, "No findings", fg=C_MID)

    _hdr(ws1, 18, 1, "Regulatory References")
    _cell(ws1, 19, 1, _REG_DCI, wrap=True)
    ws1.merge_cells(start_row=19, start_column=1, end_row=19, end_column=2)
    ws1.row_dimensions[19].height = 48

    _cell(ws1, 22, 1, f"Config Hash: {config_hash or 'n/a'}",
          fg=C_MID, size=8)

    # ── Sheet 2 — Records for Review ────────────────────────────────────
    ws2 = wb.create_sheet("Records for Review")
    ws2.sheet_view.showGridLines = False

    review_df = scored_df[scored_df["Risk_Tier"].isin(
        ["Critical", "High", "Medium"])].copy()

    review_cols = [
        ("Record ID",       "record_id",           18),
        ("Type",            "record_type",         14),
        ("Category",        "deviation_category",  20),
        ("System",          "system_name",         18),
        ("Open Date",       "open_date",           14),
        ("Close Date",      "close_date",          14),
        ("Status",          "status",              12),
        ("Assigned To",     "assigned_to",         18),
        ("Approved By",     "approved_by",         18),
        ("Risk Tier",       "Risk_Tier",           12),
        ("Risk Score",      "Risk_Score",          11),
        ("Primary Rule",    "Primary_Rule",        36),
        ("All Rules Fired", "All_Rules_Fired",     50),
        ("Detection Basis", "Detection_Basis",     80),
    ]

    ws2.cell(row=1, column=1,
             value=f"Records for Review — {len(review_df)} record(s) "
                   f"at Medium or higher risk"
    ).font = Font(name="Calibri", bold=True, size=12, color=C_NAVY)
    ws2.row_dimensions[1].height = 20

    for ci, (hdr, _, width) in enumerate(review_cols, 1):
        _hdr(ws2, 3, ci, hdr, width=width)
    ws2.row_dimensions[3].height = 18

    for ri, (_, row_data) in enumerate(review_df.iterrows(), 4):
        tier = str(row_data.get("Risk_Tier", "Low"))
        tier_bg, tier_fg = _TIER_COLORS.get(tier, (C_GREY, C_DARK_TEXT))
        for ci, (_, col_key, _) in enumerate(review_cols, 1):
            val = row_data.get(col_key, "")
            if pd.isna(val):
                val = ""
            if col_key in ("open_date", "close_date"):
                try:
                    dt = pd.to_datetime(val, errors="coerce")
                    val = dt.strftime("%Y-%m-%d") if pd.notna(dt) else ""
                except Exception:
                    val = str(val) if val else ""
            c = ws2.cell(row=ri, column=ci, value=val)
            c.font = Font(name="Calibri", size=9, color=C_DARK_TEXT)
            c.alignment = Alignment(
                horizontal="center" if ci > 1 else "left",
                vertical="top",
                wrap_text=col_key in ("Detection_Basis", "All_Rules_Fired",
                                      "Primary_Rule"))
            c.border = bdr
            if col_key == "Risk_Tier":
                c.font = Font(name="Calibri", bold=True, size=9, color=tier_fg)
                c.fill = _fill(tier_bg)
            elif ri % 2 == 0:
                c.fill = _fill("F8FAFC")
        ws2.row_dimensions[ri].height = 36

    ws2.freeze_panes = "A4"

    # ── Sheet 3 — Full Log ──────────────────────────────────────────────
    ws3 = wb.create_sheet("Full Log")
    ws3.sheet_view.showGridLines = False
    ws3.cell(row=1, column=1,
             value=f"Full Record Log — {n_total} record(s), all tiers"
    ).font = Font(name="Calibri", bold=True, size=12, color=C_NAVY)
    ws3.row_dimensions[1].height = 20

    full_cols = [
        ("Record ID",     "record_id",           16),
        ("Type",          "record_type",         14),
        ("Category",      "deviation_category",  18),
        ("System",        "system_name",         16),
        ("Open Date",     "open_date",           12),
        ("Close Date",    "close_date",          12),
        ("Status",        "status",              12),
        ("SLA Days",      "sla_days",            10),
        ("Assigned To",   "assigned_to",         16),
        ("Approved By",   "approved_by",         16),
        ("RCA Text",      "rca_text",            40),
        ("CAPA Text",     "capa_text",           40),
        ("Risk Tier",     "Risk_Tier",           10),
        ("Risk Score",    "Risk_Score",          10),
        ("Primary Rule",  "Primary_Rule",        30),
    ]
    for sc, _, _ in _DCI_RULE_TIER_PRIORITY:
        rn = sc.split("rule")[1].split("_")[0]
        full_cols.append((f"R{rn} Score", sc, 9))

    for ci, (hdr, _, width) in enumerate(full_cols, 1):
        _hdr(ws3, 3, ci, hdr, width=width)
    ws3.row_dimensions[3].height = 18

    for ri, (_, row_data) in enumerate(scored_df.iterrows(), 4):
        tier = str(row_data.get("Risk_Tier", "Low"))
        tier_bg, tier_fg = _TIER_COLORS.get(tier, (C_GREY, C_DARK_TEXT))
        for ci, (_, col_key, _) in enumerate(full_cols, 1):
            val = row_data.get(col_key, "")
            if pd.isna(val):
                val = ""
            if col_key in ("open_date", "close_date"):
                try:
                    dt = pd.to_datetime(val, errors="coerce")
                    val = dt.strftime("%Y-%m-%d") if pd.notna(dt) else ""
                except Exception:
                    val = str(val) if val else ""
            c = ws3.cell(row=ri, column=ci, value=val)
            c.font = Font(name="Calibri", size=8.5, color=C_DARK_TEXT)
            c.alignment = Alignment(
                horizontal="center" if ci > 1 else "left",
                vertical="top",
                wrap_text=col_key in ("rca_text", "capa_text"))
            c.border = bdr
            if col_key == "Risk_Tier":
                c.font = Font(name="Calibri", bold=True, size=9, color=tier_fg)
                c.fill = _fill(tier_bg)
            elif ri % 2 == 0:
                c.fill = _fill("F8FAFC")
        ws3.row_dimensions[ri].height = 22

    ws3.freeze_panes = "A4"

    # ── Sheet 4 — Detection Logic ───────────────────────────────────────
    ws4 = wb.create_sheet("Detection Logic")
    ws4.sheet_view.showGridLines = False
    ws4.column_dimensions["A"].width = 6
    ws4.column_dimensions["B"].width = 42
    ws4.column_dimensions["C"].width = 12
    ws4.column_dimensions["D"].width = 10
    ws4.column_dimensions["E"].width = 12
    ws4.column_dimensions["F"].width = 70

    ws4.cell(row=1, column=1, value="DCI Detection Rules — 14 Rules Across 4 Engines"
    ).font = Font(name="Calibri", bold=True, size=12, color=C_NAVY)
    ws4.row_dimensions[1].height = 20

    for ci, hdr in enumerate(
        ["#", "Rule Name", "Severity", "Tier", "Engine", "Fire Condition"], 1
    ):
        _hdr(ws4, 3, ci, hdr)
    ws4.row_dimensions[3].height = 18

    engine_labels = {
        "A": "RCA Recurrence",
        "B": "Weak Investigation",
        "C": "CAPA Effectiveness",
        "D": "SLA / Aging",
    }
    fire_conditions = {
        1:  "Same deviation_category in >=3 records within 180-day window.",
        2:  "Same deviation_category in >=5 records within 90-day window.",
        3:  "Same system_name has >=3 deviations within 60-day window.",
        4:  "Closed record with len(rca_text.strip()) < 50 chars.",
        5:  "Closed record, RCA contains vague term AND no specific-cause term. "
            "Substring match, case-insensitive. Non-blank RCA only.",
        6:  "Closed record with blank/null rca_text.",
        7:  "Closed record with blank/null capa_text.",
        8:  "Closed record, CAPA contains training term AND no action term. "
            "Substring match. Non-blank CAPA only.",
        9:  "Same record_type + system_name + deviation_category re-opens within "
            "90 days of a prior closure.",
        10: "Status normalizes to 're-opened' / 'reopened'.",
        11: "Closed record, close_date - open_date < 3 days. "
            "Both dates required.",
        12: "open_date + sla_days < today AND status != Closed. "
            "Requires numeric sla_days.",
        13: "open_date + sla_days - today in [0, 7] AND status != Closed.",
        14: "Open >= 30 days, no close_date, status != Closed.",
    }

    for ri, (num, name, sev, tier, eng, dflt, cfg_key) in enumerate(
        _DCI_RULE_META, 4
    ):
        _cell(ws4, ri, 1, str(num), bold=True, align="center")
        _cell(ws4, ri, 2, name)
        sev_bg, sev_fg = _TIER_COLORS.get(sev, (C_GREY, C_DARK_TEXT))
        c = _cell(ws4, ri, 3, sev, bold=True, align="center")
        c.font = Font(name="Calibri", bold=True, size=9, color=sev_fg)
        c.fill = _fill(sev_bg)
        _cell(ws4, ri, 4, tier, align="center")
        _cell(ws4, ri, 5, f"{eng} — {engine_labels.get(eng, eng)}", align="center")
        _cell(ws4, ri, 6, fire_conditions.get(num, ""), wrap=True)
        ws4.row_dimensions[ri].height = 30

    ri_ai = 4 + len(_DCI_RULE_META) + 2
    _hdr(ws4, ri_ai, 1, "AI Use in Scoring")
    ws4.merge_cells(start_row=ri_ai, start_column=1, end_row=ri_ai, end_column=6)
    _cell(ws4, ri_ai + 1, 1,
          "No AI is used in DCI rule scoring or tier assignment. All 14 rules "
          "are deterministic Python logic operating on keyword/date checks. "
          "AI (if present in narrative generation for Summary) is advisory text "
          "only — Risk_Score, Risk_Tier, and rule firing are fully reproducible "
          "from config hash + input file hash. Per ISPE GAMP® Guide: Artificial "
          "Intelligence (July 2025).",
          wrap=True)
    ws4.merge_cells(start_row=ri_ai+1, start_column=1,
                    end_row=ri_ai+1, end_column=6)
    ws4.row_dimensions[ri_ai + 1].height = 60

    # ── Sheet 5 — Integrity Audit ────────────────────────────────────────
    ws5 = wb.create_sheet("Integrity Audit")
    ws5.sheet_view.showGridLines = False
    ws5.column_dimensions["A"].width = 28
    ws5.column_dimensions["B"].width = 70

    ws5.cell(row=1, column=1, value="DCI Integrity Audit"
    ).font = Font(name="Calibri", bold=True, size=12, color=C_NAVY)
    ws5.row_dimensions[1].height = 20

    try:
        file_hash = hashlib.sha256(str(fname).encode()).hexdigest()[:16]
    except Exception:
        file_hash = "—"

    audit_rows = [
        ("Module",                 "Deviation & CAPA Investigation"),
        ("Source File",            fname),
        ("Source File Hash",       file_hash),
        ("System Name",            system_name),
        ("Review Period",          f"{r_start} → {r_end}"),
        ("Analysis Timestamp",     pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")),
        ("Operator",               operator_user or "—"),
        ("Config Hash",            config_hash or "—"),
        ("Model Used (narrative)", model_used or "n/a — no AI in scoring"),
        ("Records Analyzed",       str(n_total)),
        ("Critical Findings",      str(n_critical)),
        ("High Findings",          str(n_high)),
        ("Medium Findings",        str(n_medium)),
        ("Regulatory References",  _REG_DCI),
    ]
    for ri, (k, v) in enumerate(audit_rows, 3):
        _cell(ws5, ri, 1, k, bold=True, bg=C_LIGHT)
        _cell(ws5, ri, 2, v, wrap=True)
        ws5.row_dimensions[ri].height = 18 if len(str(v)) < 60 else 32

    # ── Sheet 6 — Compliance Checklist ───────────────────────────────────
    ws6 = wb.create_sheet("Compliance Checklist")
    ws6.sheet_view.showGridLines = False
    ws6.column_dimensions["A"].width = 6
    ws6.column_dimensions["B"].width = 40
    ws6.column_dimensions["C"].width = 42
    ws6.column_dimensions["D"].width = 14

    ws6.cell(row=1, column=1,
             value="DCI Compliance Checklist — Fired Rules Mapped to Regulatory Clauses"
    ).font = Font(name="Calibri", bold=True, size=12, color=C_NAVY)
    ws6.row_dimensions[1].height = 20

    for ci, hdr in enumerate(["#", "Rule", "Regulatory Clause", "Count"], 1):
        _hdr(ws6, 3, ci, hdr)
    ws6.row_dimensions[3].height = 18

    reg_map = {
        1:  "ICH Q10 §3.2 · 21 CFR 820.100(a)(2)",
        2:  "21 CFR 820.100(a)(2) · ICH Q10 §3.2.3",
        3:  "EU Annex 11 §10 · ICH Q10 §3.2",
        4:  "21 CFR 820.100(b)(4) · ICH Q10 §3.2.2",
        5:  "FDA CAPA Guidance (2014) · 21 CFR 820.100(b)(2)",
        6:  "21 CFR 820.100(b)(2) · 21 CFR 211.192",
        7:  "21 CFR 820.100(a)(3) · 21 CFR 211.192",
        8:  "ICH Q10 §3.2.3 · FDA CAPA Guidance (2014)",
        9:  "21 CFR 820.100(a)(2) · ICH Q10 §3.2.3",
        10: "21 CFR 820.100(a)(7)",
        11: "ICH Q10 §3.2.2",
        12: "21 CFR 820.100(b)(5) · EU Annex 11 §10",
        13: "FDA CAPA Guidance (2014)",
        14: "ICH Q10 §3.2.2",
    }

    rule_fire_counts = {}
    for sc, thr, _ in _DCI_RULE_TIER_PRIORITY:
        if sc in scored_df.columns and n_total:
            cnt = int((scored_df[sc] >= thr).sum())
            rn = int(sc.split("rule")[1].split("_")[0])
            rule_fire_counts[rn] = cnt

    for ri, (num, name, _, _, _, _, _) in enumerate(_DCI_RULE_META, 4):
        _cell(ws6, ri, 1, str(num), bold=True, align="center")
        _cell(ws6, ri, 2, name)
        _cell(ws6, ri, 3, reg_map.get(num, ""), wrap=True)
        cnt = rule_fire_counts.get(num, 0)
        cell_cnt = _cell(ws6, ri, 4, str(cnt), align="center", bold=(cnt > 0))
        if cnt > 0:
            cell_cnt.fill = _fill("FEF2F2" if cnt >= 5 else "FFF7ED")
        ws6.row_dimensions[ri].height = 24

    wb.save(output)
    return output.getvalue()


# ═══════════════════════════════════════════════════════════════════════════
#  DIM banking hook
# ═══════════════════════════════════════════════════════════════════════════
def _dci_bank_to_dim(scored_df, period_label, system_name, file_name,
                     event_category_fn=None):
    """Bank H/C DCI findings to st.session_state.dim_accumulated_rows.

    event_category_fn: callable that maps Rule_Triggered -> Event_Category.
    When called from generator.py, pass the _dim_event_category function so
    DCI rule names get properly classified to Investigation / Change Control.
    """
    if event_category_fn is None:
        # Fallback — use "Investigation" as default category for DCI findings
        event_category_fn = lambda rt: "Investigation"

    hc_df = scored_df[
        scored_df["Risk_Tier"].isin(["Critical", "High"])
    ] if not scored_df.empty else pd.DataFrame()

    dci_dim_rows = []
    for _, row in hc_df.iterrows():
        status_norm = _dci_normalize_status(row.get("status"))
        cd = _dci_parse_date(row.get("close_date"))
        od = _dci_parse_date(row.get("open_date"))
        if status_norm == "closed" and pd.notna(cd):
            event_ts = str(cd)
        elif pd.notna(od):
            event_ts = str(od)
        else:
            event_ts = ""

        rule_str = str(row.get("Primary_Rule", "DCI finding"))[:120]
        dci_dim_rows.append({
            "Review_Period":   period_label,
            "Username":        str(row.get("assigned_to", "unknown")),
            "Risk_Level":      str(row.get("Risk_Tier", "High")),
            "Rule_Triggered":  rule_str,
            "Event_Category":  event_category_fn(rule_str),
            "System_Name":     str(row.get("system_name", system_name)),
            "Event_Type":      str(row.get("record_type", "DEVIATION")),
            "Event_Timestamp": event_ts,
            "Source_File":     file_name,
            "Source_Module":   "DCI",
        })

    existing = [
        r for r in st.session_state.get("dim_accumulated_rows", [])
        if not (r.get("Review_Period") == period_label
                and r.get("Source_Module") == "DCI")
    ]

    if dci_dim_rows:
        existing.extend(dci_dim_rows)
        banked = len(dci_dim_rows)
    else:
        sentinel = {
            "Review_Period":   period_label,
            "Username":        "(no escalations)",
            "Risk_Level":      "Low",
            "Rule_Triggered":  "No named rules triggered",
            "Event_Category":  "Other",
            "System_Name":     system_name,
            "Event_Type":      "DCI_REVIEW",
            "Event_Timestamp": "",
            "Source_File":     file_name,
            "Source_Module":   "DCI",
        }
        existing.append(sentinel)
        banked = 1

    st.session_state["dim_accumulated_rows"] = existing
    st.session_state["dim_periods_banked"] = len(
        set(r["Review_Period"] for r in existing))
    st.session_state["dim_analysis_done"] = False
    st.session_state["dim_result"] = None

    return banked


# ═══════════════════════════════════════════════════════════════════════════
#  Event_Category classifier — exported for generator.py to use
# ═══════════════════════════════════════════════════════════════════════════
def dci_classify_event_category(rule_triggered):
    """Map a DCI Rule_Triggered label to Event_Category.
    Rules 9,10,11 -> Change Control; Rules 1-8, 12-14 -> Investigation.
    Returns None if the rule is not a DCI rule (caller falls back to
    existing AT/UAR logic)."""
    r = str(rule_triggered).lower()
    if any(x in r for x in (
        "repeat deviation post-closure", "re-opened capa",
        "reopened capa", "short close cycle"
    )):
        return "Change Control"
    if any(x in r for x in (
        "recurring category", "repeat category", "repeat system",
        "short rca", "vague rca", "missing rca", "missing capa",
        "weak capa", "overdue", "near-breach", "no activity"
    )):
        return "Investigation"
    return None  # Not a DCI rule — let caller handle


# ═══════════════════════════════════════════════════════════════════════════
#  UI — main entry point
# ═══════════════════════════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════════════════════════
#  UI helper — render results from session state (no upload required)
# ═══════════════════════════════════════════════════════════════════════════
def _render_dci_results_from_session(user, model_id):
    """Render previously-computed DCI results without requiring re-upload.

    Called from show_dci_review when user navigates back to DCI after
    visiting another module — the file_uploader has been reset to None
    but the analysis is still in session state. Mirrors the active-flow
    Results + Download + Bank sections.
    """
    scored_df = st.session_state.get("dci_scored_df")
    if scored_df is None or scored_df.empty:
        return
    sys_name  = st.session_state.get("dci_system_name", "System")
    file_name = st.session_state.get("dci_file_name", "")
    cfg_hash  = st.session_state.get("dci_config_hash", "")
    cfg       = st.session_state.get("dci_rule_config", dict(_DCI_RULE_DEFAULTS))

    # Reconstruct review-period strings if available
    _ds = st.session_state.get("dci_review_start")
    _de = st.session_state.get("dci_review_end")
    dci_r_start = _ds.strftime("%d-%b-%Y") if _ds else ""
    dci_r_end   = _de.strftime("%d-%b-%Y") if _de else ""

    n_crit = int((scored_df["Risk_Tier"] == "Critical").sum())
    n_high = int((scored_df["Risk_Tier"] == "High").sum())
    n_med  = int((scored_df["Risk_Tier"] == "Medium").sum())
    n_low  = int((scored_df["Risk_Tier"] == "Low").sum())

    st.markdown("---")
    st.markdown("### Results")
    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1: st.metric("Critical", n_crit)
    with kc2: st.metric("High",     n_high)
    with kc3: st.metric("Medium",   n_med)
    with kc4: st.metric("Low",      n_low)

    review_df = scored_df[scored_df["Risk_Tier"].isin(
        ["Critical", "High", "Medium"])]
    if not review_df.empty:
        st.markdown("**Records for Review** (Critical + High + Medium):")
        display_cols = ["record_id", "record_type", "deviation_category",
                        "system_name", "status", "Risk_Tier", "Risk_Score",
                        "Primary_Rule"]
        display_cols = [c for c in display_cols if c in review_df.columns]
        st.dataframe(
            review_df[display_cols].head(50),
            use_container_width=True, hide_index=True,
        )
        if len(review_df) > 50:
            st.caption(f"Showing 50 of {len(review_df)} review records. "
                       "Download Excel for full set.")
    else:
        st.success("✅ No records flagged at Medium or higher.")

    # Download + Bank
    st.markdown("---")
    st.markdown("### Download Evidence Package & Bank to DIM")

    dc1, dc2 = st.columns(2)
    with dc1:
        try:
            xlsx_bytes = dci_build_excel(
                scored_df,
                system_name=sys_name,
                r_start=dci_r_start,
                r_end=dci_r_end,
                fname=file_name,
                config_hash=cfg_hash,
                operator_user=user,
                model_used=model_id,
                rule_config=cfg,
            )
            st.download_button(
                "📥 Download DCI Evidence Package (xlsx)",
                data=xlsx_bytes,
                file_name=f"DCI_{sys_name}_{dci_r_end}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="dci_download_btn_persistent",
                on_click=lambda: _gen()["log_audit"](
                    user, "DCI_DOWNLOAD", "REPORT",
                    new_value=f"DCI_{sys_name}",
                    reason=f"System: {sys_name} (re-download from session)",
                ),
            )
        except Exception as e:
            st.error(f"Excel build failed: {e}")

    with dc2:
        if st.button(
            "🏦 Bank to Data Integrity Monitor (DIM)",
            use_container_width=True,
            key="dci_bank_btn_persistent",
        ):
            try:
                _ec_fn = _gen().get("_dim_event_category")
                period_label = f"{dci_r_start} → {dci_r_end}" \
                    if (dci_r_start and dci_r_end) else \
                    f"DCI Period {st.session_state.get('dim_periods_banked', 0) + 1}"
                banked = _dci_bank_to_dim(
                    scored_df, period_label, sys_name, file_name,
                    event_category_fn=_ec_fn
                )
                if banked == 1 and n_crit == 0 and n_high == 0:
                    st.success(
                        f"✅ Banked sentinel row for clean period "
                        f"({period_label})."
                    )
                else:
                    st.success(
                        f"✅ Banked {banked} High/Critical finding(s) to DIM."
                    )
                _gen()["log_audit"](
                    user, "DCI_BANK_TO_DIM", "DATASET",
                    new_value=f"DCI_{sys_name}",
                    reason=f"Period: {period_label} · Banked: {banked} rows (re-bank from session)",
                )
                st.rerun()
            except Exception as e:
                st.error(f"Banking failed: {e}")


def show_dci_review(user, role, model_id):
    """Render Periodic Review — Module 3: Deviation & CAPA Investigation.
    Mirrors show_user_access_review structure."""
    _gen()["_scroll_top"]()
    st.title("🔍 Deviation & CAPA Investigation")
    st.markdown(
        "<p style='color:#94a3b8;margin-top:-12px;'>"
        "Upload your deviation/CAPA log to run the deterministic investigation "
        "quality engine — 14 scoring rules across 4 engines (RCA recurrence, weak "
        "investigation, CAPA effectiveness, SLA aging). Produces a GxP evidence "
        "package for 21 CFR 820.100, ICH Q10 §3.2, and EU Annex 11 §10.</p>",
        unsafe_allow_html=True,
    )
    st.markdown("---")

    # System metadata
    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.session_state["dci_system_name"] = st.text_input(
            "System Name",
            value=st.session_state.get("dci_system_name", ""),
            placeholder="e.g. TrackWise, MasterControl, Veeva QMS",
            key="dci_sysname",
        )

    def _prev_quarter_dates():
        today = _dt.date.today()
        q = (today.month - 1) // 3 + 1
        if q == 1:
            return (_dt.date(today.year - 1, 10, 1),
                    _dt.date(today.year - 1, 12, 31))
        elif q == 2:
            return (_dt.date(today.year, 1, 1), _dt.date(today.year, 3, 31))
        elif q == 3:
            return (_dt.date(today.year, 4, 1), _dt.date(today.year, 6, 30))
        else:
            return (_dt.date(today.year, 7, 1), _dt.date(today.year, 9, 30))

    _pq_start, _pq_end = _prev_quarter_dates()
    with mc2:
        dci_start = st.date_input(
            "Review Period Start",
            value=st.session_state.get("dci_review_start", _pq_start),
            format="DD/MM/YYYY", key="dci_start_picker",
        )
        st.session_state["dci_review_start"] = dci_start
    with mc3:
        dci_end = st.date_input(
            "Review Period End",
            value=st.session_state.get("dci_review_end", _pq_end),
            format="DD/MM/YYYY", key="dci_end_picker",
        )
        st.session_state["dci_review_end"] = dci_end

    dci_r_start = dci_start.strftime("%d-%b-%Y") if dci_start else ""
    dci_r_end   = dci_end.strftime("%d-%b-%Y")   if dci_end   else ""

    st.markdown("---")

    # Upload
    st.markdown("### 1. Upload Deviation/CAPA Log")
    dci_file = st.file_uploader(
        "Excel or CSV file containing your deviation and CAPA records",
        type=["xlsx", "xls", "csv"],
        key="dci_uploader",
    )

    # ── Persistent-results path ──────────────────────────────────────────────
    # Streamlit's file_uploader resets to None when the user navigates away
    # and comes back. If we have analysis results from a prior run, surface
    # them instead of forcing a fresh upload. User can hit "Clear results"
    # to start over.
    _prior_scored = st.session_state.get("dci_scored_df")
    _prior_done   = st.session_state.get("dci_analysis_done", False)
    _prior_fname  = st.session_state.get("dci_file_name", "")

    if dci_file is None and _prior_done and _prior_scored is not None:
        st.info(
            f"📊 Showing results from previous analysis of "
            f"**{_prior_fname or 'last uploaded file'}**. "
            "Upload a new file or click **Clear results** to start over."
        )
        # Clear button — single click to reset the whole DCI session
        _cc1, _cc2 = st.columns([1, 4])
        with _cc1:
            if st.button("🗑️ Clear results", key="dci_clear_results_btn",
                         use_container_width=True):
                for _k in ("dci_scored_df", "dci_analysis_done",
                           "dci_last_run_fingerprint", "dci_config_hash",
                           "dci_file_name"):
                    if _k in st.session_state:
                        del st.session_state[_k]
                st.rerun()

        # Render results from session state — bypass upload/validate/run flow
        _render_dci_results_from_session(user, model_id)
        return

    if dci_file is None:
        st.info(
            "📋 **Expected columns** (aliases accepted): record_id, record_type, "
            "deviation_category, system_name, open_date, close_date, rca_text, "
            "capa_text, assigned_to, approved_by, status, sla_days"
        )
        return

    try:
        raw_bytes = dci_file.getvalue()
        file_name = dci_file.name
        st.session_state["dci_file_name"] = file_name
        if file_name.lower().endswith(".csv"):
            dci_df_raw = pd.read_csv(io.BytesIO(raw_bytes), dtype=str).fillna("")
            all_sheets = [file_name]
        else:
            xl = pd.ExcelFile(io.BytesIO(raw_bytes))
            all_sheets = xl.sheet_names
            preferred = None
            for s in all_sheets:
                sn = str(s).lower()
                if any(k in sn for k in ("deviation", "capa", "incident",
                                          "investigation", "nc", "record")):
                    preferred = s; break
            sheet_to_use = preferred or all_sheets[0]
            dci_df_raw = pd.read_excel(
                io.BytesIO(raw_bytes), sheet_name=sheet_to_use, dtype=str
            ).fillna("")
    except Exception as e:
        st.error(f"Failed to read file: {e}")
        return

    # Validator
    v_ok, v_sev, v_title, v_results, v_evidence = _validate_dci_input_file(
        raw_bytes, file_name, dci_df_raw, all_sheets
    )

    override_key = f"validator_override__DCI__{file_name}"
    already_overridden = st.session_state.get(override_key, False)

    if not (_gen()["_render_validator_verdict"](
        v_sev, v_title, v_results, v_evidence,
        file_name, user, module="DCI"
    ) or already_overridden):
        return

    # Apply column aliases
    dci_df = dci_df_raw.copy()
    col_rename = {}
    for col in dci_df.columns:
        norm = str(col).strip().lower().replace(" ", "_")
        if norm in _DCI_COLUMN_ALIASES:
            canonical = _DCI_COLUMN_ALIASES[norm]
            if canonical != col:
                col_rename[col] = canonical
    if col_rename:
        dci_df = dci_df.rename(columns=col_rename)

    missing_required = _DCI_REQUIRED_COLS - set(dci_df.columns)
    if missing_required:
        st.error(
            f"❌ Missing required columns after alias mapping: "
            f"{', '.join(sorted(missing_required))}. "
            "Please ensure your file contains all 12 required columns "
            "(or standard aliases)."
        )
        return

    st.success(f"✓ File loaded: **{len(dci_df)} record(s)** · {len(dci_df.columns)} columns")

    # Rule config
    st.markdown("---")
    st.markdown("### 2. Rule Configuration")
    st.caption(
        "14 rules across 4 engines. Default: 12 ON, 2 OFF (Rules 11 and 14). "
        "Disabled rules still compute scores for the Full Log but are "
        "excluded from Risk_Tier and Records for Review."
    )

    if "dci_rule_config" not in st.session_state:
        st.session_state["dci_rule_config"] = dict(_DCI_RULE_DEFAULTS)

    cfg = st.session_state["dci_rule_config"]

    engine_labels = {
        "A": "🟡 RCA Recurrence",
        "B": "🔴 Weak Investigation",
        "C": "🔵 CAPA Effectiveness",
        "D": "🟣 SLA / Aging",
    }

    with st.expander("Configure rules (optional)", expanded=False):
        for engine_code, engine_name in engine_labels.items():
            st.markdown(f"**{engine_name}**")
            engine_rules = [m for m in _DCI_RULE_META if m[4] == engine_code]
            for num, name, sev, tier, _, dflt, cfg_key in engine_rules:
                cfg[cfg_key] = st.checkbox(
                    f"Rule {num} — {name} ({sev} · {tier})",
                    value=cfg.get(cfg_key, dflt),
                    key=f"dci_cfg_{cfg_key}",
                )
            st.markdown("")
    st.session_state["dci_rule_config"] = cfg

    active_count = sum(1 for v in cfg.values() if v)
    st.info(f"🔧 **{active_count}** of 14 rules active")

    if active_count == 0:
        st.error("⚠️ At least one rule must be active to run analysis.")
        return

    # Run Analysis
    st.markdown("---")
    st.markdown("### 3. Run Analysis")

    dci_running = st.session_state.get("dci_running", False)

    # ── Fingerprint of current inputs ────────────────────────────────────────
    # If file + config haven't changed since last successful run, the Run
    # button is disabled with a helpful message. User must change the file
    # OR toggle a rule config to enable a re-run.
    import json as _json_fp, hashlib as _hash_fp
    _input_fp = _hash_fp.sha256(
        _json_fp.dumps(
            {
                "file":  file_name,
                "rows":  len(dci_df),
                "cols":  sorted(dci_df.columns.tolist()),
                "cfg":   {k: bool(v) for k, v in cfg.items()},
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()[:16]
    _last_run_fp = st.session_state.get("dci_last_run_fingerprint")
    _already_analyzed_this_input = (
        _last_run_fp == _input_fp
        and st.session_state.get("dci_analysis_done", False)
    )

    run_col1, _run_col2 = st.columns([1, 3])
    with run_col1:
        if dci_running:
            _btn_label    = "⏳ Running…"
            _btn_disabled = True
        elif _already_analyzed_this_input:
            _btn_label    = "✓ Analyzed — change file or rules to re-run"
            _btn_disabled = True
        else:
            _btn_label    = "🚀 Run DCI Analysis"
            _btn_disabled = False
        run_clicked = st.button(
            _btn_label,
            disabled=_btn_disabled,
            key="dci_run_btn",
            use_container_width=True,
        )

    if run_clicked:
        st.session_state["dci_running"] = True
        try:
            with st.spinner("Scoring records across 14 rules…"):
                scored_df = dci_score_records(dci_df, rule_config=cfg)
            st.session_state["dci_scored_df"] = scored_df
            st.session_state["dci_analysis_done"] = True
            st.session_state["dci_last_run_fingerprint"] = _input_fp
            try:
                cfg_str = _json_fp.dumps(cfg, sort_keys=True)
                st.session_state["dci_config_hash"] = \
                    _hash_fp.sha256(cfg_str.encode()).hexdigest()[:16]
            except Exception:
                st.session_state["dci_config_hash"] = ""
            _gen()["log_audit"](
                user, "DCI_ANALYSIS_RUN", "DATASET",
                new_value=f"DCI_{st.session_state.get('dci_system_name','')}",
                reason=(
                    f"Records: {len(scored_df)} · "
                    f"Rules active: {active_count} · "
                    f"File: {file_name}"
                ),
            )
        except Exception as e:
            st.error(f"Analysis failed: {e}")
            st.session_state["dci_analysis_done"] = False
            st.session_state["dci_last_run_fingerprint"] = None
        finally:
            st.session_state["dci_running"] = False
            st.rerun()

    if not st.session_state.get("dci_analysis_done"):
        return

    # Results
    scored_df = st.session_state.get("dci_scored_df")
    if scored_df is None or scored_df.empty:
        st.warning("No records scored.")
        return

    st.markdown("---")
    _hr1, _hr2 = st.columns([4, 1])
    with _hr1:
        st.markdown("### 4. Results")
    with _hr2:
        if st.button("🗑️ Clear results", key="dci_clear_results_btn_inflow",
                     use_container_width=True):
            for _k in ("dci_scored_df", "dci_analysis_done",
                       "dci_last_run_fingerprint", "dci_config_hash",
                       "dci_file_name"):
                if _k in st.session_state:
                    del st.session_state[_k]
            st.rerun()

    n_crit = int((scored_df["Risk_Tier"] == "Critical").sum())
    n_high = int((scored_df["Risk_Tier"] == "High").sum())
    n_med  = int((scored_df["Risk_Tier"] == "Medium").sum())
    n_low  = int((scored_df["Risk_Tier"] == "Low").sum())

    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1: st.metric("Critical", n_crit)
    with kc2: st.metric("High",     n_high)
    with kc3: st.metric("Medium",   n_med)
    with kc4: st.metric("Low",      n_low)

    review_df = scored_df[scored_df["Risk_Tier"].isin(
        ["Critical", "High", "Medium"])]
    if not review_df.empty:
        st.markdown("**Records for Review** (Critical + High + Medium):")
        display_cols = ["record_id", "record_type", "deviation_category",
                        "system_name", "status", "Risk_Tier", "Risk_Score",
                        "Primary_Rule"]
        display_cols = [c for c in display_cols if c in review_df.columns]
        st.dataframe(
            review_df[display_cols].head(50),
            use_container_width=True, hide_index=True,
        )
        if len(review_df) > 50:
            st.caption(f"Showing 50 of {len(review_df)} review records. "
                       "Download Excel for full set.")
    else:
        st.success("✅ No records flagged at Medium or higher.")

    # Download + Bank
    st.markdown("---")
    st.markdown("### 5. Download Evidence Package & Bank to DIM")

    dc1, dc2 = st.columns(2)

    sys_name = st.session_state.get("dci_system_name", "System")
    cfg_hash = st.session_state.get("dci_config_hash", "")

    with dc1:
        try:
            xlsx_bytes = dci_build_excel(
                scored_df,
                system_name=sys_name,
                r_start=dci_r_start,
                r_end=dci_r_end,
                fname=file_name,
                config_hash=cfg_hash,
                operator_user=user,
                model_used=model_id,
                rule_config=cfg,
            )
            st.download_button(
                "📥 Download DCI Evidence Package (xlsx)",
                data=xlsx_bytes,
                file_name=f"DCI_{sys_name}_{dci_r_end}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                on_click=lambda: _gen()["log_audit"](
                    user, "DCI_DOWNLOAD", "REPORT",
                    new_value=f"DCI_{sys_name}",
                    reason=f"System: {sys_name}",
                ),
            )
        except Exception as e:
            st.error(f"Excel build failed: {e}")

    with dc2:
        if st.button(
            "🏦 Bank to Data Integrity Monitor (DIM)",
            use_container_width=True,
            key="dci_bank_btn",
        ):
            try:
                # Use _gen() lookup instead of `from generator import` to avoid
                # re-executing generator.py (would re-run st.set_page_config).
                _ec_fn = _gen().get("_dim_event_category")

                period_label = f"{dci_r_start} → {dci_r_end}" \
                    if (dci_r_start and dci_r_end) else \
                    f"DCI Period {st.session_state.get('dim_periods_banked', 0) + 1}"
                banked = _dci_bank_to_dim(
                    scored_df, period_label, sys_name, file_name,
                    event_category_fn=_ec_fn
                )
                if banked == 1 and n_crit == 0 and n_high == 0:
                    st.success(
                        f"✅ Banked sentinel row for clean period "
                        f"({period_label}) — DIM will see DCI ran clean."
                    )
                else:
                    st.success(
                        f"✅ Banked {banked} High/Critical finding(s) to DIM "
                        f"for period {period_label}."
                    )
                _gen()["log_audit"](
                    user, "DCI_BANK_TO_DIM", "DATASET",
                    new_value=f"DCI_{sys_name}",
                    reason=f"Period: {period_label} · Banked: {banked} rows",
                )
                st.rerun()
            except Exception as e:
                st.error(f"Banking failed: {e}")
