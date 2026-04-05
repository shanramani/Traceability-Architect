"""
VALINTEL.AI — Audit Trail Module QA Test Suite
===============================================
Run this script against any output .xlsx file from the Audit Trail module
to verify correctness before release.

Usage:
    python valintel_qa_suite.py <output.xlsx> [input.xlsx]

    output.xlsx  — the downloaded Evidence Package from VALINTEL
    input.xlsx   — the original audit log uploaded (optional, enables T01/T02/T24/T25)

Exit code: 0 = all tests passed, 1 = one or more failures
"""

import sys
import re
import os
import pandas as pd
from pathlib import Path


# ── Helpers ───────────────────────────────────────────────────────────────────

class TestResult:
    def __init__(self):
        self.passed  = []
        self.failed  = []
        self.skipped = []

    def ok(self, tid, label, detail=""):
        self.passed.append((tid, label, detail))

    def fail(self, tid, label, detail=""):
        self.failed.append((tid, label, detail))

    def skip(self, tid, label, detail="no input file"):
        self.skipped.append((tid, label, detail))

    def summary(self):
        total = len(self.passed) + len(self.failed) + len(self.skipped)
        print(f"\n{'='*65}")
        print(f"VALINTEL QA TEST SUITE — {total} tests")
        print(f"{'='*65}")
        if self.passed:
            print(f"\n✅ PASSED ({len(self.passed)})")
            for tid, label, detail in self.passed:
                d = f" — {detail}" if detail else ""
                print(f"   {tid}: {label}{d}")
        if self.skipped:
            print(f"\n⏭  SKIPPED ({len(self.skipped)}) — requires input file")
            for tid, label, detail in self.skipped:
                print(f"   {tid}: {label}")
        if self.failed:
            print(f"\n❌ FAILED ({len(self.failed)})")
            for tid, label, detail in self.failed:
                print(f"   {tid}: {label}")
                if detail:
                    for line in str(detail).splitlines():
                        print(f"        {line}")
        print(f"\n{'='*65}")
        verdict = "ALL TESTS PASSED ✅" if not self.failed else f"{len(self.failed)} TEST(S) FAILED ❌"
        print(f"VERDICT: {verdict}")
        print(f"{'='*65}\n")
        return len(self.failed) == 0


def load_output(path: str):
    xl = pd.ExcelFile(path)
    sheets = {}
    for sh in ['Summary', 'Events for Review', 'Full Audit Log', 'Detection Logic']:
        if sh in xl.sheet_names:
            sheets[sh] = xl.parse(sh, dtype=str).fillna('')
    return sheets


def sheet_text(sheets, name):
    if name not in sheets:
        return ""
    df = sheets[name]
    return ' '.join(str(v) for v in df.values.flatten() if str(v) not in ('','nan'))


def sum_raw(path: str):
    """Read Summary as raw (no header) for full text extraction."""
    try:
        df = pd.read_excel(path, sheet_name='Summary', dtype=str,
                           header=None).fillna('')
        return ' '.join(str(v) for v in df.values.flatten()
                        if str(v) not in ('','nan'))
    except Exception:
        return ""


def ev_col(df_ev, *candidates):
    """Find the first matching column header (partial match)."""
    for cand in candidates:
        for col in df_ev.columns:
            if cand.lower() in col.lower():
                return col
    return None


# ── Test functions ─────────────────────────────────────────────────────────────

def run_tests(output_path: str, input_path: str | None = None) -> bool:
    r = TestResult()
    sheets   = load_output(output_path)
    sum_text = sum_raw(output_path)
    df_ev    = sheets.get('Events for Review', pd.DataFrame())
    df_log   = sheets.get('Full Audit Log',    pd.DataFrame())
    ev_text  = sheet_text(sheets, 'Events for Review')
    log_text = sheet_text(sheets, 'Full Audit Log')
    sum_text2= sheet_text(sheets, 'Summary')
    all_text = sum_text + ev_text + log_text

    df_in = None
    if input_path and Path(input_path).exists():
        try:
            df_in = pd.read_excel(input_path, sheet_name='Audit Log',
                                  dtype=str).fillna('')
        except Exception:
            try:
                df_in = pd.read_excel(input_path, dtype=str).fillna('')
            except Exception:
                df_in = None

    # Column helpers
    wh_col   = ev_col(df_ev, 'What Happened')
    wim_col  = ev_col(df_ev, 'Why It Matters')
    db_col   = ev_col(df_ev, 'Decision Basis')
    disp_col = ev_col(df_ev, 'System-Proposed', 'Suggested Disposition')
    pr_col   = ev_col(df_ev, 'Primary Rule')
    rl_col   = ev_col(df_ev, 'Risk Level')
    ev_col_  = ev_col(df_ev, 'Evidence Strength', 'Evidence')
    seq_col  = ev_col(df_ev, 'Related Sequence')
    usr_col  = ev_col(df_ev, 'User')
    act_col  = ev_col(df_ev, 'Action')
    rec_col  = ev_col(df_ev, 'Record')

    # ── T01: Planted anomalies detected ───────────────────────────────────────
    if df_in is not None:
        expected = [
            ("admin_sys",  "INSERT", "BATCH",        "Rule 3"),
            ("analyst_x",  "DELETE", "RESULTS",      "Rule 5"),
            ("analyst_x",  "LOGIN",  "USER_SESSION",  "Rule 5"),
            ("analyst_y",  "DELETE", "RESULTS",      "Rule 6"),
            ("analyst_y",  "INSERT", "RESULTS",      "Rule 6"),
            ("dba_prod",   "UPDATE", "AUDIT_TRAIL",  "Rule 7"),
            ("analyst_y",  "UPDATE", "RESULTS",      "Rule 14"),
        ]
        missing = []
        for user, action, rec, rule in expected:
            found = df_ev[
                (df_ev.get(usr_col,'') == user) &
                (df_ev.get(act_col,'') == action) &
                (df_ev.get(rec_col,'').str.contains(rec, na=False)) &
                (df_ev.get(pr_col, '').str.contains(rule, na=False))
            ] if all(c for c in [usr_col,act_col,rec_col,pr_col]) else pd.DataFrame()
            if len(found) == 0:
                missing.append(f"{user}/{action}/{rec}→{rule}")
        if missing:
            r.fail("T01", "All planted anomalies detected",
                   "Missing: " + ", ".join(missing))
        else:
            r.ok("T01", "All planted anomalies detected", f"{len(expected)}/{len(expected)}")
    else:
        r.skip("T01", "All planted anomalies detected")

    # ── T02: SELECT on AUDIT_TRAIL not escalated (false positive) ─────────────
    if df_in is not None:
        select_at = df_in[
            (df_in['action_type'].str.upper() == 'SELECT') &
            (df_in['record_type'].str.upper().str.contains('AUDIT', na=False))
        ]
        if len(select_at) > 0 and not df_ev.empty:
            jones_users = select_at['user_id'].unique()
            fp = df_ev[
                df_ev.get(act_col,'').str.upper().eq('SELECT') &
                df_ev.get(rec_col,'').str.upper().str.contains('AUDIT', na=False)
            ] if act_col and rec_col else pd.DataFrame()
            if len(fp):
                r.fail("T02", "No SELECT/AUDIT_TRAIL false positive",
                       f"{len(fp)} SELECT event(s) incorrectly escalated: "
                       f"{fp.get(usr_col,'').values.tolist()}")
            else:
                r.ok("T02", "No SELECT/AUDIT_TRAIL false positive",
                     "Read-only audit trail access correctly excluded")
        else:
            r.ok("T02", "No SELECT/AUDIT_TRAIL false positive", "No SELECT events in input")
    else:
        r.skip("T02", "No SELECT/AUDIT_TRAIL false positive")

    # ── T03: What Happened — regulatory language free ─────────────────────────
    if wh_col and not df_ev.empty:
        bad = ['may indicate','raises a concern','inconsistent with',
               'alcoa','21 cfr','warrants','data integrity requirement']
        hits = []
        for i, row in df_ev.iterrows():
            wh = str(row.get(wh_col,'')).lower()
            for w in bad:
                if w in wh:
                    hits.append(f"Event #{row.get('No.',i+1)}: '{w}'")
        if hits:
            r.fail("T03", "What Happened — regulatory language free",
                   "\n".join(hits))
        else:
            r.ok("T03", "What Happened — regulatory language free")
    else:
        r.skip("T03", "What Happened check", "Column not found")

    # ── T04: Why It Matters — no action instructions ──────────────────────────
    if wim_col and not df_ev.empty:
        action_verbs = ['verify ','obtain ','retrieve ','investigate ',
                        'escalate ','initiate a','confirm ']
        hits = []
        for i, row in df_ev.iterrows():
            wim = str(row.get(wim_col,'')).lower()
            for w in action_verbs:
                if w in wim:
                    hits.append(f"Event #{row.get('No.',i+1)}: '{w.strip()}'")
                    break
        if hits:
            r.fail("T04", "Why It Matters — no action instructions", "\n".join(hits))
        else:
            r.ok("T04", "Why It Matters — no action instructions")
    else:
        r.skip("T04", "Why It Matters check", "Column not found")

    # ── T05: Decision Basis distinct from Disposition ─────────────────────────
    # Decision Basis column header is exactly "Decision Basis"
    # The System-Proposed Disposition column contains "Decision Basis" as a
    # substring — must match the shorter header only.
    db_col_exact   = next((c for c in df_ev.columns
                           if c.strip().lower() == 'decision basis'), None)
    disp_col_exact = next((c for c in df_ev.columns
                           if 'system-proposed' in c.lower()
                           or (c.strip().lower() == 'suggested disposition')), None)
    if db_col_exact and disp_col_exact and not df_ev.empty:
        same = []
        for i, row in df_ev.iterrows():
            db  = str(row.get(db_col_exact,'')).strip()
            dis = str(row.get(disp_col_exact,'')).strip()
            # DB should be a full sentence, not just the disposition label
            if db == dis and db:
                same.append(f"Event #{row.get('No.',i+1)}: both = '{db[:60]}'")
        if same:
            r.fail("T05", "Decision Basis distinct from Disposition",
                   "\n".join(same))
        else:
            r.ok("T05", "Decision Basis distinct from Disposition")
    else:
        r.skip("T05", "Decision Basis check",
               f"Exact columns not found (db={db_col_exact}, disp={disp_col_exact})")

    # ── T06: No Risk Score column anywhere ───────────────────────────────────
    score_cols = []
    for sh_name, df in sheets.items():
        for col in df.columns:
            if re.search(r'risk.?score', col, re.IGNORECASE):
                score_cols.append(f"'{sh_name}' → '{col}'")
    if score_cols:
        r.fail("T06", "No Risk Score column anywhere", "\n".join(score_cols))
    else:
        r.ok("T06", "No Risk Score column anywhere")

    # ── T07: No X.X/10 patterns in output text ────────────────────────────────
    hits = re.findall(r'\b\d+\.\d+/10\b', all_text)
    if hits:
        r.fail("T07", "No X.X/10 score patterns", f"Found: {hits[:5]}")
    else:
        r.ok("T07", "No X.X/10 score patterns")

    # ── T08: No EC-NNN identifiers ───────────────────────────────────────────
    ec_hits = re.findall(r'\bEC-\d{3}\b', all_text)
    if ec_hits:
        r.fail("T08", "No EC-NNN identifiers", f"Found: {list(set(ec_hits))}")
    else:
        r.ok("T08", "No EC-NNN identifiers anywhere")

    # ── T09: Sequence context populated for chain events ──────────────────────
    if seq_col and pr_col and not df_ev.empty:
        chain_rules = ['Rule 5','Rule 6','Rule 15']
        missing_seq = []
        for i, row in df_ev.iterrows():
            pr  = str(row.get(pr_col,''))
            seq = str(row.get(seq_col,'')).strip()
            if any(r in pr for r in chain_rules) and not seq:
                missing_seq.append(f"Event #{row.get('No.',i+1)} ({pr[:40]})")
        if missing_seq:
            r.fail("T09", "Sequence context populated for chain events",
                   "\n".join(missing_seq))
        else:
            r.ok("T09", "Sequence context populated for chain events")
    else:
        r.skip("T09", "Sequence context check", "Columns not found")

    # ── T10: What Happened includes sequence context for chain events ──────────
    if wh_col and pr_col and not df_ev.empty:
        chain_rules = ['Rule 5','Rule 6','Rule 15']
        ctx_words   = ['sequence','part of','failed-login','delete-recreate',
                       'following repeated','preceded']
        missing_ctx = []
        for i, row in df_ev.iterrows():
            pr = str(row.get(pr_col,''))
            wh = str(row.get(wh_col,'')).lower()
            if any(rule in pr for rule in chain_rules):
                if not any(w in wh for w in ctx_words):
                    missing_ctx.append(f"Event #{row.get('No.',i+1)}: '{wh[:60]}'")
        if missing_ctx:
            r.fail("T10", "What Happened includes sequence context",
                   "\n".join(missing_ctx))
        else:
            r.ok("T10", "What Happened includes sequence context")
    else:
        r.skip("T10", "What Happened context check", "Columns not found")

    # ── T11: Summary breakdown matches Events sheet ────────────────────────────
    if not df_ev.empty and rl_col:
        n_esc  = len(df_ev)
        n_crit = len(df_ev[df_ev[rl_col]=='Critical'])
        n_high = len(df_ev[df_ev[rl_col]=='High'])
        errors = []
        if f"{n_crit} of {n_esc} escalated events" not in sum_text:
            errors.append(f"Critical count '{n_crit} of {n_esc}' not in Summary")
        if f"{n_high} of {n_esc} escalated events" not in sum_text:
            errors.append(f"High count '{n_high} of {n_esc}' not in Summary")
        if errors:
            r.fail("T11", "Summary breakdown matches Events sheet",
                   "\n".join(errors))
        else:
            r.ok("T11", "Summary breakdown matches Events sheet",
                 f"{n_crit} Critical, {n_high} High of {n_esc}")
    else:
        r.skip("T11", "Summary breakdown check")

    # ── T12: Auto-cleared percentage correct ──────────────────────────────────
    if not df_ev.empty:
        n_esc = len(df_ev)
        pct   = round((1000 - n_esc) / 1000 * 100, 1)
        n_cleared = 1000 - n_esc
        if f"{n_cleared:,}  ({pct}%)" in sum_text or f"{n_cleared} ({pct}%)" in sum_text:
            r.ok("T12", "Auto-cleared percentage correct", f"{n_cleared} ({pct}%)")
        else:
            # Try to find what's in the summary
            m = re.search(r'Auto-Cleared[^\d]*(\d[\d,]+)[^\d]+([\d.]+)%', sum_text, re.IGNORECASE)
            actual = m.group(0)[:60] if m else "not found"
            r.fail("T12", "Auto-cleared percentage correct",
                   f"Expected {n_cleared} ({pct}%) — found: {actual}")
    else:
        r.skip("T12", "Auto-cleared percentage check")

    # ── T13: Narrative uses escalated counts only ──────────────────────────────
    # Full-dataset tier counts would appear as standalone numbers not followed
    # by "of N escalated" — check for that specific toxic pattern
    bad_patterns = [
        r'\d+ critical events?,\s*\d+ high-risk',
        r'were identified out of \d+ events escalated',
    ]
    hits = []
    for pat in bad_patterns:
        if re.search(pat, sum_text, re.IGNORECASE):
            hits.append(pat)
    if hits:
        r.fail("T13", "Narrative uses escalated counts only",
               "Full-dataset sentence pattern detected: " + str(hits))
    else:
        r.ok("T13", "Narrative uses escalated counts only")

    # ── T14: No duplicate narrative sentences ─────────────────────────────────
    # Extract sentences from the narrative paragraph
    narrative_m = re.search(
        r'(The audit trail for .+?)(?=REVIEWER STATEMENT|Reviewer Name)',
        sum_text, re.DOTALL|re.IGNORECASE)
    if narrative_m:
        narrative = narrative_m.group(1)
        sentences = [s.strip() for s in re.split(r'\.\s+|\n\n', narrative)
                     if len(s.strip()) > 30]
        seen_s, dupes = set(), []
        for s in sentences:
            if s in seen_s:
                dupes.append(s[:80])
            seen_s.add(s)
        if dupes:
            r.fail("T14", "No duplicate narrative sentences",
                   "\n".join(dupes))
        else:
            r.ok("T14", "No duplicate narrative sentences",
                 f"{len(sentences)} unique sentences")
    else:
        r.skip("T14", "Duplicate sentence check", "Narrative not found")

    # ── T15: No 'violating' language ──────────────────────────────────────────
    hits = re.findall(r'\bviolat\w*', all_text, re.IGNORECASE)
    if hits:
        r.fail("T15", "No 'violating' language", f"Found: {list(set(hits))}")
    else:
        r.ok("T15", "No 'violating' language")

    # ── T16: No implementation jargon ────────────────────────────────────────
    jargon = ['maps unconditionally','comment-gate','hard gate','burst dedup',
               'top n','named rule']
    hits = [j for j in jargon if j.lower() in all_text.lower()]
    if hits:
        r.fail("T16", "No implementation jargon in output", f"Found: {hits}")
    else:
        r.ok("T16", "No implementation jargon in output")

    # ── T17: No 'threshold met / maps to' reviewer-invisible phrases ──────────
    toxic = [r'threshold met.*maps to', r'\bmaps to\b.*escalate',
             r'\bmaps to\b.*investigate']
    hits = [p for p in toxic if re.search(p, all_text, re.IGNORECASE)]
    if hits:
        r.fail("T17", "No threshold-met/maps-to jargon", f"Patterns found: {hits}")
    else:
        r.ok("T17", "No threshold-met/maps-to jargon")

    # ── T18: Rule 3 cites 21 CFR Part 11 §11.10(d) ───────────────────────────
    if wim_col and pr_col and not df_ev.empty:
        for i, row in df_ev.iterrows():
            if 'Rule 3' in str(row.get(pr_col,'')):
                wim = str(row.get(wim_col,''))
                if '11.10(d)' in wim or '11.10(d)' in wim:
                    r.ok("T18", "Rule 3 cites 21 CFR Part 11 §11.10(d)")
                else:
                    r.fail("T18", "Rule 3 cites 21 CFR Part 11 §11.10(d)",
                           f"Actual citation: '{wim[:100]}'")
                break
        else:
            r.skip("T18", "Rule 3 citation check", "No Rule 3 event found")
    else:
        r.skip("T18", "Rule 3 citation check")

    # ── T19: Rule 6 citation uses recreation not recovery ─────────────────────
    if wim_col and pr_col and not df_ev.empty:
        for i, row in df_ev.iterrows():
            if 'Rule 6' in str(row.get(pr_col,'')):
                wim = str(row.get(wim_col,''))
                if 'must not be altered by deleting' in wim:
                    r.ok("T19", "Rule 6 citation: recreation not recovery")
                elif 'recoverable' in wim.lower():
                    r.fail("T19", "Rule 6 citation: recreation not recovery",
                           "Still uses 'recoverable' language")
                else:
                    r.ok("T19", "Rule 6 citation: recreation not recovery",
                         "Neither wrong phrase present")
                break
        else:
            r.skip("T19", "Rule 6 citation check", "No Rule 6 event found")
    else:
        r.skip("T19", "Rule 6 citation check")

    # ── T20: Rule 7 citation includes EU Annex 11 ────────────────────────────
    if wim_col and pr_col and not df_ev.empty:
        for i, row in df_ev.iterrows():
            if 'Rule 7' in str(row.get(pr_col,'')):
                wim = str(row.get(wim_col,''))
                if 'Annex 11' in wim and '11.10(e)' in wim:
                    r.ok("T20", "Rule 7 citation correct (11.10(e) + Annex 11)")
                else:
                    r.fail("T20", "Rule 7 citation correct",
                           f"Missing citation elements: '{wim[:100]}'")
                break
        else:
            r.skip("T20", "Rule 7 citation check", "No Rule 7 event found")
    else:
        r.skip("T20", "Rule 7 citation check")

    # ── T21: Reviewer independence language ───────────────────────────────────
    has_ind  = 'independent human judgement' in sum_text.lower()
    if has_ind:
        r.ok("T21", "Reviewer independence language present")
    else:
        r.fail("T21", "Reviewer independence language present",
               "'independent human judgement' not found in Reviewer Statement")

    # ── T22: Informational only language ──────────────────────────────────────
    has_info = 'informational only' in sum_text.lower()
    if has_info:
        r.ok("T22", "System-Proposed Disposition marked informational")
    else:
        r.fail("T22", "System-Proposed Disposition marked informational",
               "'informational only' not found in Reviewer Statement")

    # ── T23: Scope note when out-of-period events exist ───────────────────────
    if df_in is not None:
        # Get review period from summary
        period_m = re.search(r'(\d{2}-\w{3}-\d{4})\s*→\s*(\d{2}-\w{3}-\d{4})', sum_text)
        if period_m:
            try:
                r_start = pd.to_datetime(period_m.group(1), dayfirst=True, errors='coerce')
                r_end   = pd.to_datetime(period_m.group(2), dayfirst=True, errors='coerce')
                ts_col  = 'timestamp' if 'timestamp' in df_in.columns else df_in.columns[0]
                ts      = pd.to_datetime(df_in[ts_col], errors='coerce').dropna()
                out_of  = int(((ts < r_start) | (ts > r_end)).sum())
                if out_of > 0:
                    has_note = 'scope note' in sum_text.lower() or 'post-date' in sum_text
                    if has_note:
                        r.ok("T23", "Scope note present for out-of-period events",
                             f"{out_of} out-of-period event(s) flagged")
                    else:
                        r.fail("T23", "Scope note present for out-of-period events",
                               f"{out_of} out-of-period events but no scope note in Summary")
                else:
                    r.ok("T23", "Scope note check — no out-of-period events", "N/A")
            except Exception as e:
                r.skip("T23", "Scope note check", f"Date parse error: {e}")
        else:
            r.skip("T23", "Scope note check", "Review period not found in Summary")
    else:
        r.skip("T23", "Scope note check")

    # ── T24: Full Audit Log has same rows as input ────────────────────────────
    if df_in is not None:
        n_in  = len(df_in)
        n_log = len(df_log)
        if n_in == n_log:
            r.ok("T24", "Full Audit Log row count matches input", f"{n_log} rows")
        else:
            r.fail("T24", "Full Audit Log row count matches input",
                   f"Input: {n_in}, Log: {n_log}")
    else:
        r.skip("T24", "Full Audit Log row count check")

    # ── T25: LOGIN_FAILED events in Full Log ─────────────────────────────────
    if df_in is not None and not df_log.empty:
        n_fail_in = len(df_in[df_in.get('action_type','').str.upper()=='LOGIN_FAILED'])
        act_log   = ev_col(df_log, 'Action') or ev_col(df_log, 'action')
        n_fail_log = len(df_log[df_log.get(act_log,'').str.upper()=='LOGIN_FAILED']) if act_log else 0
        if n_fail_in == n_fail_log:
            r.ok("T25", "All LOGIN_FAILED events in Full Log",
                 f"{n_fail_log} LOGIN_FAILED rows")
        else:
            r.fail("T25", "All LOGIN_FAILED events in Full Log",
                   f"Input: {n_fail_in}, Log: {n_fail_log}")
    else:
        r.skip("T25", "LOGIN_FAILED count check")

    # ── T26: Evidence Strength populated for all escalated events ─────────────
    if ev_col_ and not df_ev.empty:
        bad = []
        for i, row in df_ev.iterrows():
            ev = str(row.get(ev_col_,'')).strip()
            if ev not in ('High','Medium','Low'):
                bad.append(f"Event #{row.get('No.',i+1)}: '{ev}'")
        if bad:
            r.fail("T26", "Evidence Strength populated", "\n".join(bad))
        else:
            r.ok("T26", "Evidence Strength populated for all events")
    else:
        r.skip("T26", "Evidence Strength check")

    # ── T27: Chain events share Primary Rule ─────────────────────────────────
    if seq_col and pr_col and not df_ev.empty:
        # Group by sequence context text
        chain_evs = df_ev[df_ev[seq_col].str.strip() != '']
        if len(chain_evs):
            # Group by lead user + rule
            mismatches = []
            for pr_val, grp in chain_evs.groupby(pr_col):
                pass  # All chain events with same seq type should share primary rule
            # Simpler: same sequence keyword → same primary rule
            r5_rows = chain_evs[chain_evs[seq_col].str.contains('failed-login', na=False)]
            r6_rows = chain_evs[chain_evs[seq_col].str.contains('delete-recreate', na=False)]
            for label, rows, expected_rule in [
                ('failed-login', r5_rows, 'Rule 5'),
                ('delete-recreate', r6_rows, 'Rule 6'),
            ]:
                if len(rows):
                    wrong = rows[~rows[pr_col].str.contains(expected_rule, na=False)]
                    if len(wrong):
                        mismatches.append(
                            f"{label} chain has wrong primary rule: "
                            f"{wrong[pr_col].values.tolist()}")
            if mismatches:
                r.fail("T27", "Chain events share consistent Primary Rule",
                       "\n".join(mismatches))
            else:
                r.ok("T27", "Chain events share consistent Primary Rule")
        else:
            r.ok("T27", "Chain events check — no chain events found", "N/A")
    else:
        r.skip("T27", "Chain consistency check")

    # ── T28: dba_prod UPDATE on AUDIT_TRAIL → Rule 7 Critical ────────────────
    if not df_ev.empty and usr_col and act_col and pr_col and rl_col:
        dba = df_ev[
            df_ev.get(usr_col,'').str.lower().str.contains('dba', na=False) &
            df_ev.get(act_col,'').str.upper().eq('UPDATE')
        ]
        if len(dba):
            row = dba.iloc[0]
            pr  = str(row.get(pr_col,''))
            rl  = str(row.get(rl_col,''))
            if 'Rule 7' in pr and rl == 'Critical':
                r.ok("T28", "dba_prod UPDATE AUDIT_TRAIL → Rule 7 Critical")
            else:
                r.fail("T28", "dba_prod UPDATE AUDIT_TRAIL → Rule 7 Critical",
                       f"Got Primary Rule: '{pr}', Risk Level: '{rl}'")
        else:
            r.skip("T28", "dba_prod Rule 7 check", "No dba UPDATE event found")
    else:
        r.skip("T28", "dba_prod Rule 7 check")

    # ── T29: SELECT on AUDIT_TRAIL not in Events for Review ──────────────────
    if not df_ev.empty and act_col and rec_col:
        fp = df_ev[
            df_ev.get(act_col,'').str.upper().eq('SELECT') &
            df_ev.get(rec_col,'').str.upper().str.contains('AUDIT', na=False)
        ]
        if len(fp):
            r.fail("T29", "SELECT on AUDIT_TRAIL not escalated",
                   f"{len(fp)} SELECT event(s) incorrectly escalated: "
                   f"{fp.get(usr_col,'').values.tolist()}")
        else:
            r.ok("T29", "SELECT on AUDIT_TRAIL correctly excluded")
    else:
        r.skip("T29", "SELECT exclusion check")

    # ── T30: Rule 14 Decision Basis not Rule 10 ──────────────────────────────
    if db_col and pr_col and not df_ev.empty:
        for i, row in df_ev.iterrows():
            if 'Rule 14' in str(row.get(pr_col,'')):
                db = str(row.get(db_col,''))
                if 'Rule 10' in db or 'audit trail gap' in db.lower():
                    r.fail("T30", "Rule 14 Decision Basis not citing Rule 10",
                           f"DB reads: '{db[:100]}'")
                else:
                    r.ok("T30", "Rule 14 Decision Basis correct",
                         f"'{db[:80]}'")
                break
        else:
            r.skip("T30", "Rule 14 Decision Basis check", "No Rule 14 event found")
    else:
        r.skip("T30", "Rule 14 Decision Basis check")

    return r.summary()


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        print("Usage: python valintel_qa_suite.py <output.xlsx> [input.xlsx]")
        sys.exit(1)

    output_path = sys.argv[1]
    input_path  = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(output_path).exists():
        print(f"❌ Output file not found: {output_path}")
        sys.exit(1)

    print(f"\nRunning QA suite against: {Path(output_path).name}")
    if input_path:
        print(f"Input file:               {Path(input_path).name}")
    print()

    passed = run_tests(output_path, input_path)
    sys.exit(0 if passed else 1)