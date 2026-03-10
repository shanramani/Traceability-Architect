"""
Validation Doc Assist — v16.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Changes over v15:
  1. AUTHENTICATION HARDENING  — hashed passwords (bcrypt/sha256),
                                  roles (Admin/QA/Validator), session
                                  timeout (30 min inactivity), account
                                  lockout (5 failed attempts → 15 min)
  2. FULL AUDIT TRAIL SCHEMA   — event_id, user, timestamp, action,
                                  object_changed, old_value, new_value,
                                  reason. INSERT-only, never editable.
  3. DOCUMENT VERSIONING       — every artifact stored with auto-
                                  incremented version; previous versions
                                  never overwritten.
  4. AI GENERATION RECORD      — model, prompt_version, temperature,
                                  timestamp stored per generation run.
  5. GAP ANALYSIS              — [GAP] enforcement + Gap_Analysis tab
                                  showing untestable / uncovered reqs.
  6. URS → FRS GENERATION      — FRS built from URS + optional user guide
  7. DUAL UPLOADER             — main URS/SOP + sidebar system-context PDF
  8. RETAINED FILE MESSAGE FIX — banner hidden when no file is present
  9. ALL CSS / BRANDING        — unchanged from v15
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import streamlit as st
import os
import datetime
import pandas as pd
from litellm import completion
import tempfile
import io
import sqlite3
import re
import hashlib
import requests

try:
    import pdfplumber
    PDFPLUMBER_AVAILABLE = True
except ImportError:
    PDFPLUMBER_AVAILABLE = False

from langchain_community.document_loaders import PyPDFLoader
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False

# =============================================================================
# 1. CONFIG
# =============================================================================
VERSION        = "16.1"
PROMPT_VERSION = "v6.1-frs-id-oq-link-type"
TEMPERATURE    = 0.2
CHUNK_SIZE     = 8
DB_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation_app.db")

SESSION_TIMEOUT_MINUTES = 30
MAX_FAILED_ATTEMPTS     = 5
LOCKOUT_MINUTES         = 15

ROLES = ["Admin", "QA", "Validator"]

st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# =============================================================================
# 2. DATABASE
# =============================================================================

def db_connect():
    return sqlite3.connect(DB_PATH)

def db_migrate():
    try:
        conn = db_connect()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                username        TEXT    UNIQUE NOT NULL,
                password_hash   TEXT    NOT NULL,
                role            TEXT    DEFAULT 'Validator',
                failed_attempts INTEGER DEFAULT 0,
                locked_until    TEXT    DEFAULT NULL,
                created_at      TEXT
            )
        """)

        # INSERT-ONLY audit trail — never UPDATE or DELETE this table
        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                event_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                user           TEXT    NOT NULL,
                timestamp      TEXT    NOT NULL,
                action         TEXT    NOT NULL,
                object_changed TEXT,
                old_value      TEXT,
                new_value      TEXT,
                reason         TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_type    TEXT    NOT NULL,
                version     INTEGER NOT NULL,
                content     TEXT,
                created_by  TEXT,
                created_at  TEXT,
                project_ref TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_gen_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                model          TEXT,
                prompt_version TEXT,
                temperature    REAL,
                timestamp      TEXT,
                generated_by   TEXT,
                project_ref    TEXT,
                input_file     TEXT
            )
        """)

        # Safe migrations for pre-existing databases
        user_cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        for col, defn in [
            ("failed_attempts", "INTEGER DEFAULT 0"),
            ("locked_until",    "TEXT DEFAULT NULL"),
            ("created_at",      "TEXT"),
        ]:
            if col not in user_cols:
                conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")

        audit_cols = [r[1] for r in conn.execute("PRAGMA table_info(audit_log)").fetchall()]
        for col, defn in [
            ("object_changed", "TEXT"),
            ("old_value",      "TEXT"),
            ("new_value",      "TEXT"),
            ("reason",         "TEXT"),
        ]:
            if col not in audit_cols:
                try:
                    conn.execute(f"ALTER TABLE audit_log ADD COLUMN {col} {defn}")
                except Exception:
                    pass

        doc_cols = [r[1] for r in conn.execute("PRAGMA table_info(documents)").fetchall()]
        if "project_ref" not in doc_cols:
            conn.execute("ALTER TABLE documents ADD COLUMN project_ref TEXT")

        ai_cols = [r[1] for r in conn.execute("PRAGMA table_info(ai_gen_log)").fetchall()]
        for col, defn in [
            ("temperature",  "REAL"),
            ("project_ref",  "TEXT"),
            ("input_file",   "TEXT"),
        ]:
            if col not in ai_cols:
                conn.execute(f"ALTER TABLE ai_gen_log ADD COLUMN {col} {defn}")

        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"DB migration warning: {e}")


def db_diagnostics() -> dict:
    try:
        conn   = db_connect()
        result = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["users", "audit_log", "documents", "ai_gen_log"]}
        conn.close()
        return result
    except Exception as e:
        return {"error": str(e)}


def log_audit(user: str, action: str, object_changed: str = "",
              old_value: str = "", new_value: str = "", reason: str = ""):
    """Append-only audit write. This function must never UPDATE or DELETE rows."""
    try:
        conn = db_connect()
        conn.execute(
            """INSERT INTO audit_log
               (user, timestamp, action, object_changed, old_value, new_value, reason)
               VALUES (?,?,?,?,?,?,?)""",
            (user,
             datetime.datetime.utcnow().isoformat(),
             action,
             str(object_changed)[:500],
             str(old_value)[:2000]  if old_value  else "",
             str(new_value)[:2000]  if new_value  else "",
             str(reason)[:1000]     if reason     else "")
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Audit log write failed: {e}")


def log_ai_generation(user: str, model: str, prompt_version: str,
                      temperature: float, input_file: str = "", project_ref: str = ""):
    try:
        conn = db_connect()
        conn.execute(
            """INSERT INTO ai_gen_log
               (model, prompt_version, temperature, timestamp, generated_by, project_ref, input_file)
               VALUES (?,?,?,?,?,?,?)""",
            (model, prompt_version, temperature,
             datetime.datetime.utcnow().isoformat(),
             user, project_ref, input_file)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"AI gen log write failed: {e}")


def get_next_doc_version(doc_type: str) -> int:
    try:
        conn = db_connect()
        row  = conn.execute(
            "SELECT MAX(version) FROM documents WHERE doc_type=?", (doc_type,)
        ).fetchone()
        conn.close()
        return (row[0] or 0) + 1
    except Exception:
        return 1


def save_document(doc_type: str, content: str, created_by: str, project_ref: str = "") -> int:
    """Always inserts a new version — never overwrites previous."""
    version = get_next_doc_version(doc_type)
    try:
        conn = db_connect()
        conn.execute(
            """INSERT INTO documents
               (doc_type, version, content, created_by, created_at, project_ref)
               VALUES (?,?,?,?,?,?)""",
            (doc_type, version, content[:10000], created_by,
             datetime.datetime.utcnow().isoformat(), project_ref)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Document save failed: {e}")
    return version


# =============================================================================
# 3. AUTHENTICATION
# =============================================================================

def hash_password(plain: str) -> str:
    if BCRYPT_AVAILABLE:
        return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    return hashlib.sha256(plain.encode()).hexdigest()


def verify_password(plain: str, stored_hash: str) -> bool:
    try:
        if BCRYPT_AVAILABLE and stored_hash.startswith("$2"):
            return bcrypt.checkpw(plain.encode("utf-8"), stored_hash.encode("utf-8"))
        return hashlib.sha256(plain.encode()).hexdigest() == stored_hash
    except Exception:
        return False


def create_user(username: str, plain_password: str, role: str = "Validator"):
    pw_hash = hash_password(plain_password)
    conn    = db_connect()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?,?,?,?)",
            (username, pw_hash, role, datetime.datetime.utcnow().isoformat())
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()


def _is_account_locked(username: str) -> tuple:
    conn = db_connect()
    row  = conn.execute(
        "SELECT failed_attempts, locked_until FROM users WHERE username=?", (username,)
    ).fetchone()
    conn.close()
    if not row:
        return False, ""
    _, locked_until = row
    if locked_until:
        try:
            unlock_time = datetime.datetime.fromisoformat(locked_until)
            if datetime.datetime.utcnow() < unlock_time:
                remaining = int((unlock_time - datetime.datetime.utcnow()).total_seconds() / 60)
                return True, f"Account locked. Try again in {remaining} minute(s)."
            else:
                conn2 = db_connect()
                conn2.execute(
                    "UPDATE users SET failed_attempts=0, locked_until=NULL WHERE username=?",
                    (username,)
                )
                conn2.commit()
                conn2.close()
        except Exception:
            pass
    return False, ""


def _record_failed_attempt(username: str):
    conn = db_connect()
    conn.execute(
        "UPDATE users SET failed_attempts = failed_attempts + 1 WHERE username=?", (username,)
    )
    row = conn.execute(
        "SELECT failed_attempts FROM users WHERE username=?", (username,)
    ).fetchone()
    if row and row[0] >= MAX_FAILED_ATTEMPTS:
        lock_until = (datetime.datetime.utcnow() +
                      datetime.timedelta(minutes=LOCKOUT_MINUTES)).isoformat()
        conn.execute(
            "UPDATE users SET locked_until=? WHERE username=?", (lock_until, username)
        )
        log_audit(username, "ACCOUNT_LOCKED", "USER",
                  reason=f"Exceeded {MAX_FAILED_ATTEMPTS} failed attempts")
    conn.commit()
    conn.close()


def _reset_failed_attempts(username: str):
    conn = db_connect()
    conn.execute(
        "UPDATE users SET failed_attempts=0, locked_until=NULL WHERE username=?", (username,)
    )
    conn.commit()
    conn.close()


def authenticate_user(username: str, password: str) -> tuple:
    """Returns (success: bool, error_message: str)."""
    if not username:
        return False, "Username is required."
    try:
        conn  = db_connect()
        row   = conn.execute(
            "SELECT password_hash, role FROM users WHERE username=?", (username,)
        ).fetchone()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()

        if count == 0:
            create_user(username, password, role="Admin")
            log_audit(username, "FIRST_RUN_ADMIN_CREATED", "USER",
                      new_value=username, reason="First-run bootstrap")
            _reset_failed_attempts(username)
            return True, ""

        if not row:
            return False, "Invalid credentials."

        is_locked, lock_msg = _is_account_locked(username)
        if is_locked:
            log_audit(username, "LOGIN_BLOCKED_LOCKED", "SESSION")
            return False, lock_msg

        if verify_password(password, row[0]):
            _reset_failed_attempts(username)
            return True, ""

        _record_failed_attempt(username)
        conn2    = db_connect()
        attempts = conn2.execute(
            "SELECT failed_attempts FROM users WHERE username=?", (username,)
        ).fetchone()
        conn2.close()
        remaining = MAX_FAILED_ATTEMPTS - (attempts[0] if attempts else 1)
        log_audit(username, "LOGIN_FAILED", "SESSION",
                  reason=f"Bad password, {max(remaining,0)} attempt(s) remaining")
        if remaining <= 0:
            return False, f"Account locked for {LOCKOUT_MINUTES} minutes."
        return False, f"Invalid credentials. {remaining} attempt(s) remaining."

    except Exception as ex:
        return False, f"Authentication error: {ex}"


def get_user_role(username: str) -> str:
    try:
        conn = db_connect()
        row  = conn.execute("SELECT role FROM users WHERE username=?", (username,)).fetchone()
        conn.close()
        return row[0] if row else "Validator"
    except Exception:
        return "Validator"


def check_session_timeout() -> bool:
    last = st.session_state.get("last_activity")
    if not last:
        return True
    elapsed = (datetime.datetime.utcnow() - last).total_seconds() / 60
    return elapsed <= SESSION_TIMEOUT_MINUTES


def touch_session():
    st.session_state["last_activity"] = datetime.datetime.utcnow()


# =============================================================================
# 4. PDF EXTRACTION
# =============================================================================

def extract_pages(file_bytes: bytes) -> list:
    pages_text = []
    if PDFPLUMBER_AVAILABLE:
        try:
            with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
                for page_num, page in enumerate(pdf.pages, start=1):
                    parts = []
                    prose = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
                    if prose.strip():
                        parts.append(prose.strip())
                    for t_idx, table in enumerate(page.extract_tables() or []):
                        if not table:
                            continue
                        rows_md = []
                        for r_idx, row in enumerate(table):
                            cells = [str(c).replace("\n", " ").strip() if c else "" for c in row]
                            rows_md.append(" | ".join(cells))
                            if r_idx == 0:
                                rows_md.append(" | ".join(["---"] * len(row)))
                        parts.append(
                            f"\n[TABLE {t_idx+1} — Page {page_num}]\n"
                            + "\n".join(rows_md) + "\n[/TABLE]\n"
                        )
                    pages_text.append(f"--- Page {page_num} ---\n" + "\n".join(parts))
            if sum(len(p) for p in pages_text) >= 50:
                return pages_text
        except Exception:
            pages_text = []

    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        lc_pages   = PyPDFLoader(tmp_path).load()
        pages_text = [f"--- Page {i+1} ---\n{p.page_content}"
                      for i, p in enumerate(lc_pages)]
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    return pages_text


# =============================================================================
# 5. PROMPTS
# =============================================================================

SYSTEM_PROMPT = (
    "You are a Principal Validation Engineer specializing in GAMP 5 and 21 CFR Part 11. "
    "You output ONLY structured CSV data — no explanations, no markdown, no preamble. "
    "Always wrap field values that contain commas in double-quotes. "
    "The document text may contain [TABLE N] blocks in pipe-delimited format. "
    "Extract requirements from both prose AND table cells."
)


def build_chunk_prompt(chunk_text: str, chunk_index: int, total_chunks: int,
                       sys_context: str = "") -> str:
    context_section = ""
    if sys_context:
        context_section = (
            f"\nSYSTEM USER GUIDE (sidebar upload — describes product features/functions "
            f"used to derive FRS requirements):\n{sys_context[:3000]}\n"
        )

    return f"""
{context_section}
URS CONTENT (main upload — defines user requirements) — Segment {chunk_index + 1} of {total_chunks}:
{chunk_text}

TASK: Parse this segment into exactly 4 CSV datasets separated by |||.
Output ONLY raw CSV rows — include the header row in EVERY response.
Wrap any field value containing a comma in double-quotes.

Dataset 1 (FRS): ID,Requirement_Description,Priority,GxP_Impact,Source_URS_Ref
  - ID MUST start with "FRS-" followed by a zero-padded number, e.g. FRS-001, FRS-002.
  - Generate functional requirements derived from the URS (main document) and the
    system context / user guide (sidebar document).
  - Priority: Critical / High / Medium / Low
  - GxP_Impact: Direct / Indirect / None
  - Source_URS_Ref: the URS requirement ID or section this FRS row was derived from.

Dataset 2 (OQ): Test_ID,Requirement_Link,Requirement_Link_Type,Test_Step,Expected_Result,Pass_Fail_Criteria
  - Test_ID format: OQ-001, OQ-002, etc.
  - Requirement_Link: the ID of the requirement being tested (URS or FRS ID).
  - Requirement_Link_Type: must be exactly "URS" if linking to a URS requirement,
    or "FRS" if linking to a FRS requirement.
  - Generate one or more test cases per FRS requirement where testable.

Dataset 3 (Traceability): URS_Req_ID,FRS_Ref,Test_ID,Coverage_Status,Gap_Analysis
  - FRS_Ref MUST be the FRS ID (e.g. FRS-001) from Dataset 1 — never a URS ID here.
  - Coverage_Status: Covered / Partial / Not Covered
  - If a requirement has NO corresponding test, leave Test_ID blank and
    begin Gap_Analysis with exactly: [GAP]
  - If partially covered, prefix Gap_Analysis with: [PARTIAL GAP]

Dataset 4 (Gap_Summary): Req_ID,Requirement_Description,Gap_Type,Gap_Reason,Recommended_Action
  - Gap_Type: Untestable / No_Test_Coverage / Partial_Coverage / Out_of_Scope
  - Only include requirements with coverage gaps here.

Separate each dataset with exactly: |||
"""


# =============================================================================
# 6. SEGMENTED AI ANALYSIS
# =============================================================================

def _safe_parse_chunk(raw: str) -> tuple:
    raw   = re.sub(r'^```[a-zA-Z]*\n?', '', raw, flags=re.MULTILINE)
    raw   = re.sub(r'```\s*$',          '', raw, flags=re.MULTILINE)
    parts = re.split(r'\s*\|\|\|\s*', raw.strip())
    while len(parts) < 4:
        parts.append("")
    return parts[0].strip(), parts[1].strip(), parts[2].strip(), parts[3].strip()


def _csv_to_df(csv_text: str) -> pd.DataFrame:
    if not csv_text:
        return pd.DataFrame()
    try:
        return pd.read_csv(
            io.StringIO(csv_text),
            quotechar='"',
            on_bad_lines='skip',
            skipinitialspace=True
        )
    except Exception:
        return pd.DataFrame()


def _remove_duplicate_headers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df.columns) == 0:
        return df
    return df[df.iloc[:, 0].astype(str) != df.columns[0]].reset_index(drop=True)


def run_segmented_analysis(
    file_bytes: bytes,
    model_id: str,
    progress_bar,
    status_text,
    sys_context_bytes: bytes = None
) -> tuple:
    all_pages   = extract_pages(file_bytes)
    chunks      = [all_pages[i:i + CHUNK_SIZE] for i in range(0, len(all_pages), CHUNK_SIZE)]
    total       = len(chunks)
    sys_context = ""

    if sys_context_bytes:
        try:
            sys_pages   = extract_pages(sys_context_bytes)
            sys_context = "\n\n".join(sys_pages[:6])
        except Exception:
            pass

    frs_frames, oq_frames, trace_frames, gap_frames = [], [], [], []

    for idx, chunk_pages in enumerate(chunks):
        chunk_text = "\n\n".join(chunk_pages)
        status_text.text(f"🔍 Analysing segment {idx + 1} of {total}  ({len(chunk_pages)} pages)...")
        progress_bar.progress(idx / total)

        try:
            response = completion(
                model=model_id,
                stream=False,
                temperature=TEMPERATURE,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": build_chunk_prompt(
                        chunk_text, idx, total, sys_context
                    )}
                ]
            )
            raw = response.choices[0].message.content or ""
        except Exception as e:
            st.warning(f"⚠️ Segment {idx+1} failed ({e}) — skipping.")
            continue

        frs_csv, oq_csv, trace_csv, gap_csv = _safe_parse_chunk(raw)

        for frames, csv_text in [
            (frs_frames,   frs_csv),
            (oq_frames,    oq_csv),
            (trace_frames, trace_csv),
            (gap_frames,   gap_csv),
        ]:
            df = _csv_to_df(csv_text)
            if not df.empty:
                frames.append(df)

    progress_bar.progress(1.0)
    status_text.text("✅ All segments processed — compiling workbook...")

    def _combine(frames: list) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        combined = _remove_duplicate_headers(combined)
        combined.dropna(how='all', inplace=True)
        return combined

    frs_final   = _combine(frs_frames)
    oq_final    = _combine(oq_frames)
    trace_final = _combine(trace_frames)
    gap_final   = _combine(gap_frames)

    # Python-enforced [GAP] prefix — never rely solely on the LLM
    if (not trace_final.empty
            and "Gap_Analysis" in trace_final.columns
            and "Test_ID" in trace_final.columns):
        mask = (trace_final["Test_ID"].isna()
                | (trace_final["Test_ID"].astype(str).str.strip() == ""))
        trace_final.loc[mask, "Gap_Analysis"] = trace_final.loc[mask, "Gap_Analysis"].apply(
            lambda v: v if str(v).startswith("[GAP]") else f"[GAP] {v}"
        )

    return frs_final, oq_final, trace_final, gap_final


# =============================================================================
# 7. AUDIT LOG SHEET  (Python-owned, never hallucinated)
# =============================================================================

def build_audit_log_sheet(user: str, file_name: str, model_name: str,
                          frs_df: pd.DataFrame, oq_df: pd.DataFrame,
                          gap_df: pd.DataFrame, version_frs: int,
                          version_oq: int) -> pd.DataFrame:
    now_str   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    role      = get_user_role(user)
    gap_count = len(gap_df) if not gap_df.empty else 0

    rows = [
        {
            "Event":          "SESSION_LOGIN",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": "SESSION",
            "Old_Value":      "",
            "New_Value":      "AUTHENTICATED",
            "Reason":         "User authenticated successfully",
        },
        {
            "Event":          "DOCUMENT_UPLOADED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": "URS/SOP",
            "Old_Value":      "",
            "New_Value":      file_name,
            "Reason":         "URS file submitted for analysis",
        },
        {
            "Event":          "AI_ANALYSIS_INITIATED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": "ANALYSIS_ENGINE",
            "Old_Value":      "",
            "New_Value":      f"Model: {model_name} | Prompt: {PROMPT_VERSION} | Temp: {TEMPERATURE}",
            "Reason":         "GAMP-5 segmented analysis started",
        },
        {
            "Event":          "FRS_GENERATED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": f"FRS v{version_frs}.0",
            "Old_Value":      f"v{version_frs - 1}.0" if version_frs > 1 else "N/A",
            "New_Value":      f"v{version_frs}.0 — {len(frs_df)} requirements",
            "Reason":         "Functional requirements derived from URS",
        },
        {
            "Event":          "OQ_GENERATED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": f"OQ v{version_oq}.0",
            "Old_Value":      f"v{version_oq - 1}.0" if version_oq > 1 else "N/A",
            "New_Value":      f"v{version_oq}.0 — {len(oq_df)} test cases",
            "Reason":         "OQ test cases generated from FRS",
        },
        {
            "Event":          "GAP_ANALYSIS_COMPLETED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": "TRACEABILITY_MATRIX",
            "Old_Value":      "",
            "New_Value":      f"{gap_count} gaps identified",
            "Reason":         "RTM compiled; [GAP] and [PARTIAL GAP] flags enforced",
        },
        {
            "Event":          "WORKBOOK_EXPORTED",
            "User":           user,
            "Role":           role,
            "Timestamp":      now_str,
            "Object_Changed": "VALIDATION_PACKAGE",
            "Old_Value":      "",
            "New_Value":      f"Validation_Package_{datetime.date.today()}.xlsx",
            "Reason":         "Full package downloaded by user",
        },
    ]
    return pd.DataFrame(rows)


# =============================================================================
# 8. EXCEL STYLING
# =============================================================================

SHEET_COLORS = {
    "FRS":          {"header_fill": "2563EB", "tab_color": "2563EB"},
    "OQ":           {"header_fill": "059669", "tab_color": "059669"},
    "Traceability": {"header_fill": "7C3AED", "tab_color": "7C3AED"},
    "Gap_Analysis": {"header_fill": "DC2626", "tab_color": "DC2626"},
    "Audit_Log":    {"header_fill": "B45309", "tab_color": "B45309"},
}


def style_worksheet(ws, sheet_name: str):
    colors       = SHEET_COLORS.get(sheet_name, {"header_fill": "334155", "tab_color": "334155"})
    header_font  = Font(bold=True, color="FFFFFF", size=11)
    header_fill  = PatternFill("solid", fgColor=colors["header_fill"])
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    alt_fill     = PatternFill("solid", fgColor="F1F5F9")
    thin         = Side(style="thin", color="CBD5E1")
    border       = Border(left=thin, right=thin, top=thin, bottom=thin)
    max_col      = ws.max_column
    max_row      = ws.max_row

    for col in range(1, max_col + 1):
        cell           = ws.cell(row=1, column=col)
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = header_align
        cell.border    = border

    for row in range(2, max_row + 1):
        for col in range(1, max_col + 1):
            cell           = ws.cell(row=row, column=col)
            cell.border    = border
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if row % 2 == 0:
                cell.fill = alt_fill

    ws.auto_filter.ref           = ws.dimensions
    ws.freeze_panes              = "A2"
    ws.row_dimensions[1].height  = 30
    ws.sheet_properties.tabColor = colors["tab_color"]

    for col in range(1, max_col + 1):
        col_letter = get_column_letter(col)
        max_len    = 12
        for row in range(1, max_row + 1):
            val = ws.cell(row=row, column=col).value
            if val:
                cell_len = max(
                    len(str(val).split("\n")[0]),
                    min(len(str(val)) // 2, 40)
                )
                max_len = max(max_len, cell_len)
        ws.column_dimensions[col_letter].width = min(max_len + 4, 80)


def build_styled_excel(dataframes: dict) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        wb = writer.book
        for sheet_name in dataframes:
            if sheet_name in wb.sheetnames:
                style_worksheet(wb[sheet_name], sheet_name)
    return output.getvalue()


# =============================================================================
# 9. SESSION STATE
# =============================================================================

def get_auto_location():
    try:
        response = requests.get("http://ip-api.com/json/", timeout=5)
        data     = response.json()
        if data.get("status") == "success":
            return f"{data['city']}, {data['regionName']}, {data['countryCode']}"
        return "Location Unknown"
    except Exception:
        return "Los Angeles, USA"


_defaults = {
    "authenticated":      False,
    "selected_model":     "Gemini 1.5 Pro",
    "location":           get_auto_location(),
    "user_name":          "",
    "user_role":          "",
    "last_activity":      None,
    "sop_file_bytes":     None,
    "sop_file_name":      None,
    "sys_context_bytes":  None,
    "sys_context_name":   None,
}
for _k, _v in _defaults.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

db_migrate()

# =============================================================================
# 10. CSS  — All v15 branding preserved unchanged
# =============================================================================
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #fcfcfd; }

    /* ── Top Banner ── */
    .top-banner {
        background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
        padding: 12px 0px; text-align: center; margin-bottom: 5px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569; font-weight: 400; letter-spacing: 4px;
        text-transform: uppercase; font-size: 0.85rem; margin: 0;
    }

    /* ── Login inputs — same width as "Initialize Secure Session" button (40%) ── */
    [data-testid="stTextInput"] {
        width: 40% !important;
        min-width: 220px !important;
        margin: 0 auto !important;
    }

    /* ── Button container ── */
    div.stButton {
        width: 100% !important;
        display: flex !important;
        justify-content: center !important;
    }

    /* ── All buttons: base + universal hover ── */
    div.stButton > button {
        border: 1px solid #cbd5e1 !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        transition: background 0.18s ease, color 0.18s ease,
                    box-shadow 0.18s ease, transform 0.15s ease,
                    border-color 0.18s ease !important;
    }
    div.stButton > button:hover:not(:disabled) {
        background: #eff6ff !important;
        border-color: #3b82f6 !important;
        color: #1d4ed8 !important;
        box-shadow: 0 4px 14px rgba(59, 130, 246, 0.25) !important;
        transform: translateY(-1px) !important;
    }

    /* ── Login button ── */
    div.stButton > button[key="login_btn"] {
        width: 40% !important; margin: 0 auto !important; display: block !important;
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important; height: 3.2rem !important;
        border: none !important; font-weight: 600 !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    }
    div.stButton > button[key="login_btn"]:hover:not(:disabled) {
        background: linear-gradient(135deg, #60a5fa, #3b82f6) !important;
        color: white !important; border-color: transparent !important;
        box-shadow: 0 6px 18px rgba(37, 99, 235, 0.45) !important;
    }

    /* ── Run Analysis — iOS-inspired ── */
    div.stButton > button[key="run_analysis_btn"] {
        background-color: #007AFF !important;
        color: white !important;
        padding: 0.75rem 2.5rem !important;
        font-size: 1.05rem !important;
        font-weight: 500 !important;
        border-radius: 12px !important;
        border: none !important;
        box-shadow: 0 2px 8px rgba(0, 122, 255, 0.15) !important;
        transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1) !important;
    }
    div.stButton > button[key="run_analysis_btn"]:hover:not(:disabled) {
        background-color: #0063CC !important;
        transform: translateY(-1px) scale(1.02) !important;
        box-shadow: 0 5px 15px rgba(0, 122, 255, 0.25) !important;
        filter: none !important;
        cursor: pointer !important;
    }
    div.stButton > button[key="run_analysis_btn"]:active {
        transform: scale(0.96) !important;
        background-color: #0051A8 !important;
        box-shadow: 0 1px 4px rgba(0, 0, 0, 0.1) !important;
        transition: all 0.1s ease !important;
    }
    div.stButton > button[key="run_analysis_btn"]:disabled {
        background-color: #E9E9EB !important;
        color: #AEAEB2 !important;
        cursor: not-allowed !important;
        transform: none !important;
        box-shadow: none !important;
    }

    /* ── General disabled fallback ── */
    div.stButton > button:disabled {
        background: #e2e8f0 !important; color: #94a3b8 !important;
        cursor: not-allowed !important; transform: none !important;
        box-shadow: none !important; border-color: #e2e8f0 !important;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] { background-color: #0f172a; border-right: 1px solid #1e293b; }
    [data-testid="stSidebar"] [data-testid="stHeader"],
    [data-testid="stSidebarCollapseButton"],
    [title="keyboard_double_arrow_left"] { display: none !important; }

    .sb-title           { color: white !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sb-sub             { color: white !important; font-weight: 700 !important; font-size: 0.95rem; }
    .system-spacer      { margin-top: 80px; }
    .sys-context-spacer { margin-top: 2.4rem; }
    .sidebar-stats      { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro":    "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o":            "openai/gpt-4o",
    "Groq (Llama 3.3)":  "groq/llama-3.3-70b-versatile"
}

# =============================================================================
# 11. LOGIN
# =============================================================================

def show_login():
    left_space, center_content, right_space = st.columns([3, 4, 3])
    with center_content:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>',
                    unsafe_allow_html=True)
        st.markdown("<h1 style='text-align: center; font-size: 1.5rem;'>🛡️ LLM-Powered GxP Validation</h1>",
                    unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
        p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)
        b_left, b_center, b_right = st.columns([1, 2, 1])
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                success, err_msg = authenticate_user(u, p)
                if success:
                    st.session_state.user_name     = u
                    st.session_state.user_role     = get_user_role(u)
                    st.session_state.authenticated = True
                    st.session_state.last_activity = datetime.datetime.utcnow()
                    log_audit(u, "LOGIN_SUCCESS", "SESSION",
                              new_value=f"Role: {st.session_state.user_role}")
                    st.rerun()
                else:
                    st.error(err_msg or "Invalid credentials.")


# =============================================================================
# 12. MAIN APPLICATION
# =============================================================================

def show_app():
    # Session timeout enforcement
    if not check_session_timeout():
        user = st.session_state.get("user_name", "unknown")
        log_audit(user, "SESSION_TIMEOUT", "SESSION",
                  reason=f"Inactivity exceeded {SESSION_TIMEOUT_MINUTES} min")
        for k in ["authenticated", "user_name", "user_role", "last_activity",
                  "sop_file_bytes", "sop_file_name"]:
            st.session_state[k] = False if k == "authenticated" else (
                None if k in ["sop_file_bytes", "sop_file_name", "last_activity"] else ""
            )
        st.warning("⏱️ Session expired due to inactivity. Please log in again.")
        st.rerun()

    touch_session()

    user = st.session_state.get("user_name", "unknown")
    role = st.session_state.get("user_role", "Validator")

    # ── Sidebar ──
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🤖 Intelligence Engine</p>', unsafe_allow_html=True)

        engine_name = st.selectbox(
            "Model", list(MODELS.keys()),
            index=list(MODELS.keys()).index(st.session_state.selected_model),
            label_visibility="collapsed"
        )
        if engine_name != st.session_state.selected_model:
            st.session_state.selected_model = engine_name
            st.rerun()

        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sys-context-spacer"></div>', unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        sidebar_sys = st.file_uploader(
            "SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed"
        )
        if sidebar_sys is not None:
            raw = sidebar_sys.getvalue()
            if raw and b'%PDF' in raw[:1024]:
                st.session_state["sys_context_bytes"] = raw
                st.session_state["sys_context_name"]  = sidebar_sys.name
        elif sidebar_sys is None:
            st.session_state["sys_context_bytes"] = None
            st.session_state["sys_context_name"]  = None

        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {user}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Role: {role}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>',
                    unsafe_allow_html=True)

        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            log_audit(user, "LOGOUT", "SESSION")
            for k in ["authenticated", "user_name", "user_role", "last_activity",
                      "sop_file_bytes", "sop_file_name"]:
                st.session_state[k] = False if k == "authenticated" else (
                    None if k in ["sop_file_bytes", "sop_file_name", "last_activity"] else ""
                )
            st.rerun()

        # Admin-only panels
        if role == "Admin":
            with st.expander("🗄️ DB Status", expanded=False):
                st.markdown(f'<p class="sidebar-stats">📁 {DB_PATH}</p>', unsafe_allow_html=True)
                for table, count in db_diagnostics().items():
                    color = "#4ade80" if isinstance(count, int) and count > 0 else "#94a3b8"
                    st.markdown(
                        f'<p class="sidebar-stats" style="color:{color}">{table}: {count} rows</p>',
                        unsafe_allow_html=True
                    )

        # Admin-only user management panel
        if role == "Admin":
            with st.expander("👤 User Management", expanded=False):
                st.markdown('<p class="sidebar-stats">Create New User</p>', unsafe_allow_html=True)
                new_u = st.text_input("New Username", key="new_username_input",
                                      label_visibility="collapsed", placeholder="New Username")
                new_p = st.text_input("New Password", type="password", key="new_password_input",
                                      label_visibility="collapsed", placeholder="New Password")
                new_r = st.selectbox("New Role", ROLES, key="new_role_select",
                                     label_visibility="collapsed")
                if st.button("➕ Create User", key="create_user_btn"):
                    if new_u and new_p:
                        create_user(new_u, new_p, new_r)
                        log_audit(user, "USER_CREATED", "USER",
                                  new_value=f"{new_u} ({new_r})",
                                  reason=f"Created by Admin: {user}")
                        st.success(f"User '{new_u}' created with role: {new_r}.")
                    else:
                        st.warning("Username and password are required.")

    # ── Main area ──
    st.title("Auto-Generate Validation Package")

    sop_widget = st.file_uploader(
        "Upload URS / SOP (The 'What')", type="pdf", key="main_sop_uploader"
    )

    if sop_widget is not None:
        raw_bytes = sop_widget.getvalue()
        if raw_bytes and b'%PDF' in raw_bytes[:1024]:
            st.session_state.sop_file_bytes = raw_bytes
            st.session_state.sop_file_name  = sop_widget.name
        else:
            st.error("⚠️ Uploaded file does not appear to be a valid PDF. Please try again.")
            st.session_state.sop_file_bytes = None
            st.session_state.sop_file_name  = None
    else:
        # Widget is empty — user removed the file (clicked X) or never uploaded one.
        # Always clear retained bytes so the banner never shows a stale filename.
        st.session_state.sop_file_bytes = None
        st.session_state.sop_file_name  = None

    is_ready = st.session_state.sop_file_bytes is not None

    # Retained-file banner — only shown when file is retained but uploader widget is empty
    if is_ready and sop_widget is None and st.session_state.sop_file_name:
        st.info(
            f"📎 Retained: **{st.session_state.sop_file_name}** — model change did not clear the file."
        )

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        file_bytes = st.session_state.sop_file_bytes
        file_name  = st.session_state.sop_file_name or "unknown.pdf"
        model_id   = MODELS[st.session_state.selected_model]
        sys_ctx    = st.session_state.get("sys_context_bytes", None)

        log_audit(user, "ANALYSIS_INITIATED", "URS_FILE",
                  new_value=file_name,
                  reason=f"Model: {st.session_state.selected_model} | Prompt: {PROMPT_VERSION} | Temp: {TEMPERATURE}")
        st.info(f"⚙️ Segmented analysis started — {st.session_state.selected_model} — chunk size: {CHUNK_SIZE} pages")

        progress_bar = st.progress(0)
        status_text  = st.empty()

        try:
            frs_df, oq_df, trace_df, gap_df = run_segmented_analysis(
                file_bytes, model_id, progress_bar, status_text, sys_ctx
            )

            # Version-controlled document saves — never overwrite previous versions
            ver_frs   = save_document("FRS",          frs_df.to_csv(index=False),   user, file_name)
            ver_oq    = save_document("OQ",            oq_df.to_csv(index=False),    user, file_name)
            ver_trace = save_document("Traceability",  trace_df.to_csv(index=False), user, file_name)
            ver_gap   = save_document("Gap_Analysis",  gap_df.to_csv(index=False),   user, file_name)

            log_ai_generation(
                user, st.session_state.selected_model,
                PROMPT_VERSION, TEMPERATURE, file_name
            )

            log_audit(user, "FRS_GENERATED",          f"FRS v{ver_frs}.0",
                      new_value=f"{len(frs_df)} requirements")
            log_audit(user, "OQ_GENERATED",           f"OQ v{ver_oq}.0",
                      new_value=f"{len(oq_df)} test cases")
            log_audit(user, "TRACEABILITY_GENERATED", f"Trace v{ver_trace}.0",
                      new_value=f"{len(trace_df)} rows")
            log_audit(user, "GAP_ANALYSIS_GENERATED", f"Gap v{ver_gap}.0",
                      new_value=f"{len(gap_df)} gaps identified")

            audit_df = build_audit_log_sheet(
                user, file_name, st.session_state.selected_model,
                frs_df, oq_df, gap_df, ver_frs, ver_oq
            )

            dataframes = {
                "FRS":          frs_df,
                "OQ":           oq_df,
                "Traceability": trace_df,
                "Gap_Analysis": gap_df,
                "Audit_Log":    audit_df,
            }

            xlsx_bytes = build_styled_excel(dataframes)
            log_audit(user, "WORKBOOK_EXPORTED", "VALIDATION_PACKAGE",
                      new_value=f"Validation_Package_{datetime.date.today()}.xlsx")

            status_text.empty()
            progress_bar.empty()
            st.success("✅ Validation Package generated successfully.")

            if not gap_df.empty:
                st.warning(
                    f"⚠️ Gap Analysis: **{len(gap_df)}** requirement(s) flagged — "
                    "review the Gap_Analysis tab in the downloaded workbook."
                )

            with st.expander("📋 Preview Generated Sheets"):
                for sheet_name, df in dataframes.items():
                    st.markdown(f"**{sheet_name}** — {len(df)} rows")
                    st.dataframe(df, use_container_width=True)

            st.download_button(
                label="📥 Download Validation Workbook (.xlsx)",
                data=xlsx_bytes,
                file_name=f"Validation_Package_{datetime.date.today()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        except Exception as e:
            log_audit(user, "ANALYSIS_ERROR", "URS_FILE", reason=str(e)[:500])
            st.error(f"❌ Engineering Error: {str(e)}")


# =============================================================================
# 13. ROUTER
# =============================================================================
if not st.session_state.authenticated:
    show_login()
else:
    show_app()
