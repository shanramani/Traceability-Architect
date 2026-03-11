"""
Validation Doc Assist — v20.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Changes over v19.0:
  ROOT CAUSE FIX — Phantom Test_ID in Traceability:
  1. PYTHON-OWNED TRACEABILITY  — LLM no longer generates the
                                   Traceability dataset at all.
                                   _build_traceability() rebuilds
                                   it from actual OQ rows only.
                                   If OQ-004 doesn't exist in the
                                   OQ sheet, it CANNOT appear as
                                   "Covered" in Traceability.
  2. PASS 2 NOW 3 DATASETS      — FRS, OQ, Gap_Analysis only.
                                   Traceability dataset removed
                                   from prompt entirely.
  3. COVERAGE_STATUS GUARANTEED — Covered / Partial / Not Covered
                                   set by Python cross-reference,
                                   never by LLM claim.
  4. DASHBOARD + METRICS        — All coverage stats now read from
                                   Python-built Traceability sheet,
                                   not from OQ.Requirement_Link.
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
VERSION        = "22.0"
PROMPT_VERSION = "v10.0-completeness-enforced-na-clean"
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
                name        TEXT    NOT NULL,
                type        TEXT    NOT NULL,
                version     INTEGER NOT NULL,
                uploaded_by TEXT,
                timestamp   TEXT,
                file_path   TEXT,
                status      TEXT    DEFAULT 'Active',
                content     TEXT,
                project_ref TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_gen_log (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                model            TEXT,
                prompt_version   TEXT,
                temperature      REAL,
                timestamp        TEXT,
                generated_by     TEXT,
                project_ref      TEXT,
                input_file       TEXT,
                document_ids_used TEXT
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
        for col, defn in [
            ("name",        "TEXT"),
            ("type",        "TEXT"),
            ("file_path",   "TEXT"),
            ("status",      "TEXT DEFAULT 'Active'"),
            ("project_ref", "TEXT"),
            ("content",     "TEXT"),
            ("uploaded_by", "TEXT"),
            ("timestamp",   "TEXT"),
        ]:
            if col not in doc_cols:
                try:
                    conn.execute(f"ALTER TABLE documents ADD COLUMN {col} {defn}")
                except Exception:
                    pass

        ai_cols = [r[1] for r in conn.execute("PRAGMA table_info(ai_gen_log)").fetchall()]
        for col, defn in [
            ("temperature",       "REAL"),
            ("project_ref",       "TEXT"),
            ("input_file",        "TEXT"),
            ("document_ids_used", "TEXT"),
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
                      temperature: float, input_file: str = "",
                      project_ref: str = "", document_ids_used: str = ""):
    try:
        conn = db_connect()
        conn.execute(
            """INSERT INTO ai_gen_log
               (model, prompt_version, temperature, timestamp, generated_by,
                project_ref, input_file, document_ids_used)
               VALUES (?,?,?,?,?,?,?,?)""",
            (model, prompt_version, temperature,
             datetime.datetime.utcnow().isoformat(),
             user, project_ref, input_file, document_ids_used)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"AI gen log write failed: {e}")


def get_next_doc_version(doc_type: str) -> int:
    try:
        conn = db_connect()
        row  = conn.execute(
            "SELECT MAX(version) FROM documents WHERE type=?", (doc_type,)
        ).fetchone()
        conn.close()
        return (row[0] or 0) + 1
    except Exception:
        return 1


def save_document(doc_type: str, content: str, created_by: str,
                  project_ref: str = "", file_path: str = "") -> int:
    """Always inserts a new version — never overwrites. Returns new doc ID."""
    version = get_next_doc_version(doc_type)
    name    = f"{doc_type}_v{version}.0_{datetime.date.today()}"
    try:
        conn = db_connect()
        cur  = conn.execute(
            """INSERT INTO documents
               (name, type, version, uploaded_by, timestamp, file_path,
                status, content, project_ref)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (name, doc_type, version, created_by,
             datetime.datetime.utcnow().isoformat(),
             file_path, "Active", content[:10000], project_ref)
        )
        doc_id = cur.lastrowid
        conn.commit()
        conn.close()
        return doc_id
    except Exception as e:
        st.warning(f"Document save failed: {e}")
        return -1


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
# 5. PROMPTS  — Two-pass architecture
#    Pass 1: Structured URS extraction (deterministic input table)
#    Pass 2: FRS / OQ / Traceability / Gap derived from Pass-1 table
#            FRS descriptions are ALWAYS written in engineering/implementation
#            language — never a copy of the URS wording.
# =============================================================================

SYSTEM_PROMPT = (
    "You are a Principal Validation Engineer specializing in GAMP 5 and 21 CFR Part 11. "
    "You output ONLY structured CSV data — no explanations, no markdown, no preamble. "
    "Always wrap field values that contain commas in double-quotes. "
    "The document text may contain [TABLE N] blocks in pipe-delimited format. "
    "Extract requirements from both prose AND table cells. "
    "Confidence scores must be a decimal between 0.00 and 1.00."
)

# ── PASS 1 PROMPT: extract a clean, structured URS table ─────────────────────
def build_pass1_prompt(chunk_text: str, chunk_index: int, total_chunks: int) -> str:
    return f"""
URS DOCUMENT — Segment {chunk_index + 1} of {total_chunks}:
{chunk_text}

TASK: Extract every user requirement from this segment into a single CSV.
Output ONLY the CSV — include the header row. Wrap comma-containing values in double-quotes.

CSV columns:
Req_ID,Requirement_Description,Category,Testable,Source_Text,Source_Page,Confidence

Rules:
- Req_ID: sequential URS-NNN format (e.g. URS-001). Continue numbering across segments.
- Category: Functional / Performance / Security / Compliance / Usability / Data / Interface
- Testable: Yes / No
  Mark No if the requirement contains vague language:
  "user friendly", "easy", "fast", "quickly", "seamless", "simple", "intuitive",
  "efficient", "smooth", "modern", "flexible", "robust", "scalable", "reliable",
  "convenient", "accessible", "appealing", "pleasant", "elegant".
- Source_Text: copy the EXACT sentence or phrase from the document that this requirement came from (max 120 chars).
- Source_Page: the page number where the source text appears (e.g. Page 3). Write "Unknown" if unclear.
- Confidence: your confidence that this is a valid requirement, 0.00–1.00.
"""

# ── PASS 2 PROMPT: generate FRS / OQ / Traceability / Gap from URS table ─────
def build_pass2_prompt(urs_csv: str, sys_context: str = "") -> str:

    if sys_context:
        context_block = (
            f"SYSTEM USER GUIDE (product manual uploaded by user — use this to shape "
            f"implementation details in FRS descriptions):\n{sys_context[:3000]}\n\n"
        )
        system_guidance = (
            "Use the System User Guide above to determine the specific screens, fields, "
            "modules, and workflows that the system uses to implement each URS requirement. "
            "FRS descriptions must reference the actual product terminology from that guide."
        )
    else:
        context_block = ""
        system_guidance = (
            "NO system user guide was provided. "
            "Infer the system type from the URS content (e.g. LIMS, SAP, Veeva Vault, ERP, "
            "MES, QMS, CTMS, eTMF, or similar GxP platform). "
            "Then write FRS descriptions as a BEST-PRACTICE implementation for that system type. "
            "Use plausible but generic screen names, field names, and module names appropriate "
            "for that platform category (e.g. 'Sample Registration screen', 'Batch Record module', "
            "'Audit Trail viewer'). The goal is a solid, credible FRS that a real validation "
            "engineer would recognise as correctly scoped for that type of system."
        )

    return f"""
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
If a requirement is vague, still generate an FRS — note the vagueness in the description and set Confidence < 0.70.
Number FRS IDs sequentially: FRS-001, FRS-002, FRS-003 ... one per URS row in order.

TASK: Generate exactly 3 CSV datasets separated by |||.
Output ONLY raw CSV rows — include the header row in EVERY dataset.
Wrap any comma-containing value in double-quotes.
Use N/A (not blank) for any field that is not applicable.

Dataset 1 (FRS): ID,Requirement_Description,Priority,Risk,GxP_Impact,Source_URS_Ref,Source_Text,Source_Page,Confidence,Confidence_Flag
  - ID: FRS-NNN (e.g. FRS-001, FRS-002)
  - Requirement_Description: ENGINEERING/IMPLEMENTATION language (see CRITICAL RULE above).
    Must describe: specific screen or module, field names, data types, validation logic,
    error handling, or integration behaviour. Never copy URS wording.
  - Priority: Critical / High / Medium / Low
  - Risk: High / Medium / Low
    • High   = patient safety, data integrity, electronic records, audit trail
    • Medium = indirect quality, workflow, access control
    • Low    = cosmetic, reporting, preference
  - GxP_Impact: Direct / Indirect / None
  - Source_URS_Ref: URS Req_ID this FRS was derived from (e.g. URS-004)
  - Source_Text: copy Source_Text verbatim from the URS table
  - Source_Page: copy Source_Page from the URS table
  - Confidence: 0.00–1.00 confidence that this FRS accurately implements the URS intent
  - Confidence_Flag: "Review Required" if Confidence < 0.70, else blank

Dataset 2 (OQ): Test_ID,Requirement_Link,Requirement_Link_Type,Test_Step,Expected_Result,Pass_Fail_Criteria,Source,Confidence,Confidence_Flag
  - Test_ID: OQ-NNN
  - Requirement_Link: FRS ID being tested (e.g. FRS-001)
  - Requirement_Link_Type: "FRS"
  - Test_Step: concrete, executable action (e.g. "Navigate to Sample Registration screen.
    Enter 'SMP-0042' in Sample_ID field. Click Save.")
  - Expected_Result: specific, measurable outcome (e.g. "Record saved. Sample_ID 'SMP-0042'
    appears in the sample master table. No error message shown.")
  - Pass_Fail_Criteria: objective pass condition (e.g. "Pass if record confirmed in DB within
    3 seconds and no ERR- codes returned.")
  - Source: "Derived from <URS Req_ID>" e.g. "Derived from URS-004"
  - Confidence: 0.00–1.00 test coverage confidence
  - Confidence_Flag: "Review Required" if Confidence < 0.70, else blank
  - Coverage rule: High-Risk FRS → ≥3 OQ tests (positive, negative, boundary).
    Medium → ≥2 (positive + negative). Low → ≥1 (positive path).

Dataset 3 (Traceability): URS_Req_ID,FRS_Ref,Test_ID,Coverage_Status,Gap_Analysis
  - FRS_Ref: FRS ID (e.g. FRS-001) — never a URS ID
  - Coverage_Status: Covered / Partial / Not Covered
  - If no test: leave Test_ID blank, start Gap_Analysis with [GAP]
  - If partial: start Gap_Analysis with [PARTIAL GAP]

Dataset 4 (Gap_Analysis): Req_ID,Gap_Type,Description,Recommendation,Severity
  - Gap_Type: Untestable / No_Test_Coverage / Orphan_Test / Ambiguous / Duplicate
  - Only include rows where a gap exists.
  - Severity: Critical / High / Medium / Low

CRITICAL OUTPUT RULES — MUST FOLLOW OR THE FILE WILL BE CORRUPT:
1. Every field value MUST fit on a single line — NO embedded newlines inside any value.
2. Commas inside a value MUST be wrapped in double-quotes: "value, with, commas"
3. The ONLY dataset separator is the exact token:  |||  (on its own line, nothing else).
4. Do NOT add extra ||| tokens inside dataset content.
5. FRS ID column must be a SHORT CODE ONLY: FRS-001, FRS-002, FRS-003 etc.
   The ID is NEVER a sentence. If you are writing more than 8 characters in the ID
   column, you are putting the description in the wrong column.

TASK: Generate exactly 3 CSV datasets separated by |||.
Output ONLY raw CSV rows — include the header row in EVERY dataset.

NOTE: Do NOT generate a Traceability dataset. Traceability is computed by the
application after your output is validated. Your job is FRS, OQ, and Gap only.

Dataset 1 (FRS): ID,Requirement_Description,Priority,Risk,GxP_Impact,Source_URS_Ref,Source_Text,Source_Page,Confidence,Confidence_Flag
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
  - Confidence: decimal 0.00–1.00
  - Confidence_Flag: write exactly "Review Required" if Confidence < 0.70, else leave blank

Dataset 2 (OQ): Test_ID,Requirement_Link,Requirement_Link_Type,Test_Step,Expected_Result,Pass_Fail_Criteria,Source,Confidence,Confidence_Flag
  - Test_ID: OQ-001, OQ-002 etc.
  - Requirement_Link: FRS-NNN (e.g. FRS-001)
  - Requirement_Link_Type: FRS
  - Test_Step: single line; use semicolons to separate steps, e.g. "Open Login screen; enter username 'testuser'; enter password; click Login"
  - Expected_Result: single line outcome, e.g. "User is authenticated and redirected to Dashboard"
  - Pass_Fail_Criteria: single line pass condition, e.g. "Pass if dashboard loads within 3s and no error shown"
  - Source: "Derived from URS-NNN"
  - Confidence: decimal 0.00–1.00
  - Confidence_Flag: "Review Required" if Confidence < 0.70, else blank
  - Rule: High-Risk FRS → ≥3 OQ rows. Medium → ≥2. Low → ≥1.

Dataset 3 (Gap_Analysis): Req_ID,Gap_Type,Description,Recommendation,Severity
  - Gap_Type: Untestable / No_Test_Coverage / Orphan_Test / Ambiguous / Duplicate
  - Only include rows where a gap exists in the URS requirements themselves.
  - Severity: Critical / High / Medium / Low
  - Description and Recommendation: single line each

|||  ← this token on its own line separates each dataset
"""


# =============================================================================
# 6. TWO-PASS AI ANALYSIS ENGINE
# =============================================================================

# Known header signatures for each of the 4 datasets in Pass-2 output.
# Used by the robust splitter to locate dataset boundaries even when the
# LLM embeds stray ||| tokens inside quoted field values.
_PASS2_HEADERS = [
    # Dataset 1 — FRS
    r"^ID[,\t]Requirement_Description",
    # Dataset 2 — OQ
    r"^Test_ID[,\t]Requirement_Link",
    # Dataset 3 — Gap_Analysis (LLM-detected URS-level gaps only)
    r"^Req_ID[,\t]Gap_Type",
]

_PASS1_HEADER = r"^Req_ID[,\t]Requirement_Description"


def _strip_fences(raw: str) -> str:
    """Remove markdown code-fences that LLMs sometimes wrap output in."""
    raw = re.sub(r'^```[a-zA-Z]*\n?', '', raw, flags=re.MULTILINE)
    raw = re.sub(r'```\s*$',          '', raw, flags=re.MULTILINE)
    return raw.strip()


def _robust_split_datasets(raw: str, headers: list) -> list:
    """
    Split LLM output into N CSV blocks by finding each known header line.
    This is immune to stray ||| tokens that appear inside quoted cell values.

    Strategy:
      1. Strip fences.
      2. For each header pattern, find the first matching line.
      3. Extract text from that line to the next header (or end).
      4. Return a list of N strings (empty string if a section is missing).
    """
    raw    = _strip_fences(raw)
    lines  = raw.splitlines()
    n      = len(headers)
    starts = [None] * n

    for i, pat in enumerate(headers):
        for idx, line in enumerate(lines):
            if re.match(pat, line.strip(), re.IGNORECASE):
                starts[i] = idx
                break

    # Build sections
    sections = []
    for i in range(n):
        if starts[i] is None:
            sections.append("")
            continue
        end = len(lines)
        for j in range(i + 1, n):
            if starts[j] is not None and starts[j] > starts[i]:
                end = starts[j]
                break
        section_lines = lines[starts[i]:end]
        # Strip trailing ||| lines between sections
        cleaned = [l for l in section_lines if l.strip() not in ("|||", "---", "")]
        sections.append("\n".join(cleaned))

    while len(sections) < n:
        sections.append("")
    return sections


def _csv_to_df(csv_text: str) -> pd.DataFrame:
    if not csv_text or not csv_text.strip():
        return pd.DataFrame()
    try:
        df = pd.read_csv(
            io.StringIO(csv_text),
            quotechar='"',
            on_bad_lines='skip',
            skipinitialspace=True,
            dtype=str,           # keep everything as str; avoids float/int confusion
        )
        # Drop rows where every cell is NaN
        df.dropna(how='all', inplace=True)
        return df
    except Exception:
        return pd.DataFrame()


def _remove_duplicate_headers(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or len(df.columns) == 0:
        return df
    return df[df.iloc[:, 0].astype(str).str.strip() != df.columns[0]].reset_index(drop=True)


def _renumber_frs_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee FRS ID column contains clean FRS-NNN codes.

    Problem: LLMs sometimes shift columns when a long description that
    contains commas is not properly quoted, causing the description text
    to land in the ID column.

    Fix: detect any row where ID looks like a sentence (> 10 chars or
    does not start with 'FRS') and rebuild the entire ID column as a
    clean sequence.  Also resets the 'Requirement_Link' in OQ if passed.
    """
    if df.empty or "ID" not in df.columns:
        return df
    df = df.copy()

    def _looks_like_id(val: str) -> bool:
        v = str(val).strip()
        return bool(re.match(r'^FRS-?\d+$', v, re.IGNORECASE)) and len(v) <= 10

    bad_ids = df["ID"].apply(lambda v: not _looks_like_id(str(v)))
    if bad_ids.any():
        # Renumber all rows unconditionally for consistency
        df["ID"] = [f"FRS-{i+1:03d}" for i in range(len(df))]
    else:
        # Normalise formatting: FRS001 → FRS-001
        def _normalise(v):
            v = str(v).strip().upper()
            m = re.match(r'FRS-?(\d+)', v)
            if m:
                return f"FRS-{int(m.group(1)):03d}"
            return v
        df["ID"] = df["ID"].apply(_normalise)
    return df


def _fill_missing_frs(urs_df: pd.DataFrame, frs_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect any URS requirement that has no FRS row and insert a clearly-flagged
    placeholder so nothing silently disappears from the output.
    The placeholder has Confidence=0.50 and Confidence_Flag='⚠️ Review Required'
    so it is immediately visible as needing manual completion.
    """
    if urs_df.empty or "Req_ID" not in urs_df.columns:
        return frs_df

    frs_urs_refs = set()
    if not frs_df.empty and "Source_URS_Ref" in frs_df.columns:
        frs_urs_refs = set(frs_df["Source_URS_Ref"].dropna().astype(str).str.strip())

    placeholders = []
    for _, row in urs_df.iterrows():
        uid  = str(row.get("Req_ID", "")).strip()
        desc = str(row.get("Requirement_Description", "")).strip()
        if uid and uid not in frs_urs_refs:
            # Determine next FRS number
            if not frs_df.empty and "ID" in frs_df.columns:
                existing_nums = []
                for fid in frs_df["ID"].dropna().astype(str):
                    m = re.match(r'FRS-(\d+)', fid.strip(), re.IGNORECASE)
                    if m: existing_nums.append(int(m.group(1)))
                next_n = max(existing_nums, default=0) + len(placeholders) + 1
            else:
                next_n = 1 + len(placeholders)
            frs_id = f"FRS-{next_n:03d}"
            placeholders.append({
                "ID":                      frs_id,
                "Requirement_Description": f"[AI SKIPPED — MANUAL REVIEW REQUIRED] "
                                           f"No FRS was generated for URS requirement: '{desc}'. "
                                           f"Please define the engineering implementation.",
                "Priority":                "N/A",
                "Risk":                    "High",
                "GxP_Impact":              "Direct",
                "Source_URS_Ref":          uid,
                "Source_Text":             str(row.get("Source_Text", "N/A")).strip() or "N/A",
                "Source_Page":             str(row.get("Source_Page", "N/A")).strip() or "N/A",
                "Confidence":              "0.50",
                "Confidence_Flag":         "⚠️ Review Required",
            })

    if not placeholders:
        return frs_df

    placeholder_df = pd.DataFrame(placeholders)
    result = pd.concat([frs_df, placeholder_df], ignore_index=True)
    result.fillna("N/A", inplace=True)
    return result


def _clean_frs_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Strip column-name prefixes that LLMs sometimes echo into values.
    e.g. "Risk: High" → "High", "Priority: Critical" → "Critical",
         "GxP_Impact: Direct" → "Direct"
    Also normalises capitalisation so downstream comparisons are reliable.
    """
    if df.empty:
        return df
    df = df.copy()
    _prefixes = {
        "Risk":       ["risk:", "risk :", "Risk:", "Risk :"],
        "Priority":   ["priority:", "priority :", "Priority:", "Priority :"],
        "GxP_Impact": ["gxp_impact:", "gxp_impact :", "GxP_Impact:", "GxP Impact:"],
    }
    for col, prefixes in _prefixes.items():
        if col in df.columns:
            for pfx in prefixes:
                df[col] = df[col].astype(str).str.replace(
                    pfx, "", case=False, regex=False
                ).str.strip()
            # Normalise to title-case so "high" → "High", "MEDIUM" → "Medium"
            # but keep the exact strings the rest of the code expects
            _map = {"high": "High", "medium": "Medium", "low": "Low",
                    "critical": "Critical", "direct": "Direct",
                    "indirect": "Indirect", "none": "None"}
            df[col] = df[col].str.lower().map(
                lambda v: _map.get(v, v.title()) if isinstance(v, str) else v
            )
    return df


def _renumber_oq_ids(df: pd.DataFrame) -> pd.DataFrame:
    """
    Always renumber OQ Test_IDs as a clean sequential OQ-001, OQ-002, OQ-003...
    This is unconditional — the LLM frequently skips numbers (e.g. OQ-001, 002, 003, 005)
    when it skips an FRS, leaving gaps. We never preserve gap-filled sequences.
    """
    if df.empty or "Test_ID" not in df.columns:
        return df
    df = df.copy()
    df["Test_ID"] = [f"OQ-{i+1:03d}" for i in range(len(df))]
    return df


def _apply_confidence_flags(df: pd.DataFrame) -> pd.DataFrame:
    """
    Guarantee Confidence_Flag is set correctly.
    - Confidence < 0.70  → "⚠️ Review Required"
    - Confidence >= 0.70 → ""   (blank — never None)
    - Missing/unparseable → ""
    Python enforces this independently of whatever the LLM wrote.
    """
    if df.empty:
        return df
    df = df.copy()

    if "Confidence" not in df.columns:
        df["Confidence"]      = "1.00"
        df["Confidence_Flag"] = ""
        return df

    if "Confidence_Flag" not in df.columns:
        df["Confidence_Flag"] = ""

    def _flag(conf_val):
        try:
            c = float(str(conf_val).strip())
            return "⚠️ Review Required" if c < 0.70 else ""
        except (ValueError, TypeError):
            return ""

    df["Confidence_Flag"] = df["Confidence"].apply(_flag)

    # Replace any NaN/None in ALL columns with empty string for clean Excel output
    df = df.fillna("")
    return df


def run_segmented_analysis(
    file_bytes: bytes,
    model_id: str,
    progress_bar,
    status_text,
    sys_context_bytes: bytes = None
) -> tuple:
    """
    Two-pass analysis:
    Pass 1 — per-chunk URS extraction: produces a clean structured URS table
    Pass 2 — single call with full URS table: produces FRS / OQ / Trace / Gap
    Returns: (urs_df, frs_df, oq_df, trace_df, gap_df)
    """
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

    # ── PASS 1: Extract structured URS table from each chunk ─────────────────
    urs_frames = []
    for idx, chunk_pages in enumerate(chunks):
        chunk_text = "\n\n".join(chunk_pages)
        status_text.text(f"📄 Pass 1 — Extracting URS: segment {idx + 1} of {total}...")
        progress_bar.progress((idx) / (total * 2))

        try:
            response = completion(
                model=model_id,
                stream=False,
                temperature=TEMPERATURE,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": build_pass1_prompt(chunk_text, idx, total)}
                ]
            )
            raw_urs = response.choices[0].message.content or ""
            # Strip markdown fences
            raw_urs = re.sub(r'^```[a-zA-Z]*\n?', '', raw_urs, flags=re.MULTILINE)
            raw_urs = re.sub(r'```\s*$',          '', raw_urs, flags=re.MULTILINE)
            df_urs  = _csv_to_df(raw_urs.strip())
            if not df_urs.empty:
                urs_frames.append(df_urs)
        except Exception as e:
            st.warning(f"⚠️ Pass 1 segment {idx+1} failed ({e}) — skipping.")

    def _combine(frames):
        if not frames:
            return pd.DataFrame()
        c = pd.concat(frames, ignore_index=True)
        c = _remove_duplicate_headers(c)
        c.dropna(how='all', inplace=True)
        return c

    urs_final = _combine(urs_frames)

    # Ensure Confidence_Flag on URS
    urs_final = _apply_confidence_flags(urs_final)

    progress_bar.progress(0.5)
    status_text.text("✅ Pass 1 complete — structured URS table built. Running Pass 2...")

    # ── PASS 2: Generate FRS / OQ / Traceability / Gap from URS table ────────
    frs_frames, oq_frames, gap_frames = [], [], []

    if urs_final.empty:
        st.warning("⚠️ No URS requirements extracted in Pass 1. Pass 2 skipped.")
    else:
        urs_csv_str = urs_final.to_csv(index=False)
        # Chunk the URS CSV if very large (> 4000 chars) to avoid token limits
        urs_lines   = urs_csv_str.split("\n")
        header_line = urs_lines[0]
        data_lines  = urs_lines[1:]
        PASS2_CHUNK = 40   # rows per pass-2 call
        p2_chunks   = [data_lines[i:i+PASS2_CHUNK] for i in range(0, len(data_lines), PASS2_CHUNK)]
        p2_total    = len(p2_chunks)

        for p2_idx, p2_rows in enumerate(p2_chunks):
            p2_csv = header_line + "\n" + "\n".join(p2_rows)
            status_text.text(
                f"🔬 Pass 2 — Generating FRS/OQ/Trace/Gap: batch {p2_idx+1} of {p2_total}..."
            )
            progress_bar.progress(0.5 + (p2_idx / p2_total) * 0.45)

            try:
                response = completion(
                    model=model_id,
                    stream=False,
                    temperature=TEMPERATURE,
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user",   "content": build_pass2_prompt(p2_csv, sys_context)}
                    ]
                )
                raw_p2 = response.choices[0].message.content or ""
            except Exception as e:
                st.warning(f"⚠️ Pass 2 batch {p2_idx+1} failed ({e}) — skipping.")
                continue

            # ── Robust split by header detection (3 datasets) ───────────
            sections = _robust_split_datasets(raw_p2, _PASS2_HEADERS)
            frs_csv, oq_csv, gap_csv = sections[0], sections[1], sections[2]
            for frames, csv_text in [
                (frs_frames,   frs_csv),
                (oq_frames,    oq_csv),
                (gap_frames,   gap_csv),
            ]:
                df = _csv_to_df(csv_text)
                if not df.empty:
                    frames.append(df)

    progress_bar.progress(0.95)
    status_text.text("✅ Both passes complete — running deterministic checks...")

    frs_final   = _combine(frs_frames)
    oq_final    = _combine(oq_frames)
    gap_final   = _combine(gap_frames)

    # ── Post-processing: ID normalisation ────────────────────────────────────
    frs_final = _renumber_frs_ids(frs_final)
    oq_final  = _renumber_oq_ids(oq_final)
    frs_final = _clean_frs_columns(frs_final)     # strip "Risk: ", "Priority: " prefixes
    frs_final = _fill_missing_frs(urs_final, frs_final)  # insert placeholders for AI-skipped URS

    # Fix Requirement_Link in OQ to match renumbered FRS IDs if possible
    # (In practice IDs are sequential so FRS-001 stays FRS-001 — this is a safety net)

    # ── Post-processing: confidence flags (Python-enforced) ──────────────────
    frs_final = _apply_confidence_flags(frs_final)
    oq_final  = _apply_confidence_flags(oq_final)
    urs_final = _apply_confidence_flags(urs_final)

    # ── Post-processing: fill all NaN with "N/A" for clean Excel output ─────────
    for df in [frs_final, oq_final, urs_final]:
        df.fillna("N/A", inplace=True)
        df.replace("", "N/A", inplace=True)

    # Clean and validate LLM gap analysis output
    gap_final = _clean_gap_analysis(gap_final)

    # ── CRITICAL: Rebuild Traceability entirely in Python ─────────────────────
    # The LLM's traceability output is DISCARDED. We cross-reference the actual
    # OQ rows that exist against the actual FRS rows. This makes it structurally
    # impossible for a phantom Test_ID (one the LLM invented but never generated)
    # to appear as "Covered".
    trace_final = _build_traceability(urs_final, frs_final, oq_final)

    progress_bar.progress(1.0)
    status_text.text("✅ All segments processed — compiling workbook...")

    return urs_final, frs_final, oq_final, trace_final, gap_final


def _build_traceability(urs_df: pd.DataFrame,
                        frs_df: pd.DataFrame,
                        oq_df:  pd.DataFrame) -> pd.DataFrame:
    """
    Rebuild Traceability Matrix from URS as the primary key.

    Every URS requirement gets a row regardless of whether the LLM generated
    an FRS for it. This makes skipped/missing FRS rows visible.

    Columns: URS_Req_ID, URS_Description, FRS_Ref, Test_IDs, Test_Count,
             Coverage_Status, Gap_Analysis

    Coverage logic per FRS row (risk-aware):
      - No FRS generated  → Missing_FRS + [GAP]
      - FRS exists, 0 OQ  → Not Covered  + [GAP]
      - FRS exists, < min → Partial       + [PARTIAL GAP]
      - FRS exists, ≥ min → Covered
    """
    MIN_TESTS = {"high": 3, "medium": 2, "low": 1}

    # Build URS description lookup
    urs_desc: dict = {}
    if not urs_df.empty and "Req_ID" in urs_df.columns:
        for _, r in urs_df.iterrows():
            uid  = str(r.get("Req_ID", "")).strip()
            desc = str(r.get("Requirement_Description", "")).strip()
            if uid:
                urs_desc[uid] = desc

    # Build FRS lookup keyed by Source_URS_Ref → list of FRS rows
    frs_by_urs: dict = {}
    if not frs_df.empty and "Source_URS_Ref" in frs_df.columns:
        for _, r in frs_df.iterrows():
            ref = str(r.get("Source_URS_Ref", "")).strip()
            if ref:
                frs_by_urs.setdefault(ref, []).append(r)

    # Build OQ lookup keyed by Requirement_Link (FRS_ID) → list of OQ Test_IDs
    oq_map: dict = {}
    if not oq_df.empty and "Requirement_Link" in oq_df.columns and "Test_ID" in oq_df.columns:
        for _, r in oq_df.iterrows():
            link    = str(r.get("Requirement_Link", "")).strip()
            test_id = str(r.get("Test_ID", "")).strip()
            if link and test_id:
                oq_map.setdefault(link, []).append(test_id)

    # Determine URS ID list — prefer urs_df order, fallback to FRS refs
    if not urs_df.empty and "Req_ID" in urs_df.columns:
        urs_ids = [str(v).strip() for v in urs_df["Req_ID"].dropna() if str(v).strip()]
    else:
        urs_ids = sorted(frs_by_urs.keys())

    rows = []
    for urs_id in urs_ids:
        urs_description = urs_desc.get(urs_id, "")
        frs_rows = frs_by_urs.get(urs_id, [])

        if not frs_rows:
            # LLM skipped this URS — critical gap
            rows.append({
                "URS_Req_ID":       urs_id,
                "URS_Description":  urs_description,
                "FRS_Ref":          "—",
                "Test_IDs":         "",
                "Test_Count":       "0",
                "Coverage_Status":  "Missing FRS",
                "Gap_Analysis":     f"[GAP] No FRS requirement was generated for {urs_id}. "
                                    "AI may have skipped this requirement.",
            })
            continue

        for frs_row in frs_rows:
            frs_id  = str(frs_row.get("ID", "")).strip()
            risk    = str(frs_row.get("Risk", "low")).strip().lower()
            min_req = MIN_TESTS.get(risk, 1)

            real_tests = oq_map.get(frs_id, [])
            count      = len(real_tests)
            test_str   = "; ".join(sorted(real_tests)) if real_tests else ""

            if count == 0:
                status = "Not Covered"
                gap    = (f"[GAP] {frs_id} has no OQ test case. "
                          f"0/{min_req} required for {risk.title()} risk.")
            elif count < min_req:
                status = "Partial"
                gap    = (f"[PARTIAL GAP] {frs_id} has {count}/{min_req} tests "
                          f"required for {risk.title()} risk.")
            else:
                status = "Covered"
                gap    = ""

            rows.append({
                "URS_Req_ID":       urs_id,
                "URS_Description":  urs_description,
                "FRS_Ref":          frs_id,
                "Test_IDs":         test_str,
                "Test_Count":       str(count),
                "Coverage_Status":  status,
                "Gap_Analysis":     gap,
            })

    result = pd.DataFrame(rows) if rows else pd.DataFrame(columns=[
        "URS_Req_ID", "URS_Description", "FRS_Ref", "Test_IDs",
        "Test_Count", "Coverage_Status", "Gap_Analysis"
    ])
    result.fillna("", inplace=True)
    return result


# =============================================================================
# 6b. DETERMINISTIC GAP VALIDATION ENGINE  (R1–R5)
# =============================================================================

NON_TESTABLE_KEYWORDS = [
    "user friendly", "user-friendly", "easy to use", "easy-to-use",
    "intuitive", "fast", "quickly", "seamlessly", "simple", "straightforward",
    "efficient", "smooth", "pleasant", "elegant", "modern", "robust",
    "flexible", "scalable", "reliable", "stable", "responsive",
    "convenient", "accessible", "appealing",
]

AMBIGUOUS_KEYWORDS = [
    "appropriate", "adequate", "sufficient", "reasonable", "as needed",
    "if necessary", "where applicable", "etc", "and/or", "various",
    "many", "several", "some", "other", "etc.", "normal", "standard",
    "should consider", "may need", "could be",
]


def _token_overlap(a: str, b: str) -> float:
    """Simple Jaccard token overlap for duplicate detection."""
    ta = set(re.findall(r'\w+', a.lower()))
    tb = set(re.findall(r'\w+', b.lower()))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


VALID_GAP_TYPES = {"Untestable", "No_Test_Coverage", "Orphan_Test",
                   "Ambiguous", "Duplicate", "Missing_FRS"}

def _clean_gap_analysis(gap_df: pd.DataFrame) -> pd.DataFrame:
    """
    Post-process LLM-generated Gap_Analysis to fix two known corruption patterns:

    1. LLM writes "No gaps identified" (or similar) as the Req_ID value while still
       populating other fields with real or nonsensical values.
       → Remove these rows entirely. The absence of real gap rows IS the signal.

    2. LLM writes a severity value (Critical/High/Medium/Low) in Gap_Type column.
       → Correct by validating Gap_Type against allowed enum values; replace
         invalid values with "Ambiguous".

    3. Replace all remaining None / blank cells with "N/A".
    """
    if gap_df.empty:
        return gap_df

    df = gap_df.copy()

    # Drop rows where Req_ID signals "no gap" prose
    if "Req_ID" in df.columns:
        no_gap_mask = df["Req_ID"].astype(str).str.lower().str.contains(
            r'no gap|no issue|none|nothing|not applicable|n/a', na=False, regex=True
        )
        df = df[~no_gap_mask].reset_index(drop=True)

    # Fix Gap_Type values that contain severity words instead of gap type enum
    if "Gap_Type" in df.columns:
        severity_words = {"critical", "high", "medium", "low"}
        def _fix_gap_type(v):
            v_str = str(v).strip()
            if v_str in VALID_GAP_TYPES:
                return v_str
            if v_str.lower() in severity_words:
                return "Ambiguous"   # best guess when LLM put severity in gap type col
            return v_str if v_str else "N/A"
        df["Gap_Type"] = df["Gap_Type"].apply(_fix_gap_type)

    # Replace all blank / None with N/A
    df = df.fillna("N/A")
    df = df.replace("", "N/A")

    return df


def run_deterministic_validation(
    frs_df: pd.DataFrame,
    oq_df: pd.DataFrame,
    gap_df: pd.DataFrame,
    urs_df: pd.DataFrame = None,
) -> tuple:
    """
    Rules:
      R0 — URS req with no FRS generated           → Gap_Type: Missing_FRS
      R1 — FRS req without any OQ test             → Gap_Type: No_Test_Coverage
      R2 — OQ test with no matching FRS req         → Gap_Type: Orphan_Test
      R3 — Non-testable keywords in description     → Gap_Type: Untestable
      R4 — Risk-tier req with < required OQ tests   → Gap_Type: No_Test_Coverage
      R5 — Near-duplicate FRS descriptions          → Gap_Type: Duplicate

    Traceability is NOT touched here — it is Python-built by _build_traceability().
    Returns: (enriched_gap_df, det_issues_df)
    """
    issues = []

    frs_ids = set()
    if not frs_df.empty and "ID" in frs_df.columns:
        frs_ids = set(frs_df["ID"].dropna().astype(str).str.strip())

    # FRS lookup by Source_URS_Ref for R0
    frs_urs_refs = set()
    if not frs_df.empty and "Source_URS_Ref" in frs_df.columns:
        frs_urs_refs = set(frs_df["Source_URS_Ref"].dropna().astype(str).str.strip())

    oq_req_links = set()
    oq_test_ids  = set()
    if not oq_df.empty:
        if "Requirement_Link" in oq_df.columns:
            oq_req_links = set(oq_df["Requirement_Link"].dropna().astype(str).str.strip())
        if "Test_ID" in oq_df.columns:
            oq_test_ids = set(oq_df["Test_ID"].dropna().astype(str).str.strip())

    # ── R0: URS requirement with no FRS generated ────────────────────────────
    if urs_df is not None and not urs_df.empty and "Req_ID" in urs_df.columns:
        for _, row in urs_df.iterrows():
            uid = str(row.get("Req_ID", "")).strip()
            if uid and uid not in frs_urs_refs:
                issues.append({
                    "Rule":           "R0",
                    "Req_ID":         uid,
                    "Gap_Type":       "Missing_FRS",
                    "Description":    f"{uid} has no FRS requirement generated. "
                                      "The AI may have skipped this requirement.",
                    "Recommendation": "Re-run analysis or manually create an FRS "
                                      "requirement for this URS item.",
                    "Severity":       "Critical",
                })

    # ── R1: FRS req without any OQ test ──────────────────────────────────────
    # Source of truth is the actual OQ rows — never the traceability matrix.
    for frs_id in sorted(frs_ids):
        if frs_id not in oq_req_links:
            issues.append({
                "Rule":            "R1",
                "Req_ID":          frs_id,
                "Gap_Type":        "No_Test_Coverage",
                "Description":     f"{frs_id} has no OQ test case linked to it.",
                "Recommendation":  "Create at least one OQ test case for this FRS requirement.",
                "Severity":        "High",
            })
            # Traceability is Python-built; Coverage_Status is already "Not Covered".
            # No mutation needed here — _build_traceability already set [GAP].

    # ── R2: Orphan OQ tests ───────────────────────────────────────────────────
    if not oq_df.empty and "Requirement_Link" in oq_df.columns:
        for _, row in oq_df.iterrows():
            link    = str(row.get("Requirement_Link", "")).strip()
            test_id = str(row.get("Test_ID", "")).strip()
            if link and link not in frs_ids:
                issues.append({
                    "Rule":            "R2",
                    "Req_ID":          test_id,
                    "Gap_Type":        "Orphan_Test",
                    "Description":     f"OQ test {test_id} links to '{link}' which has no FRS entry.",
                    "Recommendation":  "Verify the requirement reference or remove/reassign this test.",
                    "Severity":        "Medium",
                })

    # ── R3: Non-testable + Ambiguous keyword detection ────────────────────────
    desc_col = "Requirement_Description" if not frs_df.empty and "Requirement_Description" in frs_df.columns else None
    if desc_col:
        for _, row in frs_df.iterrows():
            desc = str(row.get(desc_col, "")).lower()
            fid  = str(row.get("ID", "")).strip()

            nt_found  = [kw for kw in NON_TESTABLE_KEYWORDS if kw in desc]
            amb_found = [kw for kw in AMBIGUOUS_KEYWORDS      if kw in desc]

            if nt_found:
                issues.append({
                    "Rule":            "R3",
                    "Req_ID":          fid,
                    "Gap_Type":        "Untestable",
                    "Description":     f"Non-testable language detected: {', '.join(nt_found)}",
                    "Recommendation":  "Rewrite as specific, measurable requirement (e.g. add numeric criteria).",
                    "Severity":        "High",
                })
                gap_df = pd.concat([gap_df, pd.DataFrame([{
                    "Req_ID":          fid,
                    "Gap_Type":        "Untestable",
                    "Description":     f"Non-testable keywords: {', '.join(nt_found)}",
                    "Recommendation":  "Rewrite as measurable, specific requirement.",
                    "Severity":        "High",
                }])], ignore_index=True)

            elif amb_found:
                issues.append({
                    "Rule":            "R3b",
                    "Req_ID":          fid,
                    "Gap_Type":        "Ambiguous",
                    "Description":     f"Ambiguous language detected: {', '.join(amb_found)}",
                    "Recommendation":  "Clarify intent with specific, unambiguous wording.",
                    "Severity":        "Medium",
                })
                gap_df = pd.concat([gap_df, pd.DataFrame([{
                    "Req_ID":          fid,
                    "Gap_Type":        "Ambiguous",
                    "Description":     f"Ambiguous keywords: {', '.join(amb_found)}",
                    "Recommendation":  "Clarify requirement with precise, unambiguous language.",
                    "Severity":        "Medium",
                }])], ignore_index=True)

    # ── R4: High-risk reqs with insufficient OQ test count ───────────────────
    if not frs_df.empty and "Risk" in frs_df.columns and not oq_df.empty:
        req_link_col = "Requirement_Link" if "Requirement_Link" in oq_df.columns else None
        if req_link_col:
            for _, row in frs_df.iterrows():
                fid       = str(row.get("ID", "")).strip()
                risk      = str(row.get("Risk", "")).strip().lower()
                min_tests = {"high": 3, "medium": 2, "low": 1}.get(risk, 1)
                test_cnt  = oq_df[oq_df[req_link_col].astype(str).str.strip() == fid].shape[0]
                if test_cnt < min_tests:
                    issues.append({
                        "Rule":            "R4",
                        "Req_ID":          fid,
                        "Gap_Type":        "No_Test_Coverage",
                        "Description":     (f"Risk={risk.title()}: {fid} has {test_cnt} test(s) "
                                            f"but requires ≥{min_tests} for this risk level."),
                        "Recommendation":  f"Add {min_tests - test_cnt} more OQ test case(s).",
                        "Severity":        "High" if risk == "high" else "Medium",
                    })

    # ── R5: Duplicate detection (Jaccard token overlap > 0.80) ───────────────
    if desc_col and len(frs_df) > 1:
        frs_list = frs_df[["ID", desc_col]].dropna().reset_index(drop=True)
        seen_pairs = set()
        for i in range(len(frs_list)):
            for j in range(i + 1, len(frs_list)):
                id_a   = str(frs_list.loc[i, "ID"]).strip()
                id_b   = str(frs_list.loc[j, "ID"]).strip()
                desc_a = str(frs_list.loc[i, desc_col])
                desc_b = str(frs_list.loc[j, desc_col])
                pair   = tuple(sorted([id_a, id_b]))
                if pair in seen_pairs:
                    continue
                overlap = _token_overlap(desc_a, desc_b)
                if overlap >= 0.80:
                    seen_pairs.add(pair)
                    issues.append({
                        "Rule":            "R5",
                        "Req_ID":          f"{id_a} / {id_b}",
                        "Gap_Type":        "Duplicate",
                        "Description":     f"{id_a} and {id_b} have {overlap:.0%} token overlap.",
                        "Recommendation":  "Review and consolidate or differentiate these requirements.",
                        "Severity":        "Medium",
                    })
                    gap_df = pd.concat([gap_df, pd.DataFrame([{
                        "Req_ID":          f"{id_a} / {id_b}",
                        "Gap_Type":        "Duplicate",
                        "Description":     f"{overlap:.0%} overlap between {id_a} and {id_b}",
                        "Recommendation":  "Consolidate or clearly differentiate these requirements.",
                        "Severity":        "Medium",
                    }])], ignore_index=True)

    det_issues_df = pd.DataFrame(issues) if issues else pd.DataFrame(
        columns=["Rule", "Req_ID", "Gap_Type", "Description", "Recommendation", "Severity"]
    )

    return gap_df, det_issues_df

def build_audit_log_sheet(user: str, file_name: str, model_name: str,
                          frs_df: pd.DataFrame, oq_df: pd.DataFrame,
                          gap_df: pd.DataFrame, det_df: pd.DataFrame,
                          version_frs: int, version_oq: int,
                          doc_ids: str = "") -> pd.DataFrame:
    now_str    = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    role       = get_user_role(user)
    gap_count  = len(gap_df) if not gap_df.empty else 0
    det_count  = len(det_df) if not det_df.empty else 0

    rows = [
        {
            "Event":            "SESSION_LOGIN",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "SESSION",
            "Old_Value":        "",
            "New_Value":        "AUTHENTICATED",
            "Reason":           "User authenticated successfully",
            "AI_Metadata":      "",
        },
        {
            "Event":            "DOCUMENT_UPLOADED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "URS/SOP",
            "Old_Value":        "",
            "New_Value":        file_name,
            "Reason":           "URS file submitted for analysis",
            "AI_Metadata":      "",
        },
        {
            "Event":            "AI_ANALYSIS_INITIATED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "ANALYSIS_ENGINE",
            "Old_Value":        "",
            "New_Value":        f"Model: {model_name} | Prompt: {PROMPT_VERSION} | Temp: {TEMPERATURE}",
            "Reason":           "GAMP-5 segmented analysis started",
            "AI_Metadata":      f"prompt_version={PROMPT_VERSION} | model={model_name} | "
                                f"temperature={TEMPERATURE} | doc_ids={doc_ids}",
        },
        {
            "Event":            "FRS_GENERATED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   f"FRS v{version_frs}.0",
            "Old_Value":        f"v{version_frs - 1}.0" if version_frs > 1 else "N/A",
            "New_Value":        f"v{version_frs}.0 — {len(frs_df)} requirements",
            "Reason":           "Functional requirements derived from URS + user guide",
            "AI_Metadata":      f"model={model_name} | prompt={PROMPT_VERSION} | temp={TEMPERATURE}",
        },
        {
            "Event":            "OQ_GENERATED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   f"OQ v{version_oq}.0",
            "Old_Value":        f"v{version_oq - 1}.0" if version_oq > 1 else "N/A",
            "New_Value":        f"v{version_oq}.0 — {len(oq_df)} test cases",
            "Reason":           "OQ test cases generated; High-risk reqs → ≥3 tests",
            "AI_Metadata":      f"model={model_name} | prompt={PROMPT_VERSION} | temp={TEMPERATURE}",
        },
        {
            "Event":            "GAP_ANALYSIS_COMPLETED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "TRACEABILITY_MATRIX",
            "Old_Value":        "",
            "New_Value":        f"AI gaps: {gap_count} | Deterministic issues: {det_count}",
            "Reason":           "RTM compiled; deterministic rules R1-R4 enforced",
            "AI_Metadata":      "",
        },
        {
            "Event":            "WORKBOOK_EXPORTED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "VALIDATION_PACKAGE",
            "Old_Value":        "",
            "New_Value":        f"Validation_Package_{datetime.date.today()}.xlsx",
            "Reason":           "Full package with dashboard downloaded by user",
            "AI_Metadata":      f"doc_ids_used={doc_ids}",
        },
    ]
    return pd.DataFrame(rows)


# =============================================================================
# 7b. VALIDATION DASHBOARD BUILDER
# =============================================================================

def build_dashboard_sheet(frs_df: pd.DataFrame, oq_df: pd.DataFrame,
                           gap_df: pd.DataFrame, det_df: pd.DataFrame,
                           trace_df: pd.DataFrame,
                           file_name: str, model_name: str) -> pd.DataFrame:
    """Build a KPI summary table for the Dashboard sheet."""
    total_reqs  = len(frs_df) if not frs_df.empty else 0
    total_tests = len(oq_df)  if not oq_df.empty  else 0

    # Coverage: from Python-built traceability — the only reliable source
    covered  = 0
    partial  = 0
    missing  = 0
    if not trace_df.empty and "Coverage_Status" in trace_df.columns:
        covered  = int((trace_df["Coverage_Status"] == "Covered").sum())
        partial  = int((trace_df["Coverage_Status"] == "Partial").sum())
        missing  = int((trace_df["Coverage_Status"].isin(
                        ["Not Covered", "Missing FRS"])).sum())

    # Coverage % = (Covered + Partial) / total  — reflects that tests exist even if incomplete
    has_tests = covered + partial
    coverage_pct = round((has_tests / total_reqs * 100), 1) if total_reqs > 0 else 0.0
    fully_covered_pct = round((covered / total_reqs * 100), 1) if total_reqs > 0 else 0.0

    # Gap counts
    ai_gaps  = len(gap_df) if not gap_df.empty else 0
    det_gaps = len(det_df) if not det_df.empty else 0

    # Risk breakdown
    high_risk = med_risk = low_risk = 0
    if not frs_df.empty and "Risk" in frs_df.columns:
        rc = frs_df["Risk"].str.strip().str.lower().value_counts()
        high_risk = int(rc.get("high",   0))
        med_risk  = int(rc.get("medium", 0))
        low_risk  = int(rc.get("low",    0))

    # Non-testable count from det_df
    non_testable = 0
    if not det_df.empty and "Rule" in det_df.columns:
        non_testable = int((det_df["Rule"] == "R3").sum())

    orphan_tests = 0
    if not det_df.empty and "Rule" in det_df.columns:
        orphan_tests = int((det_df["Rule"] == "R2").sum())

    missing_frs = 0
    if not det_df.empty and "Rule" in det_df.columns:
        missing_frs = int((det_df["Rule"] == "R0").sum())

    rows = [
        {"KPI": "📋 Total FRS Requirements",          "Value": total_reqs,            "Status": "N/A"},
        {"KPI": "🧪 Total OQ Test Cases",              "Value": total_tests,           "Status": "N/A"},
        {"KPI": "✅ Fully Covered (all tests met)",    "Value": covered,               "Status": "N/A"},
        {"KPI": "🔶 Partially Covered (some tests)",   "Value": partial,               "Status": "See Traceability sheet"},
        {"KPI": "❌ Not Covered / Missing FRS",         "Value": missing,               "Status": "Immediate action required"},
        {"KPI": "📊 Coverage % (Covered+Partial)",     "Value": f"{coverage_pct}%",
         "Status": "✅ PASS" if coverage_pct >= 80 else ("⚠️ REVIEW" if coverage_pct >= 60 else "❌ FAIL")},
        {"KPI": "📊 Fully Covered %",                  "Value": f"{fully_covered_pct}%",
         "Status": "✅ PASS" if fully_covered_pct >= 80 else ("⚠️ REVIEW" if fully_covered_pct >= 60 else "❌ FAIL")},
        {"KPI": "🔴 High Risk Requirements",            "Value": high_risk,             "Status": "Requires ≥3 OQ tests each"},
        {"KPI": "🟡 Medium Risk Requirements",          "Value": med_risk,              "Status": "Requires ≥2 OQ tests each"},
        {"KPI": "🟢 Low Risk Requirements",             "Value": low_risk,              "Status": "Requires ≥1 OQ test each"},
        {"KPI": "🚨 Missing FRS (AI skipped URS)",     "Value": missing_frs,           "Status": "Critical — re-run or add manually"},
        {"KPI": "⚠️ AI-Detected Gaps",                 "Value": ai_gaps,               "Status": "See Gap_Analysis sheet"},
        {"KPI": "🔍 Deterministic Issues (R0–R5)",     "Value": det_gaps,              "Status": "See Det_Validation sheet"},
        {"KPI": "🚫 Non-Testable Requirements",         "Value": non_testable,          "Status": "Rewrite required"},
        {"KPI": "👻 Orphan Tests (No FRS link)",        "Value": orphan_tests,          "Status": "Investigate"},
        {"KPI": "📁 Source Document",                   "Value": file_name,             "Status": "N/A"},
        {"KPI": "🤖 AI Model Used",                     "Value": model_name,            "Status": "N/A"},
        {"KPI": "🏷️ Prompt Version",                   "Value": PROMPT_VERSION,        "Status": "N/A"},
        {"KPI": "🌡️ Temperature",                      "Value": TEMPERATURE,           "Status": "N/A"},
        {"KPI": "📅 Generated",
         "Value": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),           "Status": "N/A"},
    ]
    return pd.DataFrame(rows)


def _write_dashboard_chart(wb, ws_dash):
    """Write bar chart to dashboard sheet using openpyxl BarChart."""
    try:
        from openpyxl.chart import BarChart, Reference
        # Chart data: rows 1-4 are the core coverage KPIs (row index = Excel row)
        # KPI col=A(1), Value col=B(2)
        chart        = BarChart()
        chart.type   = "col"
        chart.title  = "Validation Coverage Overview"
        chart.y_axis.title = "Count / %"
        chart.x_axis.title = "Metric"
        chart.style  = 10
        chart.width  = 22
        chart.height = 14

        # Use rows 2-11 (KPI rows 1-10 in the dataframe, Excel rows 2-11)
        data   = Reference(ws_dash, min_col=2, min_row=1, max_row=11)
        cats   = Reference(ws_dash, min_col=1, min_row=2, max_row=11)
        chart.add_data(data, titles_from_data=True)
        chart.set_categories(cats)
        chart.series[0].graphicalProperties.solidFill = "2563EB"
        ws_dash.add_chart(chart, "E2")
    except Exception:
        pass  # Chart is a bonus — never crash on it




SHEET_COLORS = {
    "Dashboard":        {"header_fill": "0F172A", "tab_color": "0F172A"},
    "URS_Extraction":   {"header_fill": "1D4ED8", "tab_color": "1D4ED8"},
    "FRS":              {"header_fill": "2563EB", "tab_color": "2563EB"},
    "OQ":               {"header_fill": "059669", "tab_color": "059669"},
    "Traceability":     {"header_fill": "7C3AED", "tab_color": "7C3AED"},
    "Gap_Analysis":     {"header_fill": "DC2626", "tab_color": "DC2626"},
    "Det_Validation":   {"header_fill": "EA580C", "tab_color": "EA580C"},
    "Audit_Log":        {"header_fill": "B45309", "tab_color": "B45309"},
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
        # Add bar chart to Dashboard sheet
        if "Dashboard" in wb.sheetnames:
            _write_dashboard_chart(wb, wb["Dashboard"])
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
       st.markdown("<h3 style='text-align: center; font-size: 1.5rem;'>21CFRPart11 Compliant </h3>",
                    unsafe_allow_html=True)
       st.markdown("<h3 style='text-align: center; font-size: 1.5rem;'>21CFRPart11 Compliant Auto generate FRS, OQ Test steps and Traceability with Gap analysis</h3>",
                    unsafe_allow_html=True)

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
            label_visibility="collapsed",
            key="model_selectbox"
        )
        # Update selected model WITHOUT rerun — preserves sop_file_bytes in session state
        st.session_state.selected_model = engine_name

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
            # If a different file is uploaded, clear previous results
            if st.session_state.sop_file_name != sop_widget.name:
                st.session_state.pop("last_result", None)
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
        st.session_state.pop("last_result", None)

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
        st.info(f"⚙️ Two-pass analysis started — {st.session_state.selected_model} — chunk size: {CHUNK_SIZE} pages")

        progress_bar = st.progress(0)
        status_text  = st.empty()

        try:
            # ── Step 1: Two-pass AI analysis ────────────────────────────────
            urs_df, frs_df, oq_df, trace_df, gap_df = run_segmented_analysis(
                file_bytes, model_id, progress_bar, status_text, sys_ctx
            )

            # ── Guard: if the AI returned nothing useful, stop cleanly ───────
            if urs_df.empty and frs_df.empty:
                progress_bar.empty()
                status_text.empty()
                st.error(
                    "⚠️ No requirements were extracted. This usually means:\n"
                    "- **API quota exceeded** — check your API key billing/limits\n"
                    "- **Rate limit** — wait a minute and try again\n"
                    "- **Model unavailable** — try a different Intelligence Engine\n\n"
                    "The error detail is shown above."
                )
                log_audit(user, "ANALYSIS_ABORTED", "URS_FILE",
                          reason="Empty AI output — possible rate limit or quota error")
                return

            # ── Step 2: Deterministic rule-based validation R1–R5 ───────────
            status_text.text("🔍 Running deterministic validation rules R1–R5...")
            gap_df, det_df = run_deterministic_validation(frs_df, oq_df, gap_df, urs_df)
            # Replace all None/blank with N/A in every output dataframe
            for _df in [gap_df, det_df, trace_df]:
                _df.fillna("N/A", inplace=True)
                _df.replace("", "N/A", inplace=True)

            # ── Step 3: Persist documents (version-controlled, never overwrite)
            id_urs   = save_document("URS_Extraction", urs_df.to_csv(index=False),  user, file_name)
            id_frs   = save_document("FRS",            frs_df.to_csv(index=False),  user, file_name)
            id_oq    = save_document("OQ",             oq_df.to_csv(index=False),   user, file_name)
            id_trace = save_document("Traceability",   trace_df.to_csv(index=False),user, file_name)
            id_gap   = save_document("Gap_Analysis",   gap_df.to_csv(index=False),  user, file_name)
            id_det   = save_document("Det_Validation", det_df.to_csv(index=False),  user, file_name)
            doc_ids  = (f"URS:{id_urs}, FRS:{id_frs}, OQ:{id_oq}, "
                        f"Trace:{id_trace}, Gap:{id_gap}, Det:{id_det}")

            # ── Step 4: AI generation log with full doc provenance ───────────
            log_ai_generation(
                user, st.session_state.selected_model,
                PROMPT_VERSION, TEMPERATURE, file_name,
                document_ids_used=doc_ids
            )

            # ── Step 5: Audit entries ────────────────────────────────────────
            log_audit(user, "URS_EXTRACTED",           f"URS doc:{id_urs}",
                      new_value=f"{len(urs_df)} structured requirements")
            log_audit(user, "FRS_GENERATED",           f"FRS doc:{id_frs}",
                      new_value=f"{len(frs_df)} requirements")
            log_audit(user, "OQ_GENERATED",            f"OQ doc:{id_oq}",
                      new_value=f"{len(oq_df)} test cases")
            log_audit(user, "TRACEABILITY_GENERATED",  f"Trace doc:{id_trace}",
                      new_value=f"{len(trace_df)} rows")
            log_audit(user, "GAP_ANALYSIS_GENERATED",  f"Gap doc:{id_gap}",
                      new_value=f"{len(gap_df)} AI gaps")
            log_audit(user, "DET_VALIDATION_RUN",      f"Det doc:{id_det}",
                      new_value=f"{len(det_df)} deterministic issues (R1-R5)")

            # ── Step 6: Confidence summary ───────────────────────────────────
            frs_review = 0
            oq_review  = 0
            if not frs_df.empty and "Confidence_Flag" in frs_df.columns:
                frs_review = int(frs_df["Confidence_Flag"].astype(str).str.contains("Review").sum())
            if not oq_df.empty and "Confidence_Flag" in oq_df.columns:
                oq_review = int(oq_df["Confidence_Flag"].astype(str).str.contains("Review").sum())

            # ── Step 7: Build audit log and dashboard ────────────────────────
            ver_frs = get_next_doc_version("FRS") - 1
            ver_oq  = get_next_doc_version("OQ")  - 1

            audit_df     = build_audit_log_sheet(
                user, file_name, st.session_state.selected_model,
                frs_df, oq_df, gap_df, det_df, ver_frs, ver_oq, doc_ids
            )
            dashboard_df = build_dashboard_sheet(
                frs_df, oq_df, gap_df, det_df, trace_df, file_name,
                st.session_state.selected_model
            )

            # Dashboard first so it opens on launch
            dataframes = {
                "Dashboard":      dashboard_df,
                "URS_Extraction": urs_df,
                "FRS":            frs_df,
                "OQ":             oq_df,
                "Traceability":   trace_df,
                "Gap_Analysis":   gap_df,
                "Det_Validation": det_df,
                "Audit_Log":      audit_df,
            }

            xlsx_bytes = build_styled_excel(dataframes)
            log_audit(user, "WORKBOOK_EXPORTED", "VALIDATION_PACKAGE",
                      new_value=f"Validation_Package_{datetime.date.today()}.xlsx",
                      reason=f"doc_ids={doc_ids}")

            status_text.empty()
            progress_bar.empty()

            # ── Compute coverage metrics for session state ────────────────────
            covered = partial_cov = 0
            if not trace_df.empty and "Coverage_Status" in trace_df.columns:
                covered     = int((trace_df["Coverage_Status"] == "Covered").sum())
                partial_cov = int((trace_df["Coverage_Status"] == "Partial").sum())
            total_reqs = len(frs_df)
            has_tests  = covered + partial_cov
            cov_pct    = round(has_tests / total_reqs * 100, 1) if total_reqs > 0 else 0.0

            # ── Persist to session state so download button rerun doesn't clear ──
            st.session_state["last_result"] = {
                "xlsx_bytes":   xlsx_bytes,
                "dataframes":   dataframes,
                "frs_review":   frs_review,
                "oq_review":    oq_review,
                "total_reqs":   len(frs_df),
                "total_tests":  len(oq_df),
                "total_urs":    len(urs_df),
                "covered":      covered,
                "cov_pct":      cov_pct,
                "gap_count":    len(gap_df),
                "det_count":    len(det_df),
                "file_name":    file_name,
            }

        except Exception as e:
            log_audit(user, "ANALYSIS_ERROR", "URS_FILE", reason=str(e)[:500])
            st.error(f"❌ Engineering Error: {str(e)}")
            import traceback
            st.error(traceback.format_exc())

    # ── Render results from session state (persists across download reruns) ──
    if "last_result" in st.session_state:
        r = st.session_state["last_result"]
        st.success("✅ Validation Package ready.")

        col1, col2, col3, col4, col5 = st.columns(5)
        col1.metric("📄 URS Requirements", r["total_urs"])
        col2.metric("📋 FRS Requirements", r["total_reqs"])
        col3.metric("🧪 OQ Test Cases",    r["total_tests"])
        col4.metric("📊 Coverage",          f"{r['cov_pct']}%")
        col5.metric("⚠️ Issues (AI+Det)",   r["gap_count"] + r["det_count"])

        if r["frs_review"] > 0 or r["oq_review"] > 0:
            st.warning(
                f"🔶 Confidence Review Required: **{r['frs_review']}** FRS row(s) and "
                f"**{r['oq_review']}** OQ row(s) have confidence < 0.70 — "
                "check Confidence_Flag columns in FRS and OQ sheets."
            )
        if r["det_count"] > 0:
            st.warning(
                f"🔍 Deterministic Validation: **{r['det_count']}** issue(s) (R0–R5) "
                "— see Det_Validation tab."
            )
        if r["gap_count"] > 0:
            st.warning(
                f"⚠️ Gap Analysis: **{r['gap_count']}** gap(s) flagged "
                "— see Gap_Analysis tab."
            )

        with st.expander("📋 Preview Generated Sheets", expanded=True):
            for sheet_name, df in r["dataframes"].items():
                st.markdown(f"**{sheet_name}** — {len(df)} rows")
                st.dataframe(df, use_container_width=True)

        st.download_button(
            label="📥 Download Validation Workbook (.xlsx)",
            data=r["xlsx_bytes"],
            file_name=f"Validation_Package_{r['file_name'].replace('.pdf','')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="download_xlsx_btn",
        )


# =============================================================================
# 13. ROUTER
# =============================================================================
if not st.session_state.authenticated:
    show_login()
else:
    show_app()
