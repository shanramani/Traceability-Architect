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
import streamlit.components.v1 as _st_components
import os
import datetime
import pandas as pd
from litellm import completion
import tempfile
import io
import sqlite3
import re
import hashlib
import secrets
import html as _html_lib

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
VERSION        = "37.0"
PROMPT_VERSION = "v19.0-esignature-test-type-r3c"
TEMPERATURE    = 0.2
CHUNK_SIZE     = 8
DB_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation_app.db")

SESSION_TIMEOUT_MINUTES = 15   # 21 CFR Part 11 — 15-minute inactivity timeout
MAX_FAILED_ATTEMPTS     = 5
LOCKOUT_MINUTES         = 15
MAX_UPLOAD_BYTES        = 10 * 1024 * 1024   # 10 MB hard limit per uploaded file

ROLES = ["Admin", "QA", "Validator"]

# =============================================================================
# 1b. PROMPT LOADER
# =============================================================================
# All prompts live in ./prompts/*.md — loaded once at startup.
# Separating prompts from code lets domain experts edit clinical/regulatory
# language without touching Python, and gives prompt changes their own git history.

def _load_prompt(filename: str) -> str:
    """Load a prompt template from the prompts/ directory next to this file."""
    prompt_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts")
    path       = os.path.join(prompt_dir, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        # Graceful fallback — log warning but don't crash the app
        import warnings
        warnings.warn(f"Prompt file not found: {path}. Using empty string fallback.")
        return ""

# Load all prompt templates at module level — single I/O hit at startup
_PROMPT_SYSTEM_RAW          = _load_prompt("system_prompt.md")
_PROMPT_PREFLIGHT_RAW       = _load_prompt("preflight_classifier.md")
_PROMPT_PASS1_RAW           = _load_prompt("pass1_urs_extraction.md")
_PROMPT_PASS2_RAW           = _load_prompt("pass2_frs_oq_gap.md")
_PROMPT_CIA_PASS1_RAW       = _load_prompt("cia_pass1_change_extraction.md")
_PROMPT_CIA_PASS2_RAW       = _load_prompt("cia_pass2_impact_mapping.md")

# =============================================================================
# 1b. SECURITY HELPERS
# =============================================================================

# In-memory rate-limiter (keyed by username, clears on server restart intentionally)
_LOGIN_RATE: dict = {}
_RATE_WINDOW_SEC   = 60
_RATE_MAX_ATTEMPTS = 15   # per 60-second window (above DB-level account lockout)


def _rate_allowed(key: str) -> bool:
    """True = request is within rate limit. False = reject immediately."""
    now      = datetime.datetime.utcnow().timestamp()
    attempts = [t for t in _LOGIN_RATE.get(key, []) if now - t < _RATE_WINDOW_SEC]
    _LOGIN_RATE[key] = attempts
    return len(attempts) < _RATE_MAX_ATTEMPTS


def _rate_record(key: str):
    _LOGIN_RATE.setdefault(key, []).append(datetime.datetime.utcnow().timestamp())


def sanitize_input(value: str, max_length: int = 128) -> str:
    """
    Strip null bytes, ASCII control characters, and HTML tags.
    Limits string length to max_length. Safe for use before any DB write.
    """
    if not isinstance(value, str):
        value = str(value)
    # Remove null bytes and non-printable control characters (keep tab/newline for text areas)
    value = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)
    # Remove HTML tags
    value = re.sub(r'<[^>]+>', '', value)
    return value.strip()[:max_length]


def _inject_password_security():
    """
    Inject JavaScript (via iframe parent access) that sets autocomplete='new-password'
    on every password input in the Streamlit page. This prevents browsers and password
    managers from offering to save or auto-fill credentials.

    Also sets data-lpignore (LastPass) and data-1p-ignore (1Password) opt-out attributes.
    A MutationObserver re-applies the attributes after every Streamlit rerender.
    """
    _st_components.html("""
    <script>
    (function() {
      var ATTRS = {
        'autocomplete': 'new-password',
        'data-form-type': 'other',
        'data-lpignore': 'true',
        'data-1p-ignore': 'true',
        'aria-autocomplete': 'none'
      };
      function patch() {
        try {
          var inputs = window.parent.document.querySelectorAll('input[type="password"]');
          inputs.forEach(function(el) {
            for (var k in ATTRS) { el.setAttribute(k, ATTRS[k]); }
            // Prevent browser from reading back cached value
            if (el._pwPatched) return;
            el._pwPatched = true;
            el.addEventListener('focus', function() {
              this.removeAttribute('value');
            });
          });
        } catch(e) {}
      }
      patch();
      try {
        new MutationObserver(patch).observe(
          window.parent.document.body, {childList: true, subtree: true}
        );
      } catch(e) {}
    })();
    </script>
    """, height=0, scrolling=False)

st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

# =============================================================================
# 2. DATABASE
# =============================================================================

def db_connect():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    # Deny any attempt to write to the audit_log table via DDL at runtime
    # (defence-in-depth — the Python layer never calls DELETE/UPDATE on audit_log)
    return conn

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
                reason         TEXT,
                location       TEXT,
                user_ip        TEXT
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

        # ── Electronic Signature log — 21 CFR Part 11 §11.50 / §11.200 ────────
        # INSERT-ONLY. Append-only triggers added below alongside audit_log.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS signature_log (
                signature_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user              TEXT    NOT NULL,
                role              TEXT,
                timestamp         TEXT    NOT NULL,
                action            TEXT    NOT NULL,
                signature_meaning TEXT    NOT NULL,
                document_hash     TEXT    NOT NULL,
                document_name     TEXT,
                model_used        TEXT,
                prompt_version    TEXT,
                ip_address        TEXT,
                doc_ids           TEXT
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
            ("location",       "TEXT"),
            ("user_ip",        "TEXT"),   # v29 — separate column for faster audit search
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

        # ── Append-only enforcement: block UPDATE + DELETE on audit_log ──────
        # These triggers make the audit trail structurally immutable at the
        # database level — a defence-in-depth layer on top of the Python
        # "never call UPDATE/DELETE" convention.
        conn2 = db_connect()
        conn2.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_audit_no_update
            BEFORE UPDATE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, '21CFR11: audit_log rows are immutable — UPDATE denied');
            END
        """)
        conn2.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_audit_no_delete
            BEFORE DELETE ON audit_log
            BEGIN
                SELECT RAISE(ABORT, '21CFR11: audit_log rows are immutable — DELETE denied');
            END
        """)
        # signature_log is also append-only — e-signatures cannot be retracted
        conn2.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_esig_no_update
            BEFORE UPDATE ON signature_log
            BEGIN
                SELECT RAISE(ABORT, '21CFR11: signature_log rows are immutable — UPDATE denied');
            END
        """)
        conn2.execute("""
            CREATE TRIGGER IF NOT EXISTS trg_esig_no_delete
            BEFORE DELETE ON signature_log
            BEGIN
                SELECT RAISE(ABORT, '21CFR11: signature_log rows are immutable — DELETE denied');
            END
        """)
        conn2.commit()
        conn2.close()

    except Exception as e:
        st.warning(f"DB migration warning: {e}")


def db_diagnostics() -> dict:
    try:
        conn   = db_connect()
        result = {t: conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["users", "audit_log", "documents", "ai_gen_log", "signature_log"]}
        conn.close()
        return result
    except Exception as e:
        return {"error": str(e)}


def log_audit(user: str, action: str, object_changed: str = "",
              old_value: str = "", new_value: str = "", reason: str = ""):
    """
    Append-only audit write. This function must never UPDATE or DELETE rows.
    user_ip is pulled from st.session_state and written as a separate indexed
    column (not embedded in Reason) to support fast audit searches per 21 CFR Part 11.
    """
    try:
        user_ip = st.session_state.get("user_ip", "")
    except Exception:
        user_ip = ""
    try:
        conn = db_connect()
        conn.execute(
            """INSERT INTO audit_log
               (user, timestamp, action, object_changed, old_value, new_value,
                reason, location, user_ip)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (user,
             datetime.datetime.utcnow().isoformat(),
             action,
             str(object_changed)[:500],
             str(old_value)[:2000]  if old_value  else "",
             str(new_value)[:2000]  if new_value  else "",
             str(reason)[:1000]     if reason     else "",
             "",          # location intentionally blank — removed from UI in v27
             str(user_ip)[:100])
        )
        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"Audit log write failed: {e}")


# =============================================================================
# 21 CFR PART 11 — ELECTRONIC SIGNATURE CONSTANTS & WRITER
# =============================================================================
# Controlled vocabulary for signature meaning per §11.50(a)(1).
# Free text is NOT allowed — auditors expect a finite, pre-approved list.
# ── MANUAL EDIT v29-custom — DO NOT OVERWRITE ──────────────
ESIG_MEANINGS = [
    "I executed this validation package",
    "I reviewed this validation package",
    "I approved this validation package",
    ]
ESIG_DEFAULT_MEANING = ESIG_MEANINGS[0]
# ── END MANUAL EDIT ────────────────────────────────────────

def log_esignature(user: str, role: str, action: str, meaning: str,
                   document_hash: str, document_name: str = "",
                   model_used: str = "", prompt_ver: str = "",
                   ip_address: str = "", doc_ids: str = "") -> int:
    """
    Insert one row into signature_log (append-only).

    21 CFR Part 11 compliance:
      §11.50(a)(1) — printed name, date/time, meaning recorded
      §11.50(a)(2) — linked to associated record via document_hash
      §11.100(a)   — unique to one individual (username)
      §11.200(b)   — two distinct components: username + password re-entry
                     (verification happens in calling code before this runs)

    document_hash covers all workbook sheets EXCEPT the Signature sheet.
    This is standard practice (same as PDF digital signatures) and is
    documented explicitly on the Signature sheet.

    Returns the new signature_id (or -1 on failure).
    """
    try:
        conn = db_connect()
        cur  = conn.execute(
            """INSERT INTO signature_log
               (user, role, timestamp, action, signature_meaning,
                document_hash, document_name, model_used, prompt_version,
                ip_address, doc_ids)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (user, role,
             datetime.datetime.utcnow().isoformat(),
             action, meaning, document_hash, document_name,
             model_used, prompt_ver, ip_address, doc_ids)
        )
        sig_id = cur.lastrowid
        conn.commit()
        conn.close()
        return sig_id
    except Exception as e:
        st.warning(f"E-signature log write failed: {e}")
        return -1


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
    """bcrypt only — no plaintext fallback. Auditor-safe."""
    if not BCRYPT_AVAILABLE:
        raise RuntimeError(
            "bcrypt is required but not installed. "
            "Run: pip install bcrypt  — SHA-256 plaintext fallback is disabled for GxP compliance."
        )
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(plain: str, stored_hash: str) -> bool:
    """bcrypt only — rejects any non-bcrypt hash rather than falling back to SHA-256."""
    if not BCRYPT_AVAILABLE:
        return False   # cannot verify without bcrypt — fail closed
    try:
        if not stored_hash.startswith("$2"):
            # Hash is not a bcrypt hash — refuse to compare
            return False
        return bcrypt.checkpw(plain.encode("utf-8"), stored_hash.encode("utf-8"))
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
    # Sanitize inputs before any processing
    username = sanitize_input(username, max_length=64)
    password = sanitize_input(password, max_length=256)

    if not username:
        return False, "Username is required."

    # In-memory rate limit (complements DB-level account lockout)
    if not _rate_allowed(username):
        log_audit(username, "LOGIN_RATE_LIMITED", "SESSION",
                  reason="Exceeded in-memory rate limit")
        return False, "Too many login attempts. Please wait 60 seconds."
    _rate_record(username)

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

# PDF magic bytes — all valid PDF files start with %PDF
_PDF_MAGIC = b'%PDF'


def validate_upload(file_obj) -> tuple:
    """
    Two-gate upload security check.

    Gate 1 — File size: reject anything over MAX_UPLOAD_BYTES (10 MB).
      Prevents memory exhaustion and DoS via oversized uploads.

    Gate 2 — MIME / magic bytes: read the first 4 bytes and confirm the
      file starts with %PDF regardless of the .pdf extension chosen.
      Extension-only checks are trivially bypassed by renaming a file.

    Returns (is_valid: bool, error_message: str).
    """
    if file_obj is None:
        return False, "No file provided."

    # Gate 1 — size
    size = file_obj.size
    if size > MAX_UPLOAD_BYTES:
        mb = size / (1024 * 1024)
        return False, (
            f"⛔ File rejected: {mb:.1f} MB exceeds the 10 MB limit. "
            "Split the document or remove embedded images before uploading."
        )

    # Gate 2 — MIME / magic bytes
    raw = file_obj.getvalue()
    if not raw or raw[:4] != _PDF_MAGIC:
        return False, (
            "⛔ File rejected: content does not match PDF format. "
            "Renaming a non-PDF file to .pdf does not make it a valid PDF. "
            "Upload a genuine PDF document."
        )

    return True, ""


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
# 4b. URS DOCUMENT VALIDATION — Two-stage gate
#     Stage 1: Fast heuristic (free, instant) — rejects obvious non-URS docs
#     Stage 2: LLM pre-flight (one cheap call) — catches edge cases
# =============================================================================

# Positive signals — language expected in a URS / SOP
_URS_POSITIVE = [
    r'\bshall\b', r'\bmust\b', r'\brequirement[s]?\b', r'\buser requirement\b',
    r'\bsystem shall\b', r'\bthe system\b', r'\burs\b', r'\bsop\b',
    r'\bfunctional requirement\b', r'\buse case\b', r'\bstakeholder\b',
    r'\bscope\b', r'\bpurpose\b', r'\bspecification\b',
    r'\bvalidation\b', r'\bcompliance\b', r'\baudit trail\b',
    r'\breq[-_\s]?\d+\b', r'\burs[-_\s]?\d+\b',   # REQ-001, URS-001 style IDs
    r'\d+\.\d+[\s]+\w',                              # 1.1 Section style numbering
]

# Negative signals — language that identifies clearly wrong document types
_URS_NEGATIVE = [
    r'\bdate of birth\b', r'\blicense number\b', r'\bdriver.?s license\b',
    r'\bpassport\b', r'\bstate id\b', r'\bidentification card\b',
    r'\bexpir(?:es|ation)\b.*\b\d{4}\b',   # "Expires 2027" — ID card pattern
    r'\bsocial security\b', r'\bssn\b',
    r'\binvoice\b', r'\bpurchase order\b', r'\breceipt\b',
    r'\btotal due\b', r'\bamount due\b', r'\bremit payment\b',
    r'\bdear\b.*\bsincerely\b',            # letter pattern
    r'\bresume\b', r'\bcurriculum vitae\b',
    r'\bmenu\b.*\bprice\b',               # restaurant menu
]

# LLM pre-flight prompt — binary YES/NO, one sentence reasoning
_PREFLIGHT_PROMPT = _PROMPT_PREFLIGHT_RAW


def validate_urs_document(
    file_bytes: bytes,
    model_id: str,
) -> tuple[bool, str]:
    """
    Three-stage URS document gate. Returns (is_valid: bool, message: str).

    Stage 0 — Structural pre-check (free, <5ms):
        Minimum extractable text length and page count. Rejects empty or
        image-only PDFs before any pattern matching.

    Stage 1 — Heuristic scoring (free, <10ms):
        Score the first ~3000 chars against positive/negative patterns.
        Also checks for minimum "shall"/"must" density and minimum
        extractable requirement count.

    Stage 2 — LLM pre-flight (one cheap API call, only if Stage 1 passes):
        Send the first page to the model with a binary YES/NO prompt.

    New checks added in v26:
      - Minimum text length gate (< 300 chars = not a document)
      - Minimum "shall"/"must" count (< 2 = insufficient requirement density)
      - Minimum positive signal score raised to 3
      - Structural section check: warns if no section headings found
    """
    # ── Extract text ─────────────────────────────────────────────────────────
    try:
        pages = extract_pages(file_bytes)
    except Exception as e:
        return False, f"Could not extract text from PDF: {e}"

    if not pages:
        return False, "The uploaded PDF appears to be empty or image-only (no extractable text)."

    # ── Stage 0: Structural pre-check ─────────────────────────────────────────
    full_text    = "\n".join(pages)
    full_lower   = full_text.lower()
    sample_text  = "\n\n".join(pages[:2])
    sample_lower = sample_text.lower()

    if len(full_text.strip()) < 300:
        return False, (
            "⛔ Document too short to be a valid URS.\n\n"
            f"Only {len(full_text.strip())} characters of text were extracted. "
            "A User Requirements Specification must contain substantive requirement "
            "statements. Check that the PDF is not an image-only scan."
        )

    # Minimum requirement-statement density
    shall_must_count = len(re.findall(r'\b(shall|must)\b', full_lower, re.IGNORECASE))
    if shall_must_count < 2:
        return False, (
            f"⛔ Insufficient requirement language detected.\n\n"
            f"Found only {shall_must_count} statement(s) using 'shall' or 'must'. "
            f"A valid URS must contain at least 2 requirement statements written in "
            f"prescriptive language (shall/must). This document does not appear to be "
            f"a User Requirements Specification."
        )

    # ── Stage 1: Heuristic scoring ────────────────────────────────────────────
    pos_hits = [p for p in _URS_POSITIVE if re.search(p, sample_lower, re.IGNORECASE)]
    neg_hits = [p for p in _URS_NEGATIVE if re.search(p, sample_lower, re.IGNORECASE)]

    score = len(pos_hits) - (3 * len(neg_hits))

    if neg_hits:
        matched = [p.replace(r'\b', '').replace('\\', '') for p in neg_hits[:3]]
        return False, (
            f"⛔ Document rejected at content screening.\n\n"
            f"This does not appear to be a URS, SRS, or SOP. "
            f"Detected non-URS content: **{', '.join(matched)}**.\n\n"
            f"Please upload a User Requirements Specification, System Requirements "
            f"Specification, or Standard Operating Procedure."
        )

    if score < 3:
        # Raised threshold from 2 → 3 for stricter gate
        return False, (
            f"⛔ Document rejected: insufficient URS content detected.\n\n"
            f"Only {len(pos_hits)} URS indicator(s) found in the document "
            f"(minimum 3 required). The document may not be a URS, SRS, or SOP.\n\n"
            f"Expected content: requirement statements using 'shall'/'must', "
            f"numbered requirements (URS-001, REQ-001), section headings like "
            f"'Scope', 'Purpose', 'Functional Requirements'."
        )

    # ── Structural section check (soft warning — does not block) ─────────────
    section_patterns = [
        r'\bscope\b', r'\bpurpose\b', r'\bintroduction\b', r'\boverview\b',
        r'\bfunctional requirement', r'\bnon-functional', r'\bsecurity requirement',
        r'\bperformance requirement', r'\bsystem requirement', r'\buser requirement',
        r'\bversion history\b', r'\bdocument control\b', r'\bapproval\b',
    ]
    section_hits = sum(1 for p in section_patterns
                       if re.search(p, sample_lower, re.IGNORECASE))
    structural_warning = "" if section_hits >= 2 else (
        " Note: fewer than 2 standard URS section headings detected — "
        "document may be informal but has been accepted on requirement density."
    )

    # ── Stage 2: LLM pre-flight ───────────────────────────────────────────────
    try:
        preflight_text = sample_text[:3000]
        response = completion(
            model=model_id,
            stream=False,
            temperature=0.0,
            max_tokens=100,
            messages=[
                {"role": "user", "content": _PREFLIGHT_PROMPT.format(text=preflight_text)}
            ]
        )
        reply = (response.choices[0].message.content or "").strip()

        verdict_match = re.search(r'VERDICT:\s*(YES|NO)', reply, re.IGNORECASE)
        reason_match  = re.search(r'REASON:\s*(.+)',      reply, re.IGNORECASE)

        verdict = verdict_match.group(1).upper() if verdict_match else None
        reason  = reason_match.group(1).strip()  if reason_match  else reply[:200]

        if verdict == "YES":
            return True, (
                f"Document validated ({len(pos_hits)} URS indicators, "
                f"{shall_must_count} shall/must statements). "
                f"LLM: {reason}{structural_warning}"
            )
        elif verdict == "NO":
            return False, (
                f"⛔ Document rejected by AI content classifier.\n\n"
                f"**AI assessment:** {reason}\n\n"
                f"Please upload a User Requirements Specification, System Requirements "
                f"Specification, or Standard Operating Procedure."
            )
        else:
            return True, (
                f"Document accepted (LLM response ambiguous; Stage 1 score={score}). "
                f"{structural_warning}"
            )

    except Exception as e:
        if score >= 5:
            return True, (
                f"LLM pre-flight failed ({e}); accepted on strong Stage 1 signal "
                f"(score={score}, {shall_must_count} shall/must statements). "
                f"{structural_warning}"
            )
        return False, (
            f"⛔ Document validation incomplete — LLM pre-flight failed: {e}\n\n"
            f"Stage 1 score was {score} (threshold: 5 for auto-accept without LLM). "
            f"Please try again or use a different model."
        )



#    Pass 1: Structured URS extraction (deterministic input table)
#    Pass 2: FRS / OQ / Traceability / Gap derived from Pass-1 table
#            FRS descriptions are ALWAYS written in engineering/implementation
#            language — never a copy of the URS wording.
# =============================================================================

SYSTEM_PROMPT = _PROMPT_SYSTEM_RAW.strip()


def _make_system_prompt(sys_context: str = "") -> str:
    """
    Build the system prompt for a completion call.

    When a SysContext (product user guide / system manual) has been uploaded,
    it is appended as Reference Material so the LLM can use real screen names,
    field names, and module terminology in BOTH Pass 1 URS extraction AND Pass 2
    FRS/OQ generation — completing the URS → FRS chain the Instructions require.

    The sys_context is capped at 4000 chars to stay within token budgets while
    still providing enough product vocabulary to produce credible engineering FRS.
    """
    if not sys_context:
        return SYSTEM_PROMPT
    return (
        SYSTEM_PROMPT
        + "\n\nREFERENCE MATERIAL — TARGET SYSTEM USER GUIDE:\n"
          "Use the following product documentation to inform ALL outputs with accurate "
          "screen names, field names, module names, and workflow terminology. "
          "Do NOT copy this text verbatim; use it to ground your engineering descriptions.\n\n"
        + sys_context[:4000]
    )

# ── PASS 1 PROMPT: extract a clean, structured URS table ─────────────────────
def build_pass1_prompt(chunk_text: str, chunk_index: int, total_chunks: int) -> str:
    return _PROMPT_PASS1_RAW.format(
        chunk_index  = chunk_index + 1,
        total_chunks = total_chunks,
        chunk_text   = chunk_text,
    )

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
            "FRS descriptions must reference the actual product terminology from that guide. "
            "For OQ test steps: apply RULE A for infrastructure/non-functional requirements "
            "(availability, uptime, SLA, failover) — write technical verification procedures, "
            "NOT UI navigation. Apply RULE B for application features described in the guide "
            "using exact screen and field names. Apply RULE C (prefix [SCREEN UNVERIFIED], "
            "Confidence=0.60) for application features NOT described in the guide."
        )
    else:
        context_block = ""
        system_guidance = (
            "NO system user guide was provided. "
            "Infer the system type from the URS content (e.g. LIMS, SAP, Veeva Vault, ERP, "
            "MES, QMS, CTMS, eTMF, or similar GxP platform). "
            "Write FRS descriptions as best-practice implementation for that system type using "
            "plausible but generic screen/module names. Set Source_Section = 'URS-derived' for "
            "all FRS rows. "
            "For OQ test steps: apply RULE A for infrastructure/non-functional requirements "
            "(write technical verification procedures, not UI navigation). "
            "Apply RULE C for all application feature requirements — prefix every Test_Step "
            "with [SCREEN UNVERIFIED] and set Confidence = 0.60. "
            "This is required because screen names cannot be verified without a guide."
        )

    return _PROMPT_PASS2_RAW.format(
        context_block   = context_block,
        urs_csv         = urs_csv,
        system_guidance = system_guidance,
    )


# =============================================================================
# 6. TWO-PASS AI ANALYSIS ENGINE
# =============================================================================

# Known header signatures for each of the 4 datasets in Pass-2 output.
# Used by the robust splitter to locate dataset boundaries even when the
# LLM embeds stray ||| tokens inside quoted field values.
_PASS2_HEADERS = [
    # Dataset 1 — FRS (now includes Source_Section)
    r"^ID[,\t]Requirement_Description",
    # Dataset 2 — OQ (now includes Suggested_Evidence)
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


def _strip_preamble(text: str) -> str:
    """
    Strip any non-CSV prose the LLM prepends before the actual header row.

    Handles patterns like:
      "Here is the CSV:"            — narrative intro
      "Dataset 1 (FRS):"            — section label
      "Dataset 2:"                  — numbered label
      "## FRS Requirements"         — markdown heading
      "Sure! Here are the results:" — chatty preamble

    Strategy: scan lines top-to-bottom; return from the first line that
    looks like a real CSV row — i.e. it contains at least one comma AND
    starts with an alphanumeric or quote character AND does NOT start with
    a known preamble keyword.
    """
    PREAMBLE_RE = re.compile(
        r'^(here|dataset\s*\d|csv|the\s+following|below|output|result|sure|note|'
        r'i\s+have|please\s+find|as\s+requested|```|#)',
        re.IGNORECASE
    )
    lines = text.splitlines()
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        # Must contain a comma (CSV requirement)
        if ',' not in s:
            continue
        # Must start with alphanumeric or quote — not a sentence
        if not re.match(r'^[A-Za-z0-9"\']', s):
            continue
        # Must not match known preamble starters
        if PREAMBLE_RE.match(s):
            continue
        return '\n'.join(lines[i:])
    return text  # fallback — return as-is if nothing matched


def _robust_split_datasets(raw: str, headers: list) -> list:
    """
    Split LLM output into N CSV blocks by finding each known header line.
    Immune to:
      - stray ||| tokens inside quoted cell values
      - "Here is the CSV:" / "Dataset 1 (FRS):" label lines before the header
      - markdown fences wrapping each section

    Strategy:
      1. Strip fences and leading preamble from the whole block.
      2. For each header pattern, find the first matching line.
      3. Extract text from that line to the next header (or end).
      4. Apply _strip_preamble to each extracted section for safety.
      5. Return a list of N strings (empty string if a section is missing).
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
        # Strip: empty lines, ||| separators, ---, and section-label lines.
        # A section label is any line that ends with ":" and contains no commas
        # (e.g. "Dataset 2 (OQ):", "Here is the CSV:") — never a CSV data row.
        def _is_noise(line: str) -> bool:
            s = line.strip()
            if not s:                        return True
            if s in ("|||", "---"):          return True
            if s.endswith(":") and "," not in s:  return True   # label line
            return False
        cleaned = [l for l in section_lines if not _is_noise(l)]
        section_text = "\n".join(cleaned)
        # Final safety: strip any residual preamble within the section
        sections.append(_strip_preamble(section_text))

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
                "Requirement_Description": f"[HUMAN-IN-THE-LOOP SAFEGUARD — MANUAL REVIEW REQUIRED] "
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


def _fill_missing_oq(frs_df: pd.DataFrame, oq_df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect any FRS requirement that has no OQ test case and insert a clearly-flagged
    placeholder so nothing silently disappears from the test matrix.

    Mirrors _fill_missing_frs — same philosophy: the AI skips quietly, Python catches it.
    The placeholder is stamped [AI SKIPPED — MANUAL REVIEW REQUIRED] so a validator
    immediately knows it needs a real test case written. Confidence = 0.50.

    Called AFTER _fill_missing_frs so that FRS placeholder rows (themselves inserted
    because the AI skipped a URS→FRS) are also caught here and get a matching OQ
    placeholder. After inserting, _renumber_oq_ids is called again by the caller
    to assign clean sequential IDs to the expanded set.
    """
    if frs_df.empty or "ID" not in frs_df.columns:
        return oq_df

    # Build set of FRS IDs that already have at least one OQ test
    linked_frs = set()
    if not oq_df.empty and "Requirement_Link" in oq_df.columns:
        linked_frs = set(
            oq_df["Requirement_Link"].dropna().astype(str).str.strip()
        )

    placeholders = []
    for _, row in frs_df.iterrows():
        fid  = str(row.get("ID", "")).strip()
        desc = str(row.get("Requirement_Description", "")).strip()[:120]
        urs  = str(row.get("Source_URS_Ref", "N/A")).strip()
        if fid and fid not in linked_frs:
            placeholders.append({
                "Test_ID":               "OQ-PLACEHOLDER",   # renumbered below
                "Requirement_Link":      fid,
                "Requirement_Link_Type": "FRS",
                "Test_Step":             (
                    f"[HUMAN-IN-THE-LOOP SAFEGUARD — MANUAL REVIEW REQUIRED] "
                    f"No OQ test was generated for {fid}: '{desc}'. "
                    f"Write executable test steps for this requirement."
                ),
                "Expected_Result":       f"[MANUAL ENTRY REQUIRED] Expected outcome for {fid}",
                "Pass_Fail_Criteria":    "[MANUAL ENTRY REQUIRED] Define pass/fail criteria",
                "Source":                f"Derived from {urs}",
                "Confidence":            "0.50",
                "Confidence_Flag":       "⚠️ Review Required",
            })

    if not placeholders:
        return oq_df

    result = pd.concat([oq_df, pd.DataFrame(placeholders)], ignore_index=True)
    result.fillna("N/A", inplace=True)
    return result


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


def run_cross_source_analysis(
    urs_text: str,
    sys_context_text: str,
    model_id: str,
    sys_context_name: str = "User Guide"
) -> tuple:
    """
    Cross-Source Gap Analysis (v29):
    Compares the URS against the User Guide to find:
      1. Features in the User Guide NOT mentioned in the URS
         → generate an FRS flagged [GAP-SOURCE: User Guide Only]
         → these represent system capabilities the company failed to validate
      2. URS requirements with NO corresponding feature in the User Guide
         → flag as [GAP-SOURCE: URS Only — Not in User Guide]
         → these suggest the system may not support the requirement at all

    Returns: (cross_frs_rows: list[dict], cross_gap_rows: list[dict])
    Both are appended to the main FRS and Gap_Analysis tables.
    """
    CROSS_SOURCE_PROMPT = f"""
You are a GxP validation engineer performing a bidirectional gap analysis.

DOCUMENT A — USER REQUIREMENTS SPECIFICATION (URS):
{urs_text[:4000]}

DOCUMENT B — SYSTEM USER GUIDE / PRODUCT MANUAL ('{sys_context_name}'):
{sys_context_text[:4000]}

TASK: Perform a bidirectional gap analysis between the two documents.

PART 1 — Features in User Guide NOT in URS:
Identify system features or capabilities described in the User Guide that have NO
corresponding requirement in the URS. These represent missed validation scope.
For each, generate one FRS row with Source_URS_Ref = "[GAP-SOURCE: User Guide Only]".

PART 2 — URS Requirements NOT supported by User Guide:
Identify URS requirements that describe functionality NOT described anywhere in the
User Guide. These suggest the system may not support the requirement.
Generate one gap row for each with Gap_Type = "URS_Not_In_UserGuide".

Output EXACTLY 2 CSV datasets separated by |||.
Include the header row in EACH dataset. Wrap comma-containing values in double-quotes.
Use N/A for any field that is not applicable.

Dataset 1 (cross-source FRS rows):
ID,Requirement_Description,Priority,Risk,GxP_Impact,Source_URS_Ref,Source_Text,Source_Page,Confidence,Confidence_Flag
- ID: XFRS-001, XFRS-002, etc.
- Requirement_Description: engineering/technical description of the feature from the User Guide
- Source_URS_Ref: EXACTLY the string "[GAP-SOURCE: User Guide Only]"
- Confidence_Flag: "Cross-Source Gap — Review Required"

Dataset 2 (cross-source gap rows):
Req_ID,Gap_Type,Description,Recommendation,Severity
- Req_ID: use the URS Req_ID if known, otherwise "URS-UNMATCHED-NNN"
- Gap_Type: URS_Not_In_UserGuide
- Description: what the URS requires and why the User Guide doesn't cover it
- Recommendation: specific action (e.g. contact vendor, raise a change request, or add to URS)
- Severity: Critical / High / Medium

If no gaps exist in either direction, output two CSV headers with no data rows.
"""
    try:
        response = completion(
            model=model_id,
            stream=False,
            temperature=0.1,   # low temp for deterministic gap comparison
            messages=[
                {"role": "system", "content": (
                    "You are a senior GxP validation specialist. "
                    "You produce only structured CSV output — no prose, no markdown fences. "
                    "Your gap analysis findings will be incorporated into a regulated validation package."
                )},
                {"role": "user", "content": CROSS_SOURCE_PROMPT}
            ]
        )
        raw = response.choices[0].message.content or ""
        raw = re.sub(r'^```[a-zA-Z]*\n?', '', raw, flags=re.MULTILINE)
        raw = re.sub(r'```\s*$',          '', raw, flags=re.MULTILINE)

        parts = raw.split("|||")
        xfrs_csv = parts[0].strip() if len(parts) > 0 else ""
        xgap_csv = parts[1].strip() if len(parts) > 1 else ""

        xfrs_df = _csv_to_df(_strip_preamble(xfrs_csv))
        xgap_df = _csv_to_df(_strip_preamble(xgap_csv))

        # Filter out "no gaps" prose rows
        if not xfrs_df.empty and "ID" in xfrs_df.columns:
            xfrs_df = xfrs_df[xfrs_df["ID"].astype(str).str.startswith("XFRS")].reset_index(drop=True)
        if not xgap_df.empty and "Req_ID" in xgap_df.columns:
            no_gap_mask = xgap_df["Req_ID"].astype(str).str.lower().str.contains(
                r'no gap|none|n/a|not applicable', na=False, regex=True
            )
            xgap_df = xgap_df[~no_gap_mask].reset_index(drop=True)

        return xfrs_df, xgap_df

    except Exception as e:
        st.warning(f"⚠️ Cross-source analysis failed ({e}) — proceeding without it.")
        return pd.DataFrame(), pd.DataFrame()


def run_segmented_analysis(
    file_bytes: bytes,
    model_id: str,
    progress_bar,
    status_text,
    sys_context_bytes: bytes = None
) -> tuple:
    """
    Two-pass analysis with Fail-Stop Protocol (v27).

    Pass 1 — per-chunk URS extraction: produces a clean structured URS table.
    Pass 2 — single call with full URS table: produces FRS / OQ / Gap.
    Returns: (urs_df, frs_df, oq_df, trace_df, gap_df)

    Fail-Stop Protocol (21 CFR Part 11 / GxP compliance):
      If ANY Pass-1 segment fails, the entire analysis is aborted and a
      SegmentFailureError is raised. A validation package with missing
      pages would fail a regulatory audit — 100% coverage or nothing.
      The exception is caught in show_app() which logs the failure and
      shows a compliance-grade error message.

    SysContext (User Guide) injection:
      If sys_context_bytes is provided, the first 6 pages of the guide are
      extracted and injected into BOTH Pass-1 and Pass-2 system prompts so
      the LLM can reference actual screen names, module names, and field
      names when writing FRS descriptions and OQ test steps.

    Cross-chunk ID resequencing:
      After combining chunks, FRS IDs and OQ IDs are globally resequenced
      so FRS-001…FRS-N and OQ-001…OQ-M are always unique and sequential
      regardless of how many chunks the document was split into.
    """
    class SegmentFailureError(RuntimeError):
        pass

    all_pages = extract_pages(file_bytes)
    if not all_pages:
        raise SegmentFailureError(
            "No pages could be extracted from the uploaded PDF. "
            "The file may be image-only (scanned) or corrupt. "
            "Per ALCOA+ standards, non-searchable PDFs cannot be AI-validated."
        )

    # OCR / searchability gate: require minimum text density
    joined_text = "\n".join(all_pages)
    if len(joined_text.strip()) < 100:
        raise SegmentFailureError(
            "⛔ Compliance Warning: Document is not OCR-searchable.\n"
            "Non-searchable PDFs cannot be validated by the AI engine per ALCOA+ standards.\n"
            "Please convert the document to a text-based PDF using OCR software before uploading."
        )

    chunks = [all_pages[i:i + CHUNK_SIZE] for i in range(0, len(all_pages), CHUNK_SIZE)]
    total  = len(chunks)

    # ── SysContext (User Guide) extraction ───────────────────────────────────
    sys_context = ""
    if sys_context_bytes:
        try:
            sys_pages   = extract_pages(sys_context_bytes)
            sys_context = "\n\n".join(sys_pages[:6])   # first 6 pages ≈ 4-8k chars
            status_text.text("📖 User Guide loaded — injecting into analysis context...")
        except Exception as e:
            st.warning(f"⚠️ Could not extract User Guide context: {e} — proceeding without it.")

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
                    {"role": "system", "content": _make_system_prompt(sys_context)},
                    {"role": "user",   "content": build_pass1_prompt(chunk_text, idx, total)}
                ]
            )
            raw_urs = response.choices[0].message.content or ""
            raw_urs = re.sub(r'^```[a-zA-Z]*\n?', '', raw_urs, flags=re.MULTILINE)
            raw_urs = re.sub(r'```\s*$',          '', raw_urs, flags=re.MULTILINE)
            raw_urs = _strip_preamble(raw_urs.strip())
            df_urs  = _csv_to_df(raw_urs)
            if not df_urs.empty:
                urs_frames.append(df_urs)
        except Exception as e:
            # FAIL-STOP: any segment failure aborts the entire run
            raise SegmentFailureError(
                f"Pass 1 segment {idx + 1}/{total} failed: {e}\n\n"
                f"Analysis aborted. Per GxP Fail-Stop Protocol, an incomplete analysis "
                f"(missing pages {idx * CHUNK_SIZE + 1}–{min((idx + 1) * CHUNK_SIZE, len(all_pages))}) "
                f"cannot be used as a validation artifact. Please retry."
            ) from e

    def _combine(frames):
        if not frames:
            return pd.DataFrame()
        c = pd.concat(frames, ignore_index=True)
        c = _remove_duplicate_headers(c)
        c.dropna(how='all', inplace=True)
        return c

    urs_final = _combine(urs_frames)

    # ── Cross-chunk URS ID resequencing ──────────────────────────────────────
    # Each chunk's LLM may restart numbering at URS-001. Renumber globally.
    if not urs_final.empty and "Req_ID" in urs_final.columns:
        urs_final = urs_final.copy()
        urs_final["Req_ID"] = [f"URS-{i+1:03d}" for i in range(len(urs_final))]

    urs_final = _apply_confidence_flags(urs_final)

    progress_bar.progress(0.5)
    status_text.text("✅ Pass 1 complete — structured URS table built. Running Pass 2...")

    # ── PASS 2: Generate FRS / OQ / Gap from URS table ────────────────────────
    frs_frames, oq_frames, gap_frames = [], [], []

    if urs_final.empty:
        raise SegmentFailureError(
            "Pass 1 extracted zero requirements. The document may be empty, "
            "image-only, or contain no recognisable requirement statements. "
            "Analysis cannot continue."
        )

    urs_csv_str = urs_final.to_csv(index=False)
    urs_lines   = urs_csv_str.split("\n")
    header_line = urs_lines[0]
    data_lines  = urs_lines[1:]
    PASS2_CHUNK = st.session_state.get("pass2_chunk_size", 40)
    p2_chunks   = [data_lines[i:i+PASS2_CHUNK] for i in range(0, len(data_lines), PASS2_CHUNK)]
    p2_total    = len(p2_chunks)

    for p2_idx, p2_rows in enumerate(p2_chunks):
        p2_csv = header_line + "\n" + "\n".join(p2_rows)
        status_text.text(
            f"🔬 Pass 2 — Generating FRS/OQ/Gap: batch {p2_idx+1} of {p2_total}..."
        )
        progress_bar.progress(0.5 + (p2_idx / p2_total) * 0.45)

        try:
            response = completion(
                model=model_id,
                stream=False,
                temperature=TEMPERATURE,
                messages=[
                    {"role": "system", "content": _make_system_prompt(sys_context)},
                    {"role": "user",   "content": build_pass2_prompt(p2_csv, sys_context)}
                ]
            )
            raw_p2 = response.choices[0].message.content or ""
        except Exception as e:
            # FAIL-STOP: abort — do not produce a partial FRS/OQ
            raise SegmentFailureError(
                f"Pass 2 batch {p2_idx + 1}/{p2_total} failed: {e}\n\n"
                f"Analysis aborted. A partial FRS/OQ package covering only "
                f"{p2_idx * PASS2_CHUNK} of {len(data_lines)} requirements "
                f"would be invalid as a GxP validation artifact. Please retry."
            ) from e

        sections = _robust_split_datasets(raw_p2, _PASS2_HEADERS)
        frs_csv, oq_csv, gap_csv = sections[0], sections[1], sections[2]
        for frames, csv_text in [
            (frs_frames,  frs_csv),
            (oq_frames,   oq_csv),
            (gap_frames,  gap_csv),
        ]:
            df = _csv_to_df(csv_text)
            if not df.empty:
                frames.append(df)

    progress_bar.progress(0.95)
    status_text.text("✅ Both passes complete — running deterministic checks...")

    frs_final = _combine(frs_frames)
    oq_final  = _combine(oq_frames)
    gap_final = _combine(gap_frames)

    # ── Post-processing: global ID resequencing (cross-chunk dedup) ──────────
    # Each Pass-2 batch restarts FRS/OQ numbering. Renumber globally BEFORE
    # any cross-reference so FRS-001…FRS-N are unique across all batches.
    frs_final = _renumber_frs_ids(frs_final)
    oq_final  = _renumber_oq_ids(oq_final)

    # Patch OQ Requirement_Link to the renumbered FRS IDs using Source_URS_Ref mapping.
    # Build a map: Source_URS_Ref → new FRS ID (after renumber)
    if not frs_final.empty and "ID" in frs_final.columns and "Source_URS_Ref" in frs_final.columns:
        urs_to_frs = {}
        for _, r in frs_final.iterrows():
            u = str(r.get("Source_URS_Ref", "")).strip()
            f = str(r.get("ID", "")).strip()
            if u and f:
                urs_to_frs[u] = f
        # Update OQ Requirement_Link using Source column ("Derived from URS-NNN")
        if not oq_final.empty and "Requirement_Link" in oq_final.columns and "Source" in oq_final.columns:
            def _remap_link(row):
                link = str(row.get("Requirement_Link", "")).strip()
                src  = str(row.get("Source", "")).strip()
                # If link looks like a FRS ID already and it exists, keep it
                if re.match(r'^FRS-\d+$', link, re.IGNORECASE) and link in set(frs_final["ID"]):
                    return link
                # Try to extract URS ref from Source field
                m = re.search(r'URS-(\d+)', src, re.IGNORECASE)
                if m:
                    urs_ref = f"URS-{int(m.group(1)):03d}"
                    return urs_to_frs.get(urs_ref, link)
                return link
            oq_final["Requirement_Link"] = oq_final.apply(_remap_link, axis=1)

    frs_final = _clean_frs_columns(frs_final)
    frs_final = _fill_missing_frs(urs_final, frs_final)
    oq_final  = _fill_missing_oq(frs_final, oq_final)
    oq_final  = _renumber_oq_ids(oq_final)

    frs_final = _apply_confidence_flags(frs_final)
    oq_final  = _apply_confidence_flags(oq_final)
    urs_final = _apply_confidence_flags(urs_final)

    for df in [frs_final, oq_final, urs_final]:
        df.fillna("N/A", inplace=True)
        df.replace("", "N/A", inplace=True)

    gap_final   = _clean_gap_analysis(gap_final)
    trace_final = _build_traceability(urs_final, frs_final, oq_final)

    # ── Pass 3 (optional): Cross-Source Gap Analysis ─────────────────────────
    # Only runs when a User Guide was uploaded. Compares the URS against the
    # guide to find: (a) features in the guide not in the URS, (b) URS reqs
    # not supported by the guide. Results are appended to FRS and Gap tables.
    if sys_context and len(sys_context.strip()) > 200:
        status_text.text("🔀 Pass 3 — Cross-source URS ↔ User Guide gap analysis...")
        urs_text_for_cross = "\n".join(all_pages[:8])   # first 8 pages of URS
        xfrs_df, xgap_df = run_cross_source_analysis(
            urs_text    = urs_text_for_cross,
            sys_context_text = sys_context,
            model_id    = model_id,
            sys_context_name = "User Guide"
        )
        # Append cross-source FRS rows to main FRS table
        if not xfrs_df.empty:
            frs_final = pd.concat([frs_final, xfrs_df], ignore_index=True)
            frs_final.fillna("N/A", inplace=True)
        # Append cross-source gap rows to main gap table
        if not xgap_df.empty:
            # Ensure matching columns
            for col in ["Req_ID", "Gap_Type", "Description", "Recommendation", "Severity"]:
                if col not in xgap_df.columns:
                    xgap_df[col] = "N/A"
            gap_final = pd.concat([gap_final, xgap_df], ignore_index=True)
            gap_final.fillna("N/A", inplace=True)

        # Rebuild traceability to include cross-source FRS rows
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
                   "Ambiguous", "Duplicate", "Missing_FRS",
                   "Non_Functional", "Missing_Test", "Non_Testable_Requirement"}

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

    # ── R3c: Non-functional requirement detection ─────────────────────────────
    # Non-functional requirements (availability, SLA, performance targets) need
    # a different test strategy — not testable via standard OQ steps.
    # Flagged so teams define a separate load/stress/performance protocol.
    NON_FUNCTIONAL_KEYWORDS = [
        "availability", "uptime", "sla", "response time", "throughput",
        "maintainability", "portability", "interoperability", "disaster recovery",
        "backup", "recovery time", "rto", "rpo", "capacity", "concurrency",
        "load", "stress", "fault tolerance",
    ]
    if desc_col:
        for _, row in frs_df.iterrows():
            desc = str(row.get(desc_col, "")).lower()
            fid  = str(row.get("ID", "")).strip()
            nf_found = [kw for kw in NON_FUNCTIONAL_KEYWORDS if kw in desc]
            if nf_found:
                issues.append({
                    "Rule":            "R3c",
                    "Req_ID":          fid,
                    "Gap_Type":        "Non_Functional",
                    "Description":     (f"Non-functional requirement detected: {', '.join(nf_found)}. "
                                        f"Standard OQ steps are insufficient for this requirement type."),
                    "Recommendation":  ("Define a separate non-functional test protocol: load/stress test, "
                                        "SLA monitoring, or performance benchmark. Document in IQ/PQ."),
                    "Severity":        "Medium",
                })

    # ── R3d: Non-Testable Requirement detection (scans URS, not FRS) ─────────
    # Flags URS requirements marked Testable=No or containing weak/ambiguous
    # verbs with no quantitative criteria. These are a direct 21 CFR Part 11
    # compliance risk — a requirement that cannot be tested cannot be validated.
    # Maps each weak term to a concrete remediation recommendation.
    WEAK_VERB_REMEDIATION = {
        "user-friendly":  "Define measurable usability criteria (e.g., task completion rate ≥95%, error rate <5%)",
        "user friendly":  "Define measurable usability criteria (e.g., task completion rate ≥95%, error rate <5%)",
        "easy":           "Specify what 'easy' means quantitatively (e.g., onboarding ≤3 clicks, help requests <2/session)",
        "intuitive":      "Define learnability criteria (e.g., new user completes primary task without help within 5 minutes)",
        "fast":           "Specify maximum response time (e.g., page load ≤2 seconds under 100 concurrent users)",
        "quickly":        "Specify maximum response time (e.g., query returns results within 1 second)",
        "seamless":       "Define integration acceptance criteria (e.g., zero data loss, end-to-end transaction ≤3 seconds)",
        "simple":         "Specify complexity metric (e.g., ISO 9241 SUS score ≥80)",
        "flexible":       "Define the specific configuration options required (list each configurable parameter)",
        "robust":         "Specify fault-tolerance criteria (e.g., system recovers from single component failure within 30 seconds)",
        "scalable":       "Define quantitative capacity targets (e.g., supports 500 concurrent users with <5% response degradation)",
        "reliable":       "Specify uptime/availability target (e.g., 99.9% uptime excluding planned maintenance)",
        "convenient":     "Define task-time criteria (e.g., common workflows completable in ≤3 steps)",
        "smooth":         "Specify latency/throughput targets for the identified workflow",
        "modern":         "Remove subjective aesthetic term; specify functional requirements instead",
        "efficient":      "Specify time-on-task or resource consumption target (e.g., batch process ≤10 minutes for 10,000 records)",
        "should":         "Replace 'should' with 'shall' to make this a mandatory requirement, or remove if optional",
        "may":            "Clarify whether this is mandatory (use 'shall') or optional — ambiguous modal verb",
        "appropriate":    "Define specific acceptance criteria for what constitutes 'appropriate'",
        "adequate":       "Define minimum quantitative threshold for adequacy",
        "sufficient":     "Define minimum quantitative threshold for sufficiency",
    }
    if not urs_df.empty:
        urs_req_col  = "Requirement_Description" if "Requirement_Description" in urs_df.columns else None
        urs_test_col = "Testable" if "Testable" in urs_df.columns else None
        urs_id_col   = "Req_ID"   if "Req_ID"   in urs_df.columns else None
        if urs_req_col and urs_id_col:
            for _, row in urs_df.iterrows():
                uid  = str(row.get(urs_id_col, "")).strip()
                desc = str(row.get(urs_req_col, "")).lower()
                testable = str(row.get(urs_test_col, "")).strip().lower() if urs_test_col else "yes"

                # Find the first matching weak verb/phrase
                matched_term = next(
                    (term for term in WEAK_VERB_REMEDIATION if term in desc),
                    None
                )
                if matched_term or testable == "no":
                    term_label = matched_term or "non-testable language"
                    recommendation = WEAK_VERB_REMEDIATION.get(
                        matched_term,
                        "Replace vague language with specific, measurable acceptance criteria."
                    )
                    issues.append({
                        "Rule":           "R3d",
                        "Req_ID":         uid,
                        "Gap_Type":       "Non_Testable_Requirement",
                        "Description":    (f"Contains ambiguous/non-testable term: '{term_label}'. "
                                           f"This requirement cannot be objectively validated — "
                                           f"a direct 21 CFR Part 11 compliance risk."),
                        "Recommendation": recommendation,
                        "Severity":       "High",
                    })
                    gap_df = pd.concat([gap_df, pd.DataFrame([{
                        "Req_ID":         uid,
                        "Gap_Type":       "Non_Testable_Requirement",
                        "Description":    f"Ambiguous term: '{term_label}'",
                        "Recommendation": recommendation,
                        "Severity":       "High",
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

    # ── R6: Human-in-the-Loop safeguard rows ─────────────────────────────────
    # OQ rows with the HITL placeholder text require manual completion.
    # These are not errors but represent mandatory human actions before sign-off.
    if not oq_df.empty and "Test_Step" in oq_df.columns:
        for _, row in oq_df.iterrows():
            step    = str(row.get("Test_Step", ""))
            test_id = str(row.get("Test_ID", "")).strip()
            if "HUMAN-IN-THE-LOOP SAFEGUARD" in step or "MANUAL REVIEW REQUIRED" in step:
                req_link = str(row.get("Requirement_Link", "")).strip()
                issues.append({
                    "Rule":            "R6",
                    "Req_ID":          test_id,
                    "Gap_Type":        "Manual_Action_Required",
                    "Description":     (f"{test_id} (linked to {req_link}) was not generated "
                                        f"by the AI and requires manual test steps to be written "
                                        f"before this validation package can be signed off."),
                    "Recommendation":  (f"Open the OQ sheet, locate {test_id}, and write executable "
                                        f"test steps, expected results, and pass/fail criteria. "
                                        f"This row must be completed before IQ/OQ execution."),
                    "Severity":        "Critical",
                })

    det_issues_df = pd.DataFrame(issues) if issues else pd.DataFrame(
        columns=["Rule", "Req_ID", "Gap_Type", "Description", "Recommendation", "Severity"]
    )

    return gap_df, det_issues_df

def build_audit_log_sheet(user: str, file_name: str, model_name: str,
                          frs_df: pd.DataFrame, oq_df: pd.DataFrame,
                          gap_df: pd.DataFrame, det_df: pd.DataFrame,
                          version_frs: int, version_oq: int,
                          doc_ids: str = "",
                          sys_context_name: str = "") -> pd.DataFrame:
    now_str   = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    role      = get_user_role(user)
    gap_count = len(gap_df) if not gap_df.empty else 0
    det_count = len(det_df) if not det_df.empty else 0

    urs_entry = {
        "Event":            "DOCUMENT_UPLOADED",
        "User":             user,
        "Role":             role,
        "Timestamp":        now_str,
        "Object_Changed":   "URS/SOP",
        "Old_Value":        "",
        "New_Value":        file_name,
        "Reason":           "URS file submitted for analysis",
        "AI_Metadata":      "",
    }

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
        urs_entry,
    ]

    # Chain-of-Custody: record User Guide if one was provided
    if sys_context_name:
        rows.append({
            "Event":            "SYSCONTEXT_UPLOADED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "USER_GUIDE",
            "Old_Value":        "",
            "New_Value":        sys_context_name,
            "Reason":           "System User Guide injected as Reference Material for FRS/OQ generation",
            "AI_Metadata":      f"guide_file={sys_context_name}",
        })

    rows += [
        {
            "Event":            "AI_ANALYSIS_INITIATED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   "ANALYSIS_ENGINE",
            "Old_Value":        "",
            "New_Value":        f"Model: {model_name} | Prompt: {PROMPT_VERSION} | Temp: {TEMPERATURE}",
            "Reason":           "GAMP-5 segmented analysis started"
                                + (f" with User Guide: {sys_context_name}" if sys_context_name else ""),
            "AI_Metadata":      f"prompt_version={PROMPT_VERSION} | model={model_name} | "
                                f"temperature={TEMPERATURE} | doc_ids={doc_ids}"
                                + (f" | guide={sys_context_name}" if sys_context_name else ""),
        },
        {
            "Event":            "FRS_GENERATED",
            "User":             user,
            "Role":             role,
            "Timestamp":        now_str,
            "Object_Changed":   f"FRS v{version_frs}.0",
            "Old_Value":        f"v{version_frs - 1}.0" if version_frs > 1 else "N/A",
            "New_Value":        f"v{version_frs}.0 — {len(frs_df)} requirements",
            "Reason":           "Functional requirements derived from URS"
                                + (f" + {sys_context_name}" if sys_context_name else ""),
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
        {"KPI": "📊 Basic Traceability % (Any Test Exists)",   "Value": f"{coverage_pct}%",
         "Status": "✅ PASS" if coverage_pct >= 80 else ("⚠️ REVIEW" if coverage_pct >= 60 else "❌ FAIL")},
        {"KPI": "🎯 Risk-Adjusted Compliance % (Fully Covered)", "Value": f"{fully_covered_pct}%",
         "Status": "✅ PASS" if fully_covered_pct >= 80 else ("⚠️ REVIEW" if fully_covered_pct >= 60 else "❌ FAIL")},
        {"KPI": "🔴 High Risk Requirements",            "Value": high_risk,             "Status": "Requires ≥3 OQ tests each"},
        {"KPI": "🟡 Medium Risk Requirements",          "Value": med_risk,              "Status": "Requires ≥2 OQ tests each"},
        {"KPI": "🟢 Low Risk Requirements",             "Value": low_risk,              "Status": "Requires ≥1 OQ test each"},
        {"KPI": "🚨 Missing FRS (AI skipped URS)",     "Value": missing_frs,           "Status": "Critical — re-run or add manually"},
        {"KPI": "⚠️ AI-Detected Gaps",                 "Value": ai_gaps,
         "Status": "See Gap_Analysis sheet" if ai_gaps > 0 else "✅ None — see Traceability sheet"},
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
    "Summary":          {"header_fill": "0F172A", "tab_color": "1E293B"},
    "Signature":        {"header_fill": "1E3A5F", "tab_color": "1E3A5F"},
    "Dashboard":        {"header_fill": "0F172A", "tab_color": "0F172A"},
    "URS_Extraction":   {"header_fill": "1D4ED8", "tab_color": "1D4ED8"},
    "FRS":              {"header_fill": "2563EB", "tab_color": "2563EB"},
    "OQ":               {"header_fill": "059669", "tab_color": "059669"},
    "Traceability":     {"header_fill": "7C3AED", "tab_color": "7C3AED"},
    "Gap_Analysis":     {"header_fill": "DC2626", "tab_color": "DC2626"},
    "Det_Validation":   {"header_fill": "EA580C", "tab_color": "EA580C"},
    "Audit_Log":        {"header_fill": "B45309", "tab_color": "B45309"},
}


def build_pdf_bytes(r: dict, sig_id: int, sig_meaning: str,
                    sig_timestamp: str, user: str, role: str) -> bytes:
    """
    Generate a signed PDF summary of the validation package.

    Includes: KPI summary table, electronic signature block, SHA-256 hash
    with scope statement, and regulatory disclaimer.

    The document_hash embedded here is identical to the one in the Excel
    Signature sheet — both are produced by the same signing event (sig_id).
    """
    from reportlab.lib.pagesizes import A4
    from reportlab.lib import colors
    from reportlab.lib.units import mm
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, HRFlowable)
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                            leftMargin=20*mm, rightMargin=20*mm,
                            topMargin=18*mm, bottomMargin=18*mm)
    styles = getSampleStyleSheet()

    C_NAVY   = colors.HexColor("#0F172A")
    C_DKBLUE = colors.HexColor("#1E3A5F")
    C_TEAL   = colors.HexColor("#2563EB")
    C_GREEN  = colors.HexColor("#059669")
    C_RED    = colors.HexColor("#DC2626")
    C_AMBER  = colors.HexColor("#D97706")
    C_LGREY  = colors.HexColor("#F8FAFC")
    C_MGREY  = colors.HexColor("#94A3B8")
    C_DGREY  = colors.HexColor("#374151")
    C_GRID   = colors.HexColor("#CBD5E1")

    def _style(name, **kw):
        return ParagraphStyle(name, parent=styles["Normal"], **kw)

    h1   = _style("h1",   fontSize=16, textColor=colors.white, backColor=C_NAVY,
                           alignment=TA_CENTER, fontName="Helvetica-Bold",
                           spaceAfter=0, topPadding=10, bottomPadding=10)
    h2   = _style("h2",   fontSize=9,  textColor=colors.white, backColor=C_TEAL,
                           alignment=TA_CENTER, fontName="Helvetica-BoldOblique",
                           spaceAfter=0, topPadding=6, bottomPadding=6)
    sec  = _style("sec",  fontSize=9,  textColor=colors.white, backColor=C_DKBLUE,
                           alignment=TA_CENTER, fontName="Helvetica-Bold",
                           spaceAfter=0, spaceBefore=6, topPadding=5, bottomPadding=5)
    body = _style("body", fontSize=8.5, textColor=C_DGREY, spaceAfter=2)
    sm   = _style("sm",   fontSize=7,  textColor=C_MGREY,
                           fontName="Helvetica-Oblique", spaceAfter=3)

    cov_pct   = r.get("cov_pct", 0)
    gap_total = r.get("gap_count", 0) + r.get("det_count", 0)
    doc_hash  = r.get("doc_hash", "N/A")
    file_name = r.get("file_name", "")

    story = []

    # ── Title ─────────────────────────────────────────────────────────────────
    story.append(Paragraph("VALIDATION PACKAGE — SIGNED SUMMARY", h1))
    story.append(Paragraph(
        f"21 CFR Part 11 Compliant &nbsp;|&nbsp; App v{VERSION} &nbsp;|&nbsp; "
        f"Generated {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC", h2))
    story.append(Spacer(1, 5*mm))

    # ── KPI Summary ───────────────────────────────────────────────────────────
    story.append(Paragraph("VALIDATION PACKAGE SUMMARY", sec))
    story.append(Spacer(1, 1*mm))
    kpi_rows = [
        ["Metric", "Value", "Status"],
        ["Requirements extracted (URS)", str(r.get("total_urs", 0)), ""],
        ["FRS requirements generated",   str(r.get("total_reqs", 0)), ""],
        ["OQ test cases generated",      str(r.get("total_tests", 0)), ""],
        ["Fully covered requirements",   str(r.get("covered", 0)), ""],
        ["Traceability coverage",        f"{cov_pct}%",
         "PASS" if cov_pct >= 80 else ("REVIEW" if cov_pct >= 60 else "FAIL")],
        ["Gaps detected (AI + Det.)",    str(gap_total),
         "PASS" if gap_total == 0 else "REVIEW"],
    ]
    kpi_tbl = Table(kpi_rows, colWidths=[90*mm, 30*mm, 30*mm])
    kpi_sty = [
        ("BACKGROUND",    (0, 0), (-1, 0), C_TEAL),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1,-1), 8.5),
        ("ROWBACKGROUNDS",(0, 1), (-1,-1), [C_LGREY, colors.white]),
        ("GRID",          (0, 0), (-1,-1), 0.3, C_GRID),
        ("TOPPADDING",    (0, 0), (-1,-1), 4),
        ("BOTTOMPADDING", (0, 0), (-1,-1), 4),
        ("LEFTPADDING",   (0, 0), (-1,-1), 6),
    ]
    for ri, row in enumerate(kpi_rows[1:], start=1):
        s = row[2]
        c = C_GREEN if s == "PASS" else (C_AMBER if s == "REVIEW" else (C_RED if s == "FAIL" else C_DGREY))
        kpi_sty += [("TEXTCOLOR", (2, ri), (2, ri), c),
                    ("FONTNAME",  (2, ri), (2, ri), "Helvetica-Bold")]
    kpi_tbl.setStyle(TableStyle(kpi_sty))
    story.append(kpi_tbl)
    story.append(Spacer(1, 5*mm))

    # ── Electronic Signature ──────────────────────────────────────────────────
    story.append(Paragraph("ELECTRONIC SIGNATURE RECORD", sec))
    story.append(Spacer(1, 1*mm))
    story.append(Paragraph(
        "21 CFR Part 11 §11.50 / §11.200 — Non-Biometric Electronic Signature", sm))
    sig_rows = [
        ["Field",             "Value",                      "Regulatory Reference"],
        ["Signer Name",       user,                          "§11.50(a)(1) — printed name"],
        ["Role",              role,                          "Role at time of signing"],
        ["Signature ID",      f"SIG-{sig_id:06d}",          "Unique signature identifier"],
        ["Date / Time (UTC)", sig_timestamp[:19],            "§11.50(a)(1) — date and time"],
        ["Action Signed",     "Generated Validation Package","Specific act being signed"],
        ["Meaning",           sig_meaning,                   "§11.50(a)(1) — meaning"],
        ["System Version",    VERSION,                       "App version at signing"],
        ["Prompt Version",    PROMPT_VERSION,                "AI prompt version"],
        ["Source Document",   file_name,                     "URS file analysed"],
    ]
    sig_tbl = Table(sig_rows, colWidths=[40*mm, 68*mm, 42*mm])
    sig_sty = [
        ("BACKGROUND",    (0, 0), (-1, 0), C_DKBLUE),
        ("TEXTCOLOR",     (0, 0), (-1, 0), colors.white),
        ("FONTNAME",      (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1,-1), 8),
        ("FONTNAME",      (0, 1), (0, -1), "Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0, 1), (-1,-1), [C_LGREY, colors.white]),
        ("GRID",          (0, 0), (-1,-1), 0.3, C_GRID),
        ("TOPPADDING",    (0, 0), (-1,-1), 4),
        ("BOTTOMPADDING", (0, 0), (-1,-1), 4),
        ("LEFTPADDING",   (0, 0), (-1,-1), 6),
        ("TEXTCOLOR",     (2, 1), (2, -1), C_MGREY),
        ("FONTNAME",      (2, 1), (2, -1), "Helvetica-Oblique"),
    ]
    sig_tbl.setStyle(TableStyle(sig_sty))
    story.append(sig_tbl)
    story.append(Spacer(1, 5*mm))

    # ── Document Hash ─────────────────────────────────────────────────────────
    story.append(Paragraph("DOCUMENT INTEGRITY — SHA-256 HASH", sec))
    story.append(Spacer(1, 1*mm))
    hash_rows = [
        ["Hash Algorithm", "SHA-256 (hashlib.sha256)"],
        ["Document Hash",  doc_hash],
        ["Hash Scope",
         "Covers all Excel workbook sheets EXCEPT the Signature sheet "
         "(standard practice — same as PDF digital signatures). "
         "To verify: re-export the workbook without the Signature sheet "
         "and recompute SHA-256."],
    ]
    hash_tbl = Table(hash_rows, colWidths=[35*mm, 115*mm])
    hash_sty = [
        ("FONTNAME",      (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1,-1), 8),
        ("ROWBACKGROUNDS",(0, 0), (-1,-1), [C_LGREY, colors.HexColor("#D1FAE5"), C_LGREY]),
        ("GRID",          (0, 0), (-1,-1), 0.3, C_GRID),
        ("TOPPADDING",    (0, 0), (-1,-1), 4),
        ("BOTTOMPADDING", (0, 0), (-1,-1), 4),
        ("LEFTPADDING",   (0, 0), (-1,-1), 6),
        ("FONTNAME",      (1, 1), (1,  1), "Courier"),
        ("FONTSIZE",      (1, 1), (1,  1), 7),
        ("TEXTCOLOR",     (1, 1), (1,  1), C_DKBLUE),
        ("VALIGN",        (0, 0), (-1,-1), "TOP"),
        ("FONTSIZE",      (1, 2), (1,  2), 7.5),
    ]
    hash_tbl.setStyle(TableStyle(hash_sty))
    story.append(hash_tbl)
    story.append(Spacer(1, 4*mm))

    # ── Regulatory disclaimer ─────────────────────────────────────────────────
    story.append(HRFlowable(width="100%", thickness=0.5, color=C_GRID))
    story.append(Spacer(1, 2*mm))
    story.append(Paragraph(
        "<b>REGULATORY STATEMENT:</b> This electronic signature was applied in accordance with "
        "21 CFR Part 11. The signer's identity was verified at the time of signing by re-entry "
        "of their system password (two-component non-biometric e-signature per §11.200(b)(1)). "
        "This record is stored in an append-only signature log and cannot be altered or deleted.",
        _style("disc", fontSize=7, textColor=C_DGREY, fontName="Helvetica-Oblique")))

    doc.build(story)
    return buf.getvalue()


def build_signature_sheet(wb,
                          user: str,
                          role: str,
                          meaning: str,
                          document_hash: str,
                          document_name: str,
                          model_used: str,
                          signature_id: int,
                          timestamp: str):
    """
    Create the Signature sheet as the LAST tab in the workbook.

    Satisfies 21 CFR Part 11 §11.50:
      - Printed name of signer, date/time, meaning of signature
      - SHA-256 hash links signature to the exact workbook bytes
      - Explicit hash scope statement (excludes this sheet — standard practice)
      - Regulatory statement citing §11.200(b)(1)
    """
    ws = wb.create_sheet("Signature")
    ws.sheet_properties.tabColor = "1E3A5F"

    navy    = PatternFill("solid", fgColor="0F172A")
    dkblue  = PatternFill("solid", fgColor="1E3A5F")
    white   = PatternFill("solid", fgColor="FFFFFF")
    lgrey   = PatternFill("solid", fgColor="F8FAFC")
    green   = PatternFill("solid", fgColor="D1FAE5")
    thin    = Side(style="thin",   color="CBD5E1")
    border  = Border(left=thin, right=thin, top=thin, bottom=thin)

    def _cell(row, col, value, bold=False, size=11, color="000000",
              fill=None, align="left", wrap=False, italic=False):
        c = ws.cell(row=row, column=col, value=value)
        c.font      = Font(bold=bold, size=size, color=color, italic=italic)
        c.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
        c.fill      = fill or white
        c.border    = border
        return c

    ws.merge_cells("A1:C1")
    ws.row_dimensions[1].height = 44
    _cell(1, 1, "ELECTRONIC SIGNATURE RECORD",
          bold=True, size=16, color="FFFFFF", fill=navy, align="center")

    ws.merge_cells("A2:C2")
    ws.row_dimensions[2].height = 20
    _cell(2, 1,
          "21 CFR Part 11 §11.50 / §11.200 — Non-Biometric Electronic Signature",
          bold=False, size=9, color="FFFFFF", fill=dkblue, align="center", italic=True)

    ws.row_dimensions[3].height = 10

    fields = [
        ("Signer Name",        user,                      "§11.50(a)(1) — printed name"),
        ("Role",               role,                      "User role at time of signing"),
        ("Signature ID",       f"SIG-{signature_id:06d}", "Unique signature record identifier"),
        ("Date / Time (UTC)",  timestamp,                 "§11.50(a)(1) — date and time"),
        ("Action Signed",      "Generated Validation Package",
                                                           "The specific act being signed"),
        ("Meaning",            meaning,                   "§11.50(a)(1) — meaning of signature"),
        ("System Version",     VERSION,                   "Application version at signing"),
        ("AI Model Used",      model_used,                "Model that generated the package"),
        ("Prompt Version",     PROMPT_VERSION,            "AI prompt version used"),
        ("Source Document",    document_name,             "URS/SOP file analysed"),
    ]

    row = 4
    for label, value, note in fields:
        ws.row_dimensions[row].height = 24
        _cell(row, 1, label, bold=True,  size=10, fill=lgrey, align="left")
        _cell(row, 2, value, bold=False, size=10, fill=white, align="left")
        _cell(row, 3, note,  bold=False, size=8,
              color="64748B", fill=lgrey, align="left", italic=True)
        row += 1

    ws.row_dimensions[row].height = 10
    row += 1

    ws.merge_cells(f"A{row}:C{row}")
    ws.row_dimensions[row].height = 22
    _cell(row, 1, "DOCUMENT INTEGRITY — SHA-256 HASH",
          bold=True, size=11, color="FFFFFF", fill=dkblue, align="center")
    row += 1

    ws.row_dimensions[row].height = 22
    _cell(row, 1, "Document Hash (SHA-256)", bold=True, size=10, fill=lgrey)
    ws.merge_cells(f"B{row}:C{row}")
    c = ws.cell(row=row, column=2, value=document_hash)
    c.font      = Font(name="Courier New", size=9, bold=True, color="1E3A5F")
    c.fill      = green
    c.alignment = Alignment(horizontal="left", vertical="center", wrap_text=False)
    c.border    = border
    row += 1

    ws.row_dimensions[row].height = 22
    _cell(row, 1, "Hash Algorithm", bold=True, size=10, fill=lgrey)
    ws.merge_cells(f"B{row}:C{row}")
    _cell(row, 2, "SHA-256 (hashlib.sha256)", bold=False, size=10, fill=white)
    row += 1

    ws.row_dimensions[row].height = 36
    _cell(row, 1, "Hash Scope", bold=True, size=10, fill=lgrey)
    ws.merge_cells(f"B{row}:C{row}")
    _cell(row, 2,
          "Hash covers all workbook sheets EXCEPT this Signature sheet. "
          "This is standard practice (identical to PDF digital signatures). "
          "To verify: re-export the workbook without this sheet and recompute SHA-256.",
          bold=False, size=8, color="374151", fill=lgrey, wrap=True)
    row += 1

    ws.row_dimensions[row].height = 10
    row += 1

    ws.merge_cells(f"A{row}:C{row}")
    ws.row_dimensions[row].height = 48
    _cell(row, 1,
          "REGULATORY STATEMENT: This electronic signature was applied in accordance with "
          "21 CFR Part 11. The signer's identity was verified at the time of signing by "
          "re-entry of their system password (two-component non-biometric e-signature per "
          "§11.200(b)(1)). This record is stored in an append-only signature log and "
          "cannot be altered or deleted.",
          bold=False, size=8, color="374151", fill=lgrey, align="left", wrap=True)

    ws.column_dimensions["A"].width = 28
    ws.column_dimensions["B"].width = 55
    ws.column_dimensions["C"].width = 38


def build_cover_sheet(wb, user: str, file_name: str, model_name: str,
                      dashboard_df: pd.DataFrame,
                      sys_context_name: str = ""):
    """
    Create an executive-ready Summary tab as the first sheet.
    Displays KPIs in large font, includes Digital Signature lines.
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    ws = wb.create_sheet("Summary", 0)   # insert at position 0 (first tab)
    ws.sheet_properties.tabColor = "1E293B"

    # ── Freeze first row ──
    ws.freeze_panes = "A2"

    navy   = PatternFill("solid", fgColor="0F172A")
    teal   = PatternFill("solid", fgColor="0E7490")
    white  = PatternFill("solid", fgColor="FFFFFF")
    lgrey  = PatternFill("solid", fgColor="F8FAFC")
    green  = PatternFill("solid", fgColor="D1FAE5")
    yellow = PatternFill("solid", fgColor="FEF9C3")
    red    = PatternFill("solid", fgColor="FEE2E2")
    thin   = Side(style="thin", color="CBD5E1")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    def _set(row, col, value, bold=False, size=11, color="000000",
             fill=None, align="left", wrap=False):
        cell = ws.cell(row=row, column=col, value=value)
        cell.font      = Font(bold=bold, size=size, color=color)
        cell.alignment = Alignment(horizontal=align, vertical="center", wrap_text=wrap)
        if fill:
            cell.fill = fill
        cell.border = border
        return cell

    # ── Row 1: Title banner ──
    ws.merge_cells("A1:F1")
    ws.row_dimensions[1].height = 40
    _set(1, 1, "GxP Validation Package — Executive Summary",
         bold=True, size=18, color="FFFFFF", fill=navy, align="center")

    # ── Row 2: Sub-header ──
    ws.merge_cells("A2:F2")
    ws.row_dimensions[2].height = 22
    gen_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    _set(2, 1, f"Generated: {gen_time}  |  Source: {file_name}  |  Model: {model_name}",
         bold=False, size=10, color="FFFFFF", fill=teal, align="center")

    if sys_context_name:
        ws.merge_cells("A3:F3")
        ws.row_dimensions[3].height = 18
        _set(3, 1, f"User Guide Reference: {sys_context_name}",
             bold=False, size=9, color="334155", fill=lgrey, align="center")
        next_row = 4
    else:
        next_row = 3

    # ── Blank spacer ──
    ws.row_dimensions[next_row].height = 10
    next_row += 1

    # ── KPI section header ──
    ws.merge_cells(f"A{next_row}:F{next_row}")
    ws.row_dimensions[next_row].height = 24
    _set(next_row, 1, "KEY PERFORMANCE INDICATORS",
         bold=True, size=13, color="FFFFFF", fill=navy, align="center")
    next_row += 1

    # ── KPI rows from dashboard_df ──
    kpi_fill_map = {}   # detect pass/fail for colouring
    if not dashboard_df.empty and "KPI" in dashboard_df.columns:
        ws.row_dimensions[next_row].height = 20
        _set(next_row, 1, "Metric",   bold=True, size=10, color="FFFFFF", fill=teal, align="center")
        _set(next_row, 2, "Value",    bold=True, size=10, color="FFFFFF", fill=teal, align="center")
        _set(next_row, 4, "Status",   bold=True, size=10, color="FFFFFF", fill=teal, align="center")
        # Merge B + C for value, E+F for status
        ws.merge_cells(f"B{next_row}:C{next_row}")
        ws.merge_cells(f"D{next_row}:F{next_row}")
        ws.cell(row=next_row, column=4).value = "Status"
        ws.cell(row=next_row, column=4).font  = Font(bold=True, size=10, color="FFFFFF")
        ws.cell(row=next_row, column=4).fill  = teal
        ws.cell(row=next_row, column=4).alignment = Alignment(horizontal="center", vertical="center")
        ws.cell(row=next_row, column=4).border = border
        next_row += 1

        for _, kpi_row in dashboard_df.iterrows():
            kpi_name   = str(kpi_row.get("KPI",    ""))
            kpi_val    = str(kpi_row.get("Value",  ""))
            kpi_status = str(kpi_row.get("Status", ""))
            row_fill   = green if "PASS" in kpi_status else (
                         red   if "FAIL" in kpi_status else (
                         yellow if "REVIEW" in kpi_status else lgrey))
            ws.row_dimensions[next_row].height = 22
            ws.merge_cells(f"B{next_row}:C{next_row}")
            ws.merge_cells(f"D{next_row}:F{next_row}")
            _set(next_row, 1, kpi_name, bold=False, size=10, fill=row_fill, align="left",  wrap=True)
            _set(next_row, 2, kpi_val,  bold=True,  size=10, fill=row_fill, align="center")
            ws.cell(row=next_row, column=4).value     = kpi_status
            ws.cell(row=next_row, column=4).font      = Font(bold=False, size=10)
            ws.cell(row=next_row, column=4).fill      = row_fill
            ws.cell(row=next_row, column=4).alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            ws.cell(row=next_row, column=4).border    = border
            next_row += 1

    # ── Blank spacer ──
    ws.row_dimensions[next_row].height = 16
    next_row += 1

    # ── Digital Signatures section ──
    ws.merge_cells(f"A{next_row}:F{next_row}")
    ws.row_dimensions[next_row].height = 24
    _set(next_row, 1, "ELECTRONIC SIGNATURE — 21 CFR PART 11",
         bold=True, size=13, color="FFFFFF", fill=navy, align="center")
    next_row += 1

    sig_fields = [
        ("Prepared By (Validation Engineer)",  user,        "Signature / Initials"),
        ("Prepared Date",                       gen_time,    "Date (UTC)"),
        ("AI Model Used",                       model_name,  "System"),
        ("Prompt Version",                      PROMPT_VERSION, "Audit Reference"),
        ("Reviewed By (QA Manager)",            "________________________", "Signature"),
        ("Review Date",                         "________________________", "Date"),
        ("Approved By (QA Director / Sponsor)", "________________________", "Signature"),
        ("Approval Date",                       "________________________", "Date"),
    ]
    for sig_label, sig_val, sig_type in sig_fields:
        ws.row_dimensions[next_row].height = 24
        ws.merge_cells(f"C{next_row}:D{next_row}")
        ws.merge_cells(f"E{next_row}:F{next_row}")
        _set(next_row, 1, sig_label,  bold=True,  size=10, fill=lgrey, align="left")
        ws.merge_cells(f"B{next_row}:B{next_row}")
        _set(next_row, 2, sig_type,   bold=False, size=9,  fill=lgrey, color="64748B", align="center")
        ws.cell(row=next_row, column=3).value     = sig_val
        ws.cell(row=next_row, column=3).font      = Font(bold=True, size=10, color="1E40AF")
        ws.cell(row=next_row, column=3).fill      = white
        ws.cell(row=next_row, column=3).alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)
        ws.cell(row=next_row, column=3).border    = border
        next_row += 1

    # ── Disclaimer footer ──
    ws.row_dimensions[next_row].height = 10
    next_row += 1
    ws.merge_cells(f"A{next_row}:F{next_row}")
    ws.row_dimensions[next_row].height = 30
    _set(next_row, 1,
         "DISCLAIMER: This document was AI-generated as a draft validation artefact. "
         "All content must be reviewed and approved by a qualified GxP professional "
         "before use in a regulated submission. AI outputs do not constitute a validated "
         "system qualification without human review per GAMP 5 and ICH Q10.",
         bold=False, size=8, color="64748B", fill=lgrey, align="center", wrap=True)

    # ── Column widths ──
    for col_letter, width in [("A", 38), ("B", 18), ("C", 28), ("D", 18), ("E", 22), ("F", 18)]:
        ws.column_dimensions[col_letter].width = width




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

    # ── Conditional row colouring ─────────────────────────────────────────────
    gap_fill    = PatternFill("solid", fgColor="FEE2E2")   # light red — gaps
    hitl_fill   = PatternFill("solid", fgColor="FEF9C3")   # light yellow — HITL
    xsrc_fill   = PatternFill("solid", fgColor="EDE9FE")   # light purple — cross-source
    pass_fill   = PatternFill("solid", fgColor="D1FAE5")   # light green — covered/pass
    warn_fill   = PatternFill("solid", fgColor="FFF7ED")   # light orange — partial/review

    # Identify key column indices by header name for this sheet
    header_vals = {ws.cell(row=1, column=c).value: c for c in range(1, max_col + 1)}

    def _colour_row(row_idx, fill_obj):
        for c in range(1, max_col + 1):
            cell = ws.cell(row=row_idx, column=c)
            cell.fill = fill_obj

    for row_idx in range(2, max_row + 1):
        if sheet_name == "Traceability":
            cov_col = header_vals.get("Coverage_Status")
            if cov_col:
                val = str(ws.cell(row=row_idx, column=cov_col).value or "").strip()
                if val in ("Not Covered", "Missing FRS", "[GAP]"):
                    _colour_row(row_idx, gap_fill)
                elif val == "Partial":
                    _colour_row(row_idx, warn_fill)
                elif val == "Covered":
                    _colour_row(row_idx, pass_fill)

        elif sheet_name == "Gap_Analysis":
            sev_col = header_vals.get("Severity")
            if sev_col:
                sev = str(ws.cell(row=row_idx, column=sev_col).value or "").strip().lower()
                if sev == "critical":
                    _colour_row(row_idx, gap_fill)
                elif sev == "high":
                    _colour_row(row_idx, warn_fill)

        elif sheet_name == "Det_Validation":
            rule_col = header_vals.get("Rule")
            sev_col  = header_vals.get("Severity")
            if rule_col:
                rule = str(ws.cell(row=row_idx, column=rule_col).value or "").strip()
                if rule == "R6":
                    _colour_row(row_idx, hitl_fill)
                elif rule in ("R0", "R1"):
                    _colour_row(row_idx, gap_fill)

        elif sheet_name == "FRS":
            src_col = header_vals.get("Source_URS_Ref")
            cf_col  = header_vals.get("Confidence_Flag")
            if src_col:
                src = str(ws.cell(row=row_idx, column=src_col).value or "")
                if "User Guide Only" in src:
                    _colour_row(row_idx, xsrc_fill)
                elif "Cross-Source Gap" in str(ws.cell(row=row_idx, column=cf_col).value if cf_col else ""):
                    _colour_row(row_idx, xsrc_fill)

        elif sheet_name == "OQ":
            step_col = header_vals.get("Test_Step")
            if step_col:
                step = str(ws.cell(row=row_idx, column=step_col).value or "")
                if "HUMAN-IN-THE-LOOP" in step:
                    _colour_row(row_idx, hitl_fill)


def build_styled_excel(dataframes: dict, user: str = "", file_name: str = "",
                       model_name: str = "", sys_context_name: str = "",
                       dashboard_df=None) -> bytes:
    # ── Inject "DRAFT – AI Generated | Pending Human Review" status column ────
    # Satisfies document control lifecycle: AI output is never presented as Final.
    # Every data sheet gets AI_Review_Status = "DRAFT – AI Generated | Pending Review"
    # so reviewers and auditors can immediately see the document state.
    DRAFT_SHEETS = {"URS_Extraction", "FRS", "OQ", "Traceability", "Gap_Analysis", "Det_Validation"}
    stamped = {}
    for sheet_name, df in dataframes.items():
        if sheet_name in DRAFT_SHEETS and not df.empty:
            df = df.copy()
            df.insert(0, "AI_Review_Status", "DRAFT – AI Generated | Pending Review")
        stamped[sheet_name] = df

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in stamped.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        wb = writer.book
        for sheet_name in stamped:
            if sheet_name in wb.sheetnames:
                style_worksheet(wb[sheet_name], sheet_name)
                # ── Colour the AI_Review_Status column amber to draw the eye ──
                if sheet_name in DRAFT_SHEETS:
                    ws = wb[sheet_name]
                    hdr_vals = {ws.cell(row=1, column=c).value: c
                                for c in range(1, ws.max_column + 1)}
                    status_col = hdr_vals.get("AI_Review_Status")
                    if status_col:
                        amber_fill = PatternFill("solid", fgColor="FEF3C7")
                        amber_font = Font(bold=True, color="92400E", size=9)
                        for row_i in range(1, ws.max_row + 1):
                            cell = ws.cell(row=row_i, column=status_col)
                            cell.fill = amber_fill
                            if row_i > 1:
                                cell.font = amber_font
                        ws.column_dimensions[
                            get_column_letter(status_col)
                        ].width = 36
        # Add bar chart to Dashboard sheet
        if "Dashboard" in wb.sheetnames:
            _write_dashboard_chart(wb, wb["Dashboard"])
        # Add executive Summary cover sheet (inserted at position 0)
        _dash = dashboard_df if dashboard_df is not None else (
            dataframes.get("Dashboard", pd.DataFrame()))
        build_cover_sheet(wb, user=user, file_name=file_name,
                          model_name=model_name, dashboard_df=_dash,
                          sys_context_name=sys_context_name)
    return output.getvalue()


# =============================================================================
# 9. SESSION STATE
# =============================================================================

def get_auto_location():
    """
    IP-based geolocation is intentionally disabled.

    When Streamlit runs on any cloud host (Streamlit Cloud, AWS, GCP, Azure, etc.)
    ip-api.com resolves the *server's* datacenter IP — not the user's browser IP.
    This reliably returns the wrong region (e.g. Oregon when the user is in California).

    For GxP 21 CFR Part 11 compliance, a validator should DECLARE their location
    explicitly rather than have it guessed. Manual entry in the sidebar replaces
    auto-detection entirely.
    """
    return ""


_defaults = {
    "authenticated":      False,
    "selected_model":     "Gemini 1.5 Pro",
    "user_name":          "",
    "user_role":          "",
    "last_activity":      None,
    "sop_file_bytes":     None,
    "sop_file_name":      None,
    "sys_context_bytes":  None,
    "sys_context_name":   None,
    "uploader_key_n":     0,       # incremented to reset the URS file-uploader widget
    "sys_uploader_key_n": 0,       # incremented to reset the sidebar sys-context uploader
    "user_ip":            "",      # client IP stored as separate audit column (v29)
    "esig_pending":       None,    # holds completed analysis awaiting e-signature
    "pass2_chunk_size":   40,      # user-tunable batch size for Pass 2 (20/40/60)
    "show_esig_form":     False,   # True when user clicked a download button → show inline form
    "esig_target":        None,    # "xlsx" or "pdf" — which format triggered the e-sig form
    "app_mode":           "New Validation",   # sidebar mode selector
    # ── Change Impact Analysis slots ────────────────────────────────────────
    "cia_change_spec_bytes": None,
    "cia_change_spec_name":  None,
    "cia_frs_bytes":         None,
    "cia_frs_name":          None,
    "cia_oq_bytes":          None,
    "cia_oq_name":           None,
    "cia_trace_bytes":       None,
    "cia_trace_name":        None,
    "cia_result":            None,   # completed CIA output dict
    "cia_key_n":             0,      # increment to reset all CIA uploaders
    # ── Audit Trail Intelligence (Periodic Review Module 1) ──────────────────
    "at_raw_df":            None,
    "at_mapped_df":         None,
    "at_scored_df":         None,
    "at_top20_df":          None,
    "at_column_map":        {},
    "at_file_name":         "",
    "at_mapping_done":      False,
    "at_analysis_done":     False,
    "at_total_events":      0,
    "at_system_name":       "",
    "at_review_start":      "",
    "at_review_end":        "",
    "at_key_n":             0,      # increment to reset uploaders
    "pr_active_module":     None,   # which PR sub-module is open (None = landing)
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

    /* ── Pull "Auto-Generate" section up by one line ── */
    section[data-testid="stMain"] h1:first-of-type {
        margin-top: -1rem !important;
        padding-top: 0 !important;
    }

    /* ── Main URS file uploader — polished card ── */
    section[data-testid="stMain"] div[data-testid="stFileUploader"] {
        max-width: calc(100% - 54px) !important;
    }
    /* Outer drop-zone card */
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%) !important;
        border: 1.5px dashed #2563eb !important;
        border-radius: 14px !important;
        padding: 28px 32px !important;
        transition: border-color 0.2s, box-shadow 0.2s !important;
        box-shadow: 0 2px 16px rgba(37, 99, 235, 0.08) !important;
    }
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"]:hover {
        border-color: #3b82f6 !important;
        box-shadow: 0 4px 24px rgba(37, 99, 235, 0.18) !important;
    }
    /* "Drag and drop file here" primary text */
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] span[data-testid="stFileUploaderDropzoneInstructions"] > div > span:first-child {
        color: #e2e8f0 !important;
        font-size: 0.95rem !important;
        font-weight: 600 !important;
        letter-spacing: 0.01em !important;
    }
    /* "Limit 200MB per file • PDF" sub-text */
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] small,
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] span[data-testid="stFileUploaderDropzoneInstructions"] > div > small {
        color: #64748b !important;
        font-size: 0.78rem !important;
    }
    /* Upload cloud icon */
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] svg {
        fill: #2563eb !important;
        opacity: 0.85 !important;
    }
    /* "Browse files" button */
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] button[data-testid="baseButton-secondary"] {
        background: #1e40af !important;
        color: #e2e8f0 !important;
        border: 1px solid #2563eb !important;
        border-radius: 8px !important;
        font-size: 0.82rem !important;
        font-weight: 600 !important;
        padding: 5px 18px !important;
        transition: background 0.15s !important;
    }
    section[data-testid="stMain"] div[data-testid="stFileUploaderDropzone"] button[data-testid="baseButton-secondary"]:hover {
        background: #2563eb !important;
        color: white !important;
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
    .sb-filename        { color: #94d2f5 !important; font-weight: 400 !important; font-size: 0.80rem;
                          margin: 4px 0 0 0; word-break: break-word; }
    .system-spacer      { margin-top: 80px; }
    .sys-context-spacer { margin-top: 2.4rem; }
    .sidebar-stats      { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    /* ── Radio button labels white on dark sidebar ── */
    section[data-testid="stSidebar"] .stRadio label,
    section[data-testid="stSidebar"] .stRadio span,
    section[data-testid="stSidebar"] .stRadio p,
    section[data-testid="stSidebar"] .stRadio div,
    section[data-testid="stSidebar"] div[data-testid="stWidgetLabel"] p,
    section[data-testid="stSidebar"] div[data-baseweb="radio"] label,
    section[data-testid="stSidebar"] div[data-baseweb="radio"] span {
        color: white !important;
    }

    div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }

    /* ── New Analysis button — neutral grey, right-aligned with download ── */
    div.stButton > button[key="clear_results_btn"] {
        background: #334155 !important; color: #e2e8f0 !important;
        border: 1px solid #475569 !important; border-radius: 8px !important;
        height: 2.6rem !important; font-size: 0.88rem !important;
    }
    div.stButton > button[key="clear_results_btn"]:hover:not(:disabled) {
        background: #475569 !important; color: white !important;
        border-color: #64748b !important;
    }

    /* ── Sticky top-right terminate button overlay ── */
    #sticky-terminate-overlay {
        position: fixed !important;
        top: 10px !important;
        right: 20px !important;
        z-index: 999999 !important;
    }
    #sticky-terminate-overlay button {
        background: #dc2626 !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 6px 16px !important;
        font-size: 0.82rem !important;
        font-weight: 600 !important;
        cursor: pointer !important;
        box-shadow: 0 2px 8px rgba(220,38,38,0.4) !important;
        transition: background 0.15s !important;
    }
    #sticky-terminate-overlay button:hover {
        background: #b91c1c !important;
    }

    /* ── Completely hide the invisible trigger button and its wrapper ── */
    /* Streamlit Cloud doesn't expose key= as an HTML attribute, so target
       by multiple selectors to ensure it's invisible across all environments */
    button[data-testid="stButton"][key="terminate_hidden_trigger"],
    div[data-testid="stButton"]:has(button[key="terminate_hidden_trigger"]) {
        display: none !important;
        visibility: hidden !important;
        height: 0 !important;
        width: 0 !important;
        overflow: hidden !important;
        position: absolute !important;
        pointer-events: none !important;
    }

    /* ── E-Signature modal ── */
    .esig-container {
        background: #0f172a;
        border: 2px solid #2563eb;
        border-radius: 12px;
        padding: 28px 32px;
        margin: 24px 0;
        font-family: 'Inter', sans-serif;
    }
    .esig-title {
        color: #e2e8f0;
        font-size: 1.15rem;
        font-weight: 700;
        letter-spacing: 1px;
        margin: 0 0 4px 0;
    }
    .esig-subtitle {
        color: #64748b;
        font-size: 0.78rem;
        margin: 0 0 20px 0;
        font-style: italic;
    }
    .esig-field-label {
        color: #94a3b8;
        font-size: 0.80rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 14px 0 4px 0;
    }
    .esig-user-display {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 6px;
        padding: 8px 12px;
        color: #e2e8f0;
        font-size: 0.92rem;
        font-weight: 600;
    }
    .esig-warning {
        color: #fbbf24;
        font-size: 0.78rem;
        margin-top: 8px;
    }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro":    "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o":            "openai/gpt-4o",
    "Groq (Llama 3.3)":  "groq/llama-3.3-70b-versatile"
}


def _capture_client_ip():
    """
    Capture client IP via JS → query_params on first page load.
    Stores in st.session_state['user_ip'] which is then written as a
    separate column in audit_log (v29). This satisfies 21 CFR Part 11
    electronic-signature traceability without relying on cloud server IPs.
    """
    if st.session_state.get("user_ip"):
        return  # already captured this session

    # Check if JS already posted the IP via query param
    qp = st.query_params
    if "uip" in qp:
        st.session_state["user_ip"] = str(qp["uip"])[:100]
        return

    # Inject JS: fetch client IP from cloudflare trace (no CORS issues), write to query param
    _st_components.html("""
    <script>
    (function() {
      try {
        fetch('https://www.cloudflare.com/cdn-cgi/trace')
          .then(r => r.text())
          .then(txt => {
            const m = txt.match(/ip=([\\d\\.a-fA-F:]+)/);
            if (m) {
              const url = new URL(window.parent.location.href);
              url.searchParams.set('uip', m[1]);
              window.parent.history.replaceState({}, '', url.toString());
            }
          }).catch(() => {});
      } catch(e) {}
    })();
    </script>
    """, height=0)


# =============================================================================
# 10b. CHANGE IMPACT ANALYSIS — BACKEND
# =============================================================================

def detect_tabular_doc_type(df: pd.DataFrame) -> str:
    """
    Fingerprint an uploaded tabular document (xlsx/csv) by column headers
    and ID patterns.  Returns: 'FRS' | 'OQ' | 'Traceability' | 'URS' | 'Unknown'
    """
    if df is None or df.empty:
        return "Unknown"
    cols      = [str(c).strip().lower() for c in df.columns]
    first_col = df.iloc[:, 0].astype(str).str.strip()

    # Count ID pattern matches in first column
    # OQ pattern is broad — matches OQ-001, OQ-LAB-01, OQ-SYS-002 etc.
    frs_ids = first_col.str.match(r'^FRS-',  case=False).sum()
    oq_ids  = first_col.str.match(r'^OQ-',   case=False).sum()
    urs_ids = first_col.str.match(r'^URS-',  case=False).sum()

    # Also scan ALL columns for OQ-style IDs (traceability has OQ_ID as a non-first column)
    all_vals_oq = sum(
        df[col].astype(str).str.match(r'^OQ-', case=False).sum()
        for col in df.columns
    )

    has_test_step   = any(c in cols for c in ['test_step', 'expected_result', 'pass_fail_criteria'])
    has_req_desc    = any('requirement_description' in c or 'req_desc' in c for c in cols)
    has_coverage    = any('coverage_status' in c or 'coverage' in c for c in cols)
    has_frs_ref     = any('frs_ref' in c or 'frs_id' in c for c in cols)
    has_urs_ref     = any('urs_req_id' in c or 'urs_ref' in c or 'urs_id' in c for c in cols)

    # Test/OQ column detection — handles Test_ID, OQ_ID, and any column starting with "oq"
    has_oq_col      = any('test_id' in c or c == 'oq_id' or c.startswith('oq') for c in cols)

    # Traceability — unique signature: has URS ref + FRS ref + OQ/Test column together
    # OR has coverage_status alongside FRS reference
    # OR has all three ID types (URS, FRS, OQ) present across any columns
    has_all_three_id_types = (
        df.apply(lambda col: col.astype(str).str.match(r'^URS-', case=False).any()).any() and
        df.apply(lambda col: col.astype(str).str.match(r'^FRS-', case=False).any()).any() and
        df.apply(lambda col: col.astype(str).str.match(r'^OQ-',  case=False).any()).any()
    )

    if has_all_three_id_types:
        return "Traceability"
    if (has_urs_ref or urs_ids > 0) and (has_frs_ref or frs_ids > 0) and has_oq_col:
        return "Traceability"
    if has_coverage and has_frs_ref:
        return "Traceability"

    # OQ — test steps present, or first column is OQ-NNN IDs
    if has_test_step or oq_ids > 3 or all_vals_oq > 3:
        return "OQ"

    # FRS
    if (has_req_desc or frs_ids > 3) and not has_test_step:
        return "FRS"

    # URS — first column is URS-NNN IDs but NOT a traceability doc
    if urs_ids > 3:
        return "URS"

    return "Unknown"


def _load_tabular(file_bytes: bytes, file_name: str) -> pd.DataFrame:
    """Load xlsx or csv into a DataFrame regardless of extension."""
    try:
        if file_name.lower().endswith(".csv"):
            return pd.read_csv(io.BytesIO(file_bytes), dtype=str).fillna("")
        else:
            return pd.read_excel(io.BytesIO(file_bytes), dtype=str).fillna("")
    except Exception as e:
        st.error(f"⛔ Could not read {file_name}: {e}")
        return pd.DataFrame()


def _validate_cia_slot(file_bytes, file_name, expected_type: str, slot_label: str):
    """
    Run type detection on an uploaded tabular file and return (df, ok, message).
    For PDFs (change spec + FRS), skip tabular detection.
    """
    if file_bytes is None:
        return None, False, ""

    # PDF slots — change spec and FRS are PDFs, skip column fingerprinting
    if file_name.lower().endswith(".pdf"):
        return file_bytes, True, f"✅ **{slot_label}** — PDF loaded ({len(file_bytes)//1024} KB)"

    df = _load_tabular(file_bytes, file_name)
    if df.empty:
        return None, False, f"⛔ **{slot_label}** — file is empty or could not be read."

    detected = detect_tabular_doc_type(df)

    if detected == expected_type:
        row_count = len(df)
        first_id  = str(df.iloc[0, 0]) if not df.empty else "?"
        last_id   = str(df.iloc[-1, 0]) if not df.empty else "?"
        return df, True, (
            f"✅ **{slot_label}** — {detected} detected, "
            f"{row_count} rows ({first_id} → {last_id})"
        )
    elif detected == "Unknown":
        return df, True, (
            f"⚠️ **{slot_label}** — document type unclear. "
            f"Expected {expected_type}. Verify columns match before running."
        )
    else:
        return None, False, (
            f"⛔ **{slot_label}** — wrong document. "
            f"Detected **{detected}** but this slot expects **{expected_type}**. "
            f"Please upload your {expected_type} file here."
        )


def build_cia_pass1_prompt(change_spec_text: str) -> str:
    """Extract structured change table from change specification PDF."""
    return _PROMPT_CIA_PASS1_RAW.format(
        change_spec_text=change_spec_text[:6000]
    )


def build_cia_pass2_prompt(
    chg_csv: str,
    frs_text: str,
    oq_df: pd.DataFrame,
    trace_df: pd.DataFrame
) -> str:
    """
    Map each change to impact on existing FRS rows, OQ rows, and trace links.
    Uses the trace matrix to build the relationship graph first.
    """
    oq_summary  = oq_df[oq_df.columns[:5]].head(80).to_csv(index=False) if not oq_df.empty else "No OQ provided"
    trc_summary = trace_df.head(80).to_csv(index=False) if not trace_df.empty else "No traceability matrix provided"

    return _PROMPT_CIA_PASS2_RAW.format(
        chg_csv     = chg_csv,
        frs_text    = frs_text[:4000],
        oq_summary  = oq_summary,
        trc_summary = trc_summary,
    )


def run_cia_analysis(
    change_spec_bytes: bytes,
    frs_bytes: bytes,
    oq_df: pd.DataFrame,
    trace_df: pd.DataFrame,
    model_id: str,
    status_widget,
    progress_widget
) -> dict:
    """
    Full Change Impact Analysis pipeline.
    Returns dict with keys: chg_df, frs_impact_df, oq_impact_df, summary
    """
    from litellm import completion as _completion

    # Extract text from PDFs
    status_widget.text("📄 Extracting change specification text...")
    progress_widget.progress(0.1)
    chg_pages  = extract_pages(change_spec_bytes)
    chg_text   = "\n".join(chg_pages)

    frs_pages  = extract_pages(frs_bytes)
    frs_text   = "\n".join(frs_pages)

    # Pass 1 — extract structured change table
    status_widget.text("🔍 Pass 1 — Extracting structured change table from spec...")
    progress_widget.progress(0.25)
    p1_resp = _completion(
        model=model_id, stream=False, temperature=TEMPERATURE,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": build_cia_pass1_prompt(chg_text)}
        ]
    )
    raw_chg = p1_resp.choices[0].message.content or ""
    raw_chg = re.sub(r'^```[a-zA-Z]*\n?', '', raw_chg, flags=re.MULTILINE)
    raw_chg = re.sub(r'```\s*$', '', raw_chg, flags=re.MULTILINE).strip()
    chg_df  = _csv_to_df(raw_chg)

    if chg_df.empty:
        raise RuntimeError(
            "Pass 1 extracted zero changes from the change specification. "
            "Ensure the document describes specific system changes."
        )

    status_widget.text(f"✅ {len(chg_df)} changes extracted. Running impact mapping...")
    progress_widget.progress(0.5)

    # Pass 2 — impact mapping
    status_widget.text("🗺️ Pass 2 — Mapping changes to existing FRS and OQ rows...")
    p2_resp = _completion(
        model=model_id, stream=False, temperature=TEMPERATURE,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": build_cia_pass2_prompt(
                raw_chg, frs_text, oq_df, trace_df
            )}
        ]
    )
    raw_p2 = p2_resp.choices[0].message.content or ""
    raw_p2 = re.sub(r'^```[a-zA-Z]*\n?', '', raw_p2, flags=re.MULTILINE)
    raw_p2 = re.sub(r'```\s*$', '', raw_p2, flags=re.MULTILINE).strip()

    parts = [p.strip() for p in raw_p2.split("|||")]
    frs_impact_df = _csv_to_df(parts[0]) if len(parts) > 0 else pd.DataFrame()
    oq_impact_df  = _csv_to_df(parts[1]) if len(parts) > 1 else pd.DataFrame()

    # ── Trace-Propagated Impact — pandas merge approach ──────────────────────
    # Guarantees 100% compliance: even if the AI missed a linked OQ test,
    # Python will catch it by walking the trace matrix.
    #
    # Rules:
    #   FRS status Must_Update → linked OQ gets Needs_Review (unless already Must_Update)
    #   FRS status Obsolete    → linked OQ gets Needs_Review (unless already Must_Update)
    #   Already Must_Update OQ rows are never downgraded.
    # ─────────────────────────────────────────────────────────────────────────
    if not trace_df.empty and not frs_impact_df.empty:
        status_widget.text("🔗 Propagating impact through traceability matrix...")
        progress_widget.progress(0.75)

        # Step 1 — Detect column names flexibly (handles varied export formats)
        frs_col = next((c for c in trace_df.columns
                        if "frs" in c.lower() and "ref" not in c.lower().replace("frs_ref","x")), None)
        frs_col = frs_col or next((c for c in trace_df.columns if "frs" in c.lower()), None)
        oq_col  = next((c for c in trace_df.columns
                        if "test_id" in c.lower() or c.lower().startswith("oq")), None)

        if frs_col and oq_col:
            # Step 2 — Build a clean bridge: trace rows where FRS col is populated
            trace_bridge = (
                trace_df[[frs_col, oq_col]]
                .copy()
                .rename(columns={frs_col: "FRS_ID", oq_col: "OQ_ID"})
                .assign(
                    FRS_ID=lambda d: d["FRS_ID"].astype(str).str.strip(),
                    OQ_ID =lambda d: d["OQ_ID"].astype(str).str.strip(),
                )
                .query("FRS_ID != '' and OQ_ID != '' and FRS_ID != 'nan' and OQ_ID != 'nan'")
                .drop_duplicates()
            )

            # Step 3 — Find FRS rows that trigger propagation
            trigger_statuses = {"Must_Update", "Obsolete"}
            if "Impact_Status" in frs_impact_df.columns and "FRS_ID" in frs_impact_df.columns:
                triggered_frs = (
                    frs_impact_df[
                        frs_impact_df["Impact_Status"].astype(str).isin(trigger_statuses)
                    ][["FRS_ID", "Impact_Status"]]
                    .assign(FRS_ID=lambda d: d["FRS_ID"].astype(str).str.strip())
                    .drop_duplicates("FRS_ID")
                )
            else:
                triggered_frs = pd.DataFrame(columns=["FRS_ID", "Impact_Status"])

            if not triggered_frs.empty:
                # Step 4 — Merge: triggered FRS → trace bridge → OQ IDs
                # Result: every OQ linked to a triggered FRS with the FRS status attached
                propagated = (
                    triggered_frs
                    .merge(trace_bridge, on="FRS_ID", how="inner")
                    .rename(columns={"Impact_Status": "FRS_Status"})
                    [["OQ_ID", "FRS_ID", "FRS_Status"]]
                    .query("OQ_ID != 'NEW'")
                    .drop_duplicates("OQ_ID")   # one row per OQ (take first FRS if multiple)
                )

                if not propagated.empty:
                    # Step 5 — Determine which OQ IDs are already flagged by the AI
                    already_flagged_must = set()
                    already_flagged_any  = set()
                    if not oq_impact_df.empty and "OQ_ID" in oq_impact_df.columns:
                        already_flagged_any  = set(oq_impact_df["OQ_ID"].astype(str).str.strip())
                        already_flagged_must = set(
                            oq_impact_df[
                                oq_impact_df["Impact_Status"].astype(str) == "Must_Update"
                            ]["OQ_ID"].astype(str).str.strip()
                        )

                    # Step 6 — Build propagated rows, inheriting status from FRS
                    # Rule: Obsolete FRS → OQ becomes Obsolete (test no longer needed)
                    #        Must_Update FRS → OQ becomes Needs_Review (steps may be wrong)
                    #        Already Must_Update OQ → never downgraded
                    new_rows = []
                    for _, pr in propagated.iterrows():
                        oid      = pr["OQ_ID"]
                        frs_id   = pr["FRS_ID"]
                        frs_stat = pr["FRS_Status"]

                        # Derive the OQ status from the FRS status
                        propagated_status = (
                            "Obsolete"     if frs_stat == "Obsolete"    else
                            "Needs_Review"                               # Must_Update → Needs_Review
                        )

                        if oid in already_flagged_must:
                            # Already Must_Update — never downgrade, skip
                            continue

                        if oid in already_flagged_any:
                            # AI already flagged at some level — upgrade/set in place
                            mask = oq_impact_df["OQ_ID"].astype(str).str.strip() == oid
                            existing_status = oq_impact_df.loc[mask, "Impact_Status"].values
                            # Only upgrade, never downgrade
                            if len(existing_status) > 0 and existing_status[0] != "Obsolete":
                                oq_impact_df.loc[mask, "Impact_Status"]    = propagated_status
                            oq_impact_df.loc[mask, "Confidence_Level"] = "High"
                            oq_impact_df.loc[mask, "Change_Driver"]    = (
                                oq_impact_df.loc[mask, "Change_Driver"].astype(str)
                                + " + Trace-propagated"
                            )
                            oq_impact_df.loc[mask, "Rationale"] = (
                                f"System-flagged: Linked FRS [{frs_id}] is {frs_stat}. "
                                + (
                                    "This test case is no longer required for execution."
                                    if frs_stat == "Obsolete" else
                                    "Review test steps for continued validity against updated FRS."
                                )
                            )
                        else:
                            # Brand new row — AI did not flag this OQ at all
                            new_rows.append({
                                "OQ_ID":            oid,
                                "Change_Driver":    "Trace-propagated",
                                "Impact_Status":    propagated_status,
                                "Confidence_Level": "High",
                                "Risk_Category":    "GxP_Critical",
                                "Rationale":        (
                                    f"System-flagged: Linked FRS [{frs_id}] is {frs_stat}. "
                                    + (
                                        "This test case is no longer required for execution."
                                        if frs_stat == "Obsolete" else
                                        "Review test steps and pass/fail criteria for continued validity."
                                    )
                                ),
                                "Action_Required":  (
                                    "Retire this test case from the active test suite."
                                    if frs_stat == "Obsolete" else
                                    "Verify test steps and pass/fail criteria remain valid "
                                    "given the updated FRS requirement. Re-execute if affected."
                                ),
                            })

                    if new_rows:
                        oq_impact_df = pd.concat(
                            [oq_impact_df, pd.DataFrame(new_rows)],
                            ignore_index=True
                        )

                    status_widget.text(
                        f"🔗 Trace propagation complete — "
                        f"{len(new_rows)} new OQ rows added, "
                        f"{len(propagated) - len(new_rows)} existing rows upgraded."
                    )

    # ── Trace Coverage Verification ───────────────────────────────────────────
    # Compute what % of OQ tests in the uploaded OQ file appear in the trace matrix.
    # Orphan OQ tests (in OQ file but not in trace) indicate broken traceability.
    trace_coverage_pct  = 0
    orphan_oq_count     = 0
    trace_coverage_ok   = False
    if not oq_df.empty and not trace_df.empty:
        oq_col_trace = next((c for c in trace_df.columns if "test_id" in c.lower() or
                             c.lower().startswith("oq")), None)
        if oq_col_trace:
            all_oq_ids    = set(oq_df.iloc[:, 0].astype(str).str.strip())
            traced_oq_ids = set(trace_df[oq_col_trace].astype(str).str.strip())
            linked_count  = len(all_oq_ids & traced_oq_ids)
            orphan_oq_count = len(all_oq_ids - traced_oq_ids)
            trace_coverage_pct = round(linked_count / len(all_oq_ids) * 100, 1) if all_oq_ids else 0
            trace_coverage_ok  = trace_coverage_pct >= 90  # 90%+ = coverage intact

    # ── CIA Gap Detection — New_Required items with no test coverage ─────────
    # Any FRS or OQ row marked New_Required has no existing test coverage.
    # Flag these as Missing_Test gaps so the validator knows new tests are needed.
    cia_gap_rows = []
    if not frs_impact_df.empty and "Impact_Status" in frs_impact_df.columns:
        new_frs = frs_impact_df[
            frs_impact_df["Impact_Status"].astype(str) == "New_Required"
        ]
        for _, row in new_frs.iterrows():
            drv = str(row.get("Change_Driver", ""))
            cia_gap_rows.append({
                "Req_ID":         row.get("FRS_ID", "NEW"),
                "Gap_Type":       "Missing_Test",
                "Description":    (
                    f"New requirement from {drv} has no existing OQ test coverage. "
                    "This item did not exist in the previous validated baseline."
                ),
                "Recommendation": (
                    "Generate new OQ test cases covering positive, negative, and boundary "
                    "conditions before the next validation cycle sign-off."
                ),
                "Severity":       "High",
                "Change_Driver":  drv,
            })

    cia_gap_df = pd.DataFrame(cia_gap_rows) if cia_gap_rows else pd.DataFrame(
        columns=["Req_ID", "Gap_Type", "Description", "Recommendation",
                 "Severity", "Change_Driver"]
    )

    progress_widget.progress(1.0)
    status_widget.text("✅ Change impact analysis complete.")

    # Summary counts
    def _count(df, col, val):
        if df.empty or col not in df.columns:
            return 0
        return int((df[col].astype(str) == val).sum())

    summary = {
        "total_changes":       len(chg_df),
        "frs_must_update":     _count(frs_impact_df, "Impact_Status", "Must_Update"),
        "frs_needs_review":    _count(frs_impact_df, "Impact_Status", "Needs_Review"),
        "frs_obsolete":        _count(frs_impact_df, "Impact_Status", "Obsolete"),
        "frs_new":             _count(frs_impact_df, "Impact_Status", "New_Required"),
        "oq_must_update":      _count(oq_impact_df,  "Impact_Status", "Must_Update"),
        "oq_needs_review":     _count(oq_impact_df,  "Impact_Status", "Needs_Review"),
        "oq_obsolete":         _count(oq_impact_df,  "Impact_Status", "Obsolete"),
        "oq_new":              _count(oq_impact_df,  "Impact_Status", "New_Required"),
        "trace_coverage_pct":  trace_coverage_pct,
        "orphan_oq_count":     orphan_oq_count,
        "trace_coverage_ok":   trace_coverage_ok,
    }

    return {
        "chg_df":        chg_df,
        "frs_impact_df": frs_impact_df,
        "oq_impact_df":  oq_impact_df,
        "cia_gap_df":    cia_gap_df,
        "summary":       summary,
    }


def build_cia_excel(result: dict, user: str, file_name: str, model_name: str) -> bytes:
    """Build the Change Impact Analysis Excel workbook."""
    output = io.BytesIO()

    STATUS_COLORS = {
        "Must_Update":   "FEE2E2",   # red
        "Needs_Review":  "FEF9C3",   # yellow
        "Obsolete":      "E5E7EB",   # grey
        "New_Required":  "DBEAFE",   # blue
    }

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        # Sheet 1 — Change Table
        result["chg_df"].to_excel(writer, sheet_name="Changes", index=False)
        # Sheet 2 — FRS Impact
        result["frs_impact_df"].to_excel(writer, sheet_name="FRS_Impact", index=False)
        # Sheet 3 — OQ Impact
        result["oq_impact_df"].to_excel(writer, sheet_name="OQ_Impact", index=False)
        # Sheet 4 — Gaps (New_Required items with no test coverage)
        cia_gap_df = result.get("cia_gap_df", pd.DataFrame())
        if not cia_gap_df.empty:
            cia_gap_df.to_excel(writer, sheet_name="Gaps", index=False)
        else:
            pd.DataFrame({"Note": ["No gaps detected — all changes have existing test coverage."]
                         }).to_excel(writer, sheet_name="Gaps", index=False)

        wb = writer.book

        for sheet_name in ["Changes", "FRS_Impact", "OQ_Impact", "Gaps"]:
            if sheet_name not in wb.sheetnames:
                continue
            ws  = wb[sheet_name]
            hdr = Font(bold=True, color="FFFFFF", size=10)
            hf  = PatternFill("solid", fgColor="1E293B")
            thin = Side(style="thin", color="CBD5E1")
            bdr  = Border(left=thin, right=thin, top=thin, bottom=thin)

            for col in range(1, ws.max_column + 1):
                c = ws.cell(row=1, column=col)
                c.font, c.fill, c.border = hdr, hf, bdr
                c.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

            # Colour rows by Impact_Status
            if sheet_name in ("FRS_Impact", "OQ_Impact"):
                hdr_vals = {ws.cell(row=1, column=c).value: c
                            for c in range(1, ws.max_column + 1)}
                status_col = hdr_vals.get("Impact_Status")
                for row_i in range(2, ws.max_row + 1):
                    status = ws.cell(row=row_i, column=status_col).value if status_col else ""
                    fill_hex = STATUS_COLORS.get(str(status), "FFFFFF")
                    fill = PatternFill("solid", fgColor=fill_hex)
                    for col in range(1, ws.max_column + 1):
                        cell = ws.cell(row=row_i, column=col)
                        cell.fill   = fill
                        cell.border = bdr
                        cell.alignment = Alignment(vertical="top", wrap_text=True)

            ws.auto_filter.ref = ws.dimensions
            ws.freeze_panes    = "A2"
            ws.sheet_properties.tabColor = (
                "DC2626" if sheet_name == "FRS_Impact" else
                "D97706" if sheet_name == "OQ_Impact"  else
                "EA580C" if sheet_name == "Gaps"        else "1E3A5F"
            )
            for col in range(1, ws.max_column + 1):
                cl = get_column_letter(col)
                ws.column_dimensions[cl].width = min(
                    max(14, max(
                        (len(str(ws.cell(r, col).value or "")) for r in range(1, ws.max_row + 1)),
                        default=14
                    ) + 4), 60
                )

        # Summary sheet
        s = result["summary"]
        trc_ok     = s.get("trace_coverage_ok", False)
        trc_pct    = s.get("trace_coverage_pct", 0)
        orphan_cnt = s.get("orphan_oq_count", 0)
        trc_label  = (f"Yes — {trc_pct}% of OQ tests linked in trace matrix"
                      if trc_ok else
                      f"No — {trc_pct}% linked, {orphan_cnt} orphan OQ test(s) not in trace")

        summary_data = {
            "Metric": [
                "Total Changes Detected",
                "FRS — Must Update",           "FRS — Needs Review",
                "FRS — Obsolete",              "FRS — New Required",
                "OQ — Must Update",            "OQ — Needs Review",
                "OQ — Obsolete",               "OQ — New Required",
                "─── Trace Coverage ───",
                "Trace Coverage Verified",
                "Orphan OQ Tests (not in trace)",
                "─── Confidence Guide ───",
                "High Confidence",             "Medium Confidence",
                "Low Confidence",
                "─── Risk Category Guide ───",
                "GxP_Critical",                "Data_Integrity",
                "Business",                    "Cosmetic",
            ],
            "Count / Value": [
                s["total_changes"],
                s["frs_must_update"],  s["frs_needs_review"],
                s["frs_obsolete"],     s["frs_new"],
                s["oq_must_update"],   s["oq_needs_review"],
                s["oq_obsolete"],      s["oq_new"],
                "",
                trc_label,
                orphan_cnt,
                "",
                "Direct unambiguous match — act immediately",
                "Clear semantic link — engineering judgement required",
                "Weak/inferred — human must verify before acting",
                "",
                "Patient safety, e-records, audit trail, e-signatures",
                "Data accuracy, completeness, retention (ALCOA+)",
                "Operational workflows, non-GxP functionality",
                "UI labels, formatting, display only",
            ],
            "Action": [
                "Review full change table",
                "Update before next validation cycle", "Human review required",
                "Mark retired in document register",   "Generate new FRS rows",
                "Re-execute tests after update",       "Re-verify before sign-off",
                "Retire from test suite",              "Generate new OQ tests",
                "", "", "",                # Trace Coverage section (3 rows)
                "", "", "", "",            # Confidence Guide section (4 rows)
                "", "", "", "", "",        # Risk Category section (5 rows: header + 4 values)
            ]
        }
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name="Summary", index=False)
        ws_s = wb["Summary"]
        ws_s.sheet_properties.tabColor = "059669"

        # Style the summary sheet — highlight Trace Coverage Verified row
        for row_i in range(2, ws_s.max_row + 1):
            cell_val = str(ws_s.cell(row=row_i, column=1).value or "")
            if cell_val == "Trace Coverage Verified":
                fill_hex = "D1FAE5" if trc_ok else "FEE2E2"
                for col_i in range(1, 4):
                    ws_s.cell(row=row_i, column=col_i).fill = PatternFill("solid", fgColor=fill_hex)
                    ws_s.cell(row=row_i, column=col_i).font = Font(bold=True)
            elif cell_val.startswith("───"):
                for col_i in range(1, 4):
                    ws_s.cell(row=row_i, column=col_i).fill = PatternFill("solid", fgColor="1E293B")
                    ws_s.cell(row=row_i, column=col_i).font = Font(bold=True, color="FFFFFF")

        for col_i in range(1, 4):
            cl = get_column_letter(col_i)
            ws_s.column_dimensions[cl].width = 45 if col_i == 2 else 30

    return output.getvalue()


def show_change_impact(user: str, role: str, model_id: str):
    """Render the Change Impact Analysis main panel."""
    st.title("🔍 Change Impact Analysis")
    st.markdown(
        "<p style='color:#94a3b8;margin-top:-12px;'>Identify which existing FRS requirements "
        "and OQ test cases are affected by a system change, version upgrade, or configuration update.</p>",
        unsafe_allow_html=True
    )

    ck = st.session_state.get("cia_key_n", 0)

    # ── Step 1 — Change Specification ──────────────────────────────────────
    st.markdown("### Step 1 — Upload Change Specification")
    st.caption("Change Request, Release Notes, Configuration Change Notice, or mini-URS (PDF)")
    chg_widget = st.file_uploader(
        "Change Spec", type="pdf",
        key=f"cia_chg_{ck}", label_visibility="collapsed"
    )
    if chg_widget:
        ok, msg = validate_upload(chg_widget)
        if not ok:
            st.error(msg)
            st.session_state["cia_change_spec_bytes"] = None
            st.session_state["cia_change_spec_name"]  = None
        else:
            st.session_state["cia_change_spec_bytes"] = chg_widget.getvalue()
            st.session_state["cia_change_spec_name"]  = chg_widget.name
            st.success(f"✅ **Change Spec** — {chg_widget.name} loaded "
                       f"({len(chg_widget.getvalue())//1024} KB)")
    elif chg_widget is None:
        st.session_state["cia_change_spec_bytes"] = None
        st.session_state["cia_change_spec_name"]  = None

    st.markdown("---")

    # ── Step 2 — Existing Validated Documents ──────────────────────────────
    st.markdown("### Step 2 — Upload Existing Validated Documents")
    st.caption(
        "The Traceability Matrix is the **prerequisite** — it carries your URS requirement IDs "
        "and their links to FRS and OQ. The URS document itself is not required."
    )

    col_frs, col_oq = st.columns(2)
    col_trc, col_note = st.columns(2)

    with col_frs:
        st.caption("📄 Approved FRS (PDF)")
        frs_widget = st.file_uploader(
            "FRS", type=["pdf"],
            key=f"cia_frs_{ck}", label_visibility="collapsed"
        )
        if frs_widget:
            ok, msg = validate_upload(frs_widget)
            if not ok:
                st.error(msg)
                st.session_state["cia_frs_bytes"] = None
                st.session_state["cia_frs_name"]  = None
            else:
                st.session_state["cia_frs_bytes"] = frs_widget.getvalue()
                st.session_state["cia_frs_name"]  = frs_widget.name
                st.success(f"✅ FRS loaded — {frs_widget.name}")
        elif frs_widget is None:
            st.session_state["cia_frs_bytes"] = None
            st.session_state["cia_frs_name"]  = None

    with col_oq:
        st.caption("📊 OQ Test Cases (.xlsx or .csv from test engine)")
        oq_widget = st.file_uploader(
            "OQ", type=["xlsx", "xls", "csv"],
            key=f"cia_oq_{ck}", label_visibility="collapsed"
        )
        if oq_widget:
            oq_bytes = oq_widget.getvalue()
            oq_df_check = _load_tabular(oq_bytes, oq_widget.name)
            _, ok, msg = _validate_cia_slot(oq_bytes, oq_widget.name, "OQ", "OQ Test Cases")
            if not ok:
                st.error(msg)
                st.session_state["cia_oq_bytes"] = None
                st.session_state["cia_oq_name"]  = None
            else:
                st.markdown(msg)
                st.session_state["cia_oq_bytes"] = oq_bytes
                st.session_state["cia_oq_name"]  = oq_widget.name
        elif oq_widget is None:
            st.session_state["cia_oq_bytes"] = None
            st.session_state["cia_oq_name"]  = None

    with col_trc:
        st.caption("📊 Traceability Matrix (.xlsx or .csv)")
        trc_widget = st.file_uploader(
            "Trace", type=["xlsx", "xls", "csv"],
            key=f"cia_trc_{ck}", label_visibility="collapsed"
        )
        if trc_widget:
            trc_bytes = trc_widget.getvalue()
            _, ok, msg = _validate_cia_slot(trc_bytes, trc_widget.name,
                                            "Traceability", "Traceability Matrix")
            if not ok:
                st.error(msg)
                st.session_state["cia_trace_bytes"] = None
                st.session_state["cia_trace_name"]  = None
            else:
                st.markdown(msg)
                st.session_state["cia_trace_bytes"] = trc_bytes
                st.session_state["cia_trace_name"]  = trc_widget.name
        elif trc_widget is None:
            st.session_state["cia_trace_bytes"] = None
            st.session_state["cia_trace_name"]  = None

    with col_note:
        st.caption("ℹ️ Prerequisites")
        st.markdown(
            "<p style='color:#64748b;font-size:0.82rem;'>"
            "<b style='color:#e2e8f0;'>Traceability Matrix is required</b> — it contains your "
            "URS requirement IDs (URS-NNN) already linked to FRS and OQ rows. "
            "A separate URS upload is not needed.<br><br>"
            "FRS: approved baseline PDF from your document management system.<br><br>"
            "OQ and Traceability: export directly from your test management tool "
            "(Veeva Vault, TestRail, Jira, HP ALM, etc.).</p>",
            unsafe_allow_html=True
        )

    # ── Cross-slot consistency check ────────────────────────────────────────
    oq_bytes  = st.session_state.get("cia_oq_bytes")
    trc_bytes = st.session_state.get("cia_trace_bytes")
    if oq_bytes and trc_bytes:
        oq_df_v  = _load_tabular(oq_bytes,  st.session_state.get("cia_oq_name", ""))
        trc_df_v = _load_tabular(trc_bytes, st.session_state.get("cia_trace_name", ""))
        if not oq_df_v.empty and not trc_df_v.empty:
            oq_ids_set  = set(oq_df_v.iloc[:, 0].astype(str).str.strip())
            # Find OQ-like column in trace
            trc_oq_col  = next((c for c in trc_df_v.columns
                                if "test_id" in c.lower() or c.lower().startswith("oq")), None)
            if trc_oq_col:
                trc_oq_ids = set(trc_df_v[trc_oq_col].astype(str).str.strip())
                shared     = oq_ids_set & trc_oq_ids
                if len(shared) == 0 and len(oq_ids_set) > 0:
                    st.warning(
                        "⚠️ **Version mismatch** — no OQ IDs in the traceability matrix match "
                        "the uploaded OQ file. These files may be from different validation cycles. "
                        "Verify both files are from the same approved baseline."
                    )

    st.markdown("---")

    # ── Run button ──────────────────────────────────────────────────────────
    all_ready = all([
        st.session_state.get("cia_change_spec_bytes"),
        st.session_state.get("cia_frs_bytes"),
        st.session_state.get("cia_oq_bytes"),
        st.session_state.get("cia_trace_bytes"),
    ])

    _es, run_col, _es2 = st.columns([3, 4, 3])
    with run_col:
        run_cia = st.button(
            "🔍 Run Change Impact Analysis",
            key="run_cia_btn",
            disabled=not all_ready,
            use_container_width=True,
            type="primary"
        )

    if not all_ready:
        missing = [
            label for label, key in [
                ("Change Specification", "cia_change_spec_bytes"),
                ("FRS",                  "cia_frs_bytes"),
                ("OQ Test Cases",        "cia_oq_bytes"),
                ("Traceability Matrix",  "cia_trace_bytes"),
            ] if not st.session_state.get(key)
        ]
        st.info(f"📋 Still needed: {', '.join(missing)}")

    if run_cia:
        oq_df_run  = _load_tabular(
            st.session_state["cia_oq_bytes"],
            st.session_state["cia_oq_name"]
        )
        trc_df_run = _load_tabular(
            st.session_state["cia_trace_bytes"],
            st.session_state["cia_trace_name"]
        )
        progress_bar = st.progress(0)
        status_text  = st.empty()

        with st.status("🔍 Change Impact Analysis Pipeline", expanded=True) as cia_status:
            try:
                st.write("📄 Step 1: Extracting change specification...")
                st.write("🗺️ Step 2: Mapping changes to existing FRS requirements...")
                st.write("🧪 Step 3: Propagating impact through traceability matrix to OQ tests...")
                st.write("📊 Step 4: Compiling impact report...")

                result = run_cia_analysis(
                    change_spec_bytes = st.session_state["cia_change_spec_bytes"],
                    frs_bytes         = st.session_state["cia_frs_bytes"],
                    oq_df             = oq_df_run,
                    trace_df          = trc_df_run,
                    model_id          = model_id,
                    status_widget     = status_text,
                    progress_widget   = progress_bar,
                )
                cia_status.update(
                    label=f"✅ Complete — {result['summary']['total_changes']} changes, "
                          f"{result['summary']['frs_must_update'] + result['summary']['oq_must_update']} "
                          f"items require immediate action",
                    state="complete", expanded=False
                )
                log_audit(user, "CIA_COMPLETE", "CHANGE_IMPACT",
                          new_value=f"{result['summary']['total_changes']} changes detected",
                          reason=f"Model: {st.session_state.selected_model}")
                st.session_state["cia_result"] = result

            except Exception as e:
                cia_status.update(label="❌ Analysis failed", state="error")
                st.error(f"❌ {e}")
                log_audit(user, "CIA_ERROR", "CHANGE_IMPACT", reason=str(e)[:300])

        progress_bar.empty()
        status_text.empty()

    # ── Display results ──────────────────────────────────────────────────────
    cia_res = st.session_state.get("cia_result")
    if cia_res:
        s = cia_res["summary"]
        trc_ok     = s.get("trace_coverage_ok", False)
        trc_pct    = s.get("trace_coverage_pct", 0)
        orphan_cnt = s.get("orphan_oq_count", 0)

        # Hero metrics — 5 tiles
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("📋 Changes Detected",    s["total_changes"])
        m2.metric("🔴 FRS Must Update",     s["frs_must_update"])
        m3.metric("🔴 OQ Must Update",      s["oq_must_update"])
        m4.metric("🔵 New Items Required",  s["frs_new"] + s["oq_new"])
        m5.metric("🔗 Trace Coverage",      f"{trc_pct}%",
                  delta=f"{orphan_cnt} orphan OQ" if orphan_cnt > 0 else "intact",
                  delta_color="inverse" if orphan_cnt > 0 else "off")

        # Colour-coded hero cards
        _c1, _c2, _c3, _c4 = st.columns(4)
        for col, icon, label, frs_v, oq_v, color, bg in [
            (_c1, "🔴", "Must Update",   s["frs_must_update"],  s["oq_must_update"],  "#dc2626", "#1a0505"),
            (_c2, "🟡", "Needs Review",  s["frs_needs_review"], s["oq_needs_review"], "#d97706", "#1a1000"),
            (_c3, "🔵", "New Required",  s["frs_new"],          s["oq_new"],          "#2563eb", "#0f1a2e"),
        ]:
            col.markdown(f"""
<div style="background:{bg};border:2px solid {color};border-radius:10px;
            padding:14px 18px;text-align:center;font-family:'Inter',sans-serif;">
  <p style="margin:0;color:#94a3b8;font-size:0.7rem;letter-spacing:2px;
            text-transform:uppercase;">{label}</p>
  <p style="margin:4px 0 2px;font-size:1.6rem;font-weight:800;color:{color};">
    FRS: {frs_v} &nbsp;|&nbsp; OQ: {oq_v}</p>
  <span style="font-size:1.5rem;">{icon}</span>
</div>""", unsafe_allow_html=True)

        # Trace Coverage Verified card
        _trc_color = "#059669" if trc_ok else "#dc2626"
        _trc_bg    = "#052019"  if trc_ok else "#1a0505"
        _trc_icon  = "🟢" if trc_ok else "🔴"
        _trc_detail = (
            f"All but {orphan_cnt} OQ test(s) are linked in the traceability matrix. "
            "Repair orphan links before signing off the impact report."
            if orphan_cnt > 0 else
            "All OQ tests appear in the traceability matrix. Validation structure is intact."
        )
        _c4.markdown(f"""
<div style="background:{_trc_bg};border:2px solid {_trc_color};border-radius:10px;
            padding:14px 18px;text-align:center;font-family:'Inter',sans-serif;">
  <p style="margin:0;color:#94a3b8;font-size:0.7rem;letter-spacing:2px;
            text-transform:uppercase;">Trace Coverage</p>
  <p style="margin:4px 0 2px;font-size:1.6rem;font-weight:800;color:{_trc_color};">
    {trc_pct}% {_trc_icon}</p>
  <p style="margin:0;color:#94a3b8;font-size:0.72rem;">
    {'✅ Verified' if trc_ok else f'⚠️ {orphan_cnt} orphan OQ test(s)'}</p>
</div>""", unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Previews
        with st.expander("📋 Changes Extracted from Spec", expanded=True):
            st.dataframe(cia_res["chg_df"], use_container_width=True)

        STATUS_DISPLAY = {
            "Must_Update":  "🔴",
            "Needs_Review": "🟡",
            "Obsolete":     "⚫",
            "New_Required": "🔵",
        }

        with st.expander("📐 FRS Impact", expanded=True):
            frs_imp = cia_res["frs_impact_df"].copy()
            if not frs_imp.empty and "Impact_Status" in frs_imp.columns:
                frs_imp["Status"] = frs_imp["Impact_Status"].map(
                    lambda x: f"{STATUS_DISPLAY.get(x, '')} {x}"
                )
            st.dataframe(frs_imp, use_container_width=True)

        with st.expander("🧪 OQ Impact", expanded=True):
            oq_imp = cia_res["oq_impact_df"].copy()
            if not oq_imp.empty and "Impact_Status" in oq_imp.columns:
                oq_imp["Status"] = oq_imp["Impact_Status"].map(
                    lambda x: f"{STATUS_DISPLAY.get(x, '')} {x}"
                )
            st.dataframe(oq_imp, use_container_width=True)

        cia_gap_df = cia_res.get("cia_gap_df", pd.DataFrame())
        with st.expander(
            f"⚠️ Gaps — Missing Test Coverage ({len(cia_gap_df)} item(s))",
            expanded=len(cia_gap_df) > 0
        ):
            if not cia_gap_df.empty:
                st.dataframe(cia_gap_df, use_container_width=True)
            else:
                st.success("✅ No gaps detected — all changes have existing test coverage.")

        # Download
        st.markdown("---")
        xlsx_bytes = build_cia_excel(
            cia_res, user,
            st.session_state.get("cia_change_spec_name", "change_spec"),
            st.session_state.selected_model
        )
        dl1, _sp, clear_c = st.columns([5, 2, 2])
        with dl1:
            st.download_button(
                label="📥 Download Change Impact Report (.xlsx)",
                data=xlsx_bytes,
                file_name=f"CIA_{st.session_state.get('cia_change_spec_name','report').replace('.pdf','')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="cia_download_btn",
                use_container_width=True,
            )
        with clear_c:
            if st.button("🔄 New Analysis", key="cia_clear_btn", use_container_width=True):
                for k in ["cia_change_spec_bytes","cia_change_spec_name","cia_frs_bytes",
                          "cia_frs_name","cia_oq_bytes","cia_oq_name","cia_trace_bytes",
                          "cia_trace_name","cia_result"]:
                    st.session_state[k] = None
                st.session_state["cia_key_n"] = st.session_state.get("cia_key_n", 0) + 1
                st.rerun()




# =============================================================================
# 10c. PERIODIC REVIEW — MODULE 1: AUDIT TRAIL INTELLIGENCE
# =============================================================================

# ── Scoring constants ─────────────────────────────────────────────────────────
_AT_ADMIN_KW    = ["admin","administrator","sysadmin","system","service",
                   "root","superuser","power user","dba","sa"]
_AT_DELETE_KW   = ["delete","del","remove","purge","void","cancel","retire"]
_AT_MODIFY_KW   = ["modify","update","edit","change","amend","correct",
                   "override","revise","write"]
_AT_CREATE_KW   = ["create","add","insert","new","submit","approve",
                   "release","publish"]
_AT_SENSITIVE   = ["batch record","audit trail","electronic signature","esig",
                   "test result","clinical","raw data","master data",
                   "configuration","user account","role","permission",
                   "gxp","quality record","change control","deviation",
                   "capa","oos","oos result"]
_AT_AUDIT_CTRL  = ["audit trail","audit log","logging","log enabled","log disabled",
                   "audit enabled","audit disabled","configuration change","system setting"]
_AT_BIZ_START   = 7
_AT_BIZ_END     = 20
_AT_WEEKENDS    = {5, 6}
_AT_VEL_WINDOW  = 60    # minutes
_AT_VEL_THRESH  = 5     # same user+action in window = anomaly
_AT_TOP_N       = 20

_AT_REQUIRED_COLS = {
    "timestamp":   "Timestamp / Date-Time of the event",
    "user_id":     "User ID / Username who performed the action",
    "action_type": "Action / Event Type (e.g. Create, Modify, Delete, Insert)",
    "record_id":   "Record ID / Object ID affected (optional)",
    "record_type": "Record Type / Table Name (e.g. RESULTS, BATCH, SAMPLE_DATA)",
    "role":        "User Role / Permission Level (e.g. Admin, DBA, Analyst)",
    "comments":    "Comments / Rationale / Change Reason field (optional — Rule 1)",
    "new_value":   "New Value / Changed Value (optional — Rule 4 drift detection)",
}

# Vague rationale terms that trigger Rule 1
_AT_VAGUE_TERMS = {"fixed","update","updated","error","changed","change","test",
                   "misc","other","n/a","na","correction","corrected","edit","edited",
                   "modified","mod","ok","done","see above","as per","per request"}

# Tables that are GxP-critical for Rule 1, 3, 4
_AT_GXP_TABLES  = ["results","result","batch","batch_release","sample_data",
                   "sample","test_result","audit_trail","electronic_signature",
                   "quality_record","raw_data"]


def _at_temporal_score(ts) -> float:
    try:
        ts = pd.Timestamp(ts)
    except Exception:
        return 3.0
    if pd.isnull(ts):
        return 3.0
    score = 0.0
    if ts.weekday() in _AT_WEEKENDS:
        score += 5.0
    if ts.hour < _AT_BIZ_START or ts.hour >= _AT_BIZ_END:
        score += 4.0
    if 0 <= ts.hour < 5:
        score += 1.0
    return min(score, 10.0)


def _at_velocity_scores(df: pd.DataFrame) -> pd.Series:
    scores = pd.Series(0.0, index=df.index)
    if not all(c in df.columns for c in ["timestamp_parsed","user_id","action_type"]):
        return scores
    df_s   = df.sort_values("timestamp_parsed")
    ts_arr = df_s["timestamp_parsed"].values
    us_arr = df_s["user_id"].astype(str).values
    ac_arr = df_s["action_type"].astype(str).str.lower().values
    ix_arr = df_s.index.values
    window = pd.Timedelta(minutes=_AT_VEL_WINDOW)
    for i in range(len(df_s)):
        if pd.isnull(ts_arr[i]):
            continue
        count = 0
        for j in range(max(0,i-200), min(len(df_s),i+200)):
            if j == i or pd.isnull(ts_arr[j]):
                continue
            if abs((ts_arr[j]-ts_arr[i]) / np.timedelta64(1,'m')) <= _AT_VEL_WINDOW:
                if us_arr[j] == us_arr[i] and ac_arr[j] == ac_arr[i]:
                    count += 1
        if count >= _AT_VEL_THRESH:
            scores.at[ix_arr[i]] = min(count / _AT_VEL_THRESH * 3.5, 10.0)
    return scores


def _at_gap_scores(df: pd.DataFrame) -> pd.Series:
    scores = pd.Series(0.0, index=df.index)
    if "timestamp_parsed" not in df.columns:
        return scores
    ts   = df["timestamp_parsed"].sort_values()
    prev = ts.shift(1)
    gap  = (ts - prev).dt.total_seconds() / 3600
    scores.loc[gap[gap > 2].index] = 7.0
    return scores


def _at_del_recreate_scores(df: pd.DataFrame) -> pd.Series:
    scores = pd.Series(0.0, index=df.index)
    needed = ["record_id","user_id","action_type","timestamp_parsed"]
    if not all(c in df.columns for c in needed):
        return scores
    df2 = df[df["record_id"].astype(str).str.strip() != ""].copy()
    df2["_del"] = df2["action_type"].astype(str).str.lower().apply(
        lambda x: any(k in x for k in _AT_DELETE_KW))
    df2["_cre"] = df2["action_type"].astype(str).str.lower().apply(
        lambda x: any(k in x for k in _AT_CREATE_KW))
    dels = df2[df2["_del"]]
    cres = df2[df2["_cre"]]
    for _, dr in dels.iterrows():
        if pd.isnull(dr["timestamp_parsed"]):
            continue
        match = cres[
            (cres["record_id"] == dr["record_id"]) &
            (cres["user_id"]   == dr["user_id"]) &
            (cres["timestamp_parsed"] >= dr["timestamp_parsed"]) &
            (cres["timestamp_parsed"] <= dr["timestamp_parsed"] + pd.Timedelta(hours=4))
        ]
        if not match.empty:
            di = df2[(df2["record_id"]==dr["record_id"]) &
                     (df2["user_id"]==dr["user_id"]) &
                     (df2["_del"]) &
                     (df2["timestamp_parsed"]==dr["timestamp_parsed"])].index
            scores.loc[di]          = 9.0
            scores.loc[match.index] = 9.0
    return scores


def at_score_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score every event across the original 6 dimensions PLUS
    4 named AI Skill rules from the GxP Operational Anomaly Detection spec.
    Returns sorted DataFrame with individual dimension scores, rule flags,
    rationale strings, and composite Risk_Score.
    """
    df = df.copy()

    # ── Timestamp parsing ─────────────────────────────────────────────────────
    if "timestamp" in df.columns:
        df["timestamp_parsed"] = pd.to_datetime(
            df["timestamp"], errors="coerce", infer_datetime_format=True)
    else:
        df["timestamp_parsed"] = pd.NaT

    # ── Original 6 dimensions ─────────────────────────────────────────────────
    df["score_temporal"]     = df["timestamp_parsed"].apply(_at_temporal_score)
    df["score_velocity"]     = _at_velocity_scores(df)
    df["score_gap"]          = _at_gap_scores(df)
    df["score_del_recreate"] = _at_del_recreate_scores(df)

    def _priv(row):
        role = str(row.get("role","")).lower()
        rec  = str(row.get("record_type","")).lower()
        act  = str(row.get("action_type","")).lower()
        if any(k in role for k in _AT_ADMIN_KW) and any(k in rec for k in _AT_SENSITIVE):
            return 8.0
        if any(k in role for k in _AT_ADMIN_KW) and any(
                k in act for k in _AT_MODIFY_KW + _AT_DELETE_KW):
            return 7.0
        return 0.0
    df["score_privilege"] = df.apply(_priv, axis=1)

    def _rec(row):
        rec = str(row.get("record_type","")).lower()
        act = str(row.get("action_type","")).lower()
        combined = act + " " + rec
        if any(k in combined for k in _AT_AUDIT_CTRL):
            return 10.0
        if any(k in rec for k in _AT_SENSITIVE) and any(k in act for k in _AT_DELETE_KW):
            return 8.0
        if any(k in rec for k in _AT_SENSITIVE):
            return 5.0
        return 0.0
    df["score_record"] = df.apply(_rec, axis=1)

    # ── Rule 1 — Vague Rationale (Compliance Gap) ─────────────────────────────
    # Target: UPDATE on RESULTS or BATCH table with <3-word or non-descriptive comment
    # Risk: High
    def _rule1(row):
        act  = str(row.get("action_type","")).upper()
        tbl  = str(row.get("record_type","")).upper()
        cmt  = str(row.get("comments","")).strip().lower()
        tbl_hit = any(t in tbl for t in ["RESULTS","RESULT","BATCH"])
        if not tbl_hit:
            return 0.0, ""
        if "UPDATE" not in act and "MODIFY" not in act and "EDIT" not in act:
            return 0.0, ""
        # Check comment quality
        if not cmt or cmt in ("", "nan", "none"):
            return 8.0, (
                "Rule 1 — Vague Rationale [HIGH]: UPDATE on GxP table with no comment. "
                "21 CFR Part 211.68 and ALCOA+ require contemporaneous, attributable "
                "documentation for every data modification."
            )
        words = [w for w in cmt.split() if len(w) > 1]
        if len(words) < 3:
            return 8.0, (
                f"Rule 1 — Vague Rationale [HIGH]: UPDATE on GxP table with only "
                f"{len(words)} word(s) in comment ('{cmt}'). Fewer than 3 words is "
                "insufficient documentation per ALCOA+ Attributable and Legible principles."
            )
        if any(vague in cmt for vague in _AT_VAGUE_TERMS):
            matched = [v for v in _AT_VAGUE_TERMS if v in cmt]
            return 7.0, (
                f"Rule 1 — Vague Rationale [HIGH]: Non-descriptive comment "
                f"('{cmt}') contains prohibited vague term(s): {matched}. "
                "Comment lacks scientific justification required for GxP data modification "
                "per 21 CFR Part 211.68."
            )
        return 0.0, ""

    r1_scores   = []
    r1_rationale = []
    for _, row in df.iterrows():
        s, r = _rule1(row)
        r1_scores.append(s)
        r1_rationale.append(r)
    df["score_rule1_vague_rationale"] = r1_scores
    df["rule1_rationale"]             = r1_rationale

    # ── Rule 2 — Contemporaneous Burst (ALCOA Gap) ────────────────────────────
    # Target: >10 RESULT_INSERT actions within 15-minute window per user
    # Risk: Medium
    r2_scores    = pd.Series(0.0, index=df.index)
    r2_rationale = pd.Series("", index=df.index)
    if "timestamp_parsed" in df.columns and "user_id" in df.columns:
        df_s   = df.sort_values("timestamp_parsed")
        ts_arr = df_s["timestamp_parsed"].values
        us_arr = df_s["user_id"].astype(str).values
        ac_arr = df_s["action_type"].astype(str).str.upper().values
        ix_arr = df_s.index.values
        insert_kw = ["INSERT","RESULT_INSERT","CREATE","ADD"]
        for i in range(len(df_s)):
            if pd.isnull(ts_arr[i]):
                continue
            if not any(kw in ac_arr[i] for kw in insert_kw):
                continue
            count = 0
            for j in range(max(0,i-200), min(len(df_s),i+200)):
                if j==i or pd.isnull(ts_arr[j]):
                    continue
                if abs((ts_arr[j]-ts_arr[i]) / np.timedelta64(1,'m')) <= 15:
                    if us_arr[j]==us_arr[i] and any(kw in ac_arr[j] for kw in insert_kw):
                        count += 1
            if count > 10:
                r2_scores.at[ix_arr[i]]    = 6.0
                r2_rationale.at[ix_arr[i]] = (
                    f"Rule 2 — Contemporaneous Burst [MEDIUM]: {count+1} INSERT actions "
                    f"by user '{us_arr[i]}' within 15 minutes. Exceeds the 10-action "
                    "threshold indicating batch processing from memory or paper scraps "
                    "rather than real-time entry. Violates ALCOA+ Contemporaneous principle."
                )
    df["score_rule2_burst"]    = r2_scores
    df["rule2_rationale"]      = r2_rationale

    # ── Rule 3 — Admin/GxP Conflict (SoD Gap) ────────────────────────────────
    # Target: Admin or DBA performing INSERT or UPDATE on SAMPLE_DATA or BATCH_RELEASE
    # Risk: Critical
    def _rule3(row):
        role = str(row.get("role","")).upper()
        act  = str(row.get("action_type","")).upper()
        tbl  = str(row.get("record_type","")).upper()
        role_hit = any(r in role for r in ["ADMIN","DBA","ADMINISTRATOR","SYSADMIN"])
        act_hit  = any(a in act  for a in ["INSERT","UPDATE","CREATE","MODIFY"])
        tbl_hit  = any(t in tbl  for t in ["SAMPLE_DATA","SAMPLE","BATCH_RELEASE",
                                            "BATCH","RESULTS","RESULT"])
        if role_hit and act_hit and tbl_hit:
            return 10.0, (
                f"Rule 3 — Admin/GxP Conflict [CRITICAL]: Role '{row.get('role','')}' "
                f"performed {row.get('action_type','')} on production GxP table "
                f"'{row.get('record_type','')}'. Admins must maintain system configuration "
                "only — modifying production data violates Segregation of Duties and "
                "21 CFR Part 11 §11.10(d) access controls."
            )
        return 0.0, ""

    r3_scores    = []
    r3_rationale = []
    for _, row in df.iterrows():
        s, r = _rule3(row)
        r3_scores.append(s)
        r3_rationale.append(r)
    df["score_rule3_admin_conflict"] = r3_scores
    df["rule3_rationale"]            = r3_rationale

    # ── Rule 4 — Change Control Drift (Validation Gap) ────────────────────────
    # Target: new_value column present + deviation from expected patterns
    # Risk: High
    # Note: without an uploaded Change Request PDF, we detect numeric outliers
    # and flag any new_value that differs from the modal value for that record type
    def _rule4_scores(df: pd.DataFrame) -> tuple:
        scores    = pd.Series(0.0, index=df.index)
        rationale = pd.Series("", index=df.index)
        if "new_value" not in df.columns or "record_type" not in df.columns:
            return scores, rationale
        # Group by record_type, find modal new_value; flag deviations
        for rec_type, grp in df.groupby("record_type"):
            if len(grp) < 3:
                continue
            vals = grp["new_value"].astype(str).str.strip()
            # Try numeric deviation detection
            try:
                numeric_vals = pd.to_numeric(vals, errors="coerce").dropna()
                if len(numeric_vals) >= 3:
                    mean = numeric_vals.mean()
                    std  = numeric_vals.std()
                    if std > 0:
                        for idx in grp.index:
                            try:
                                v = float(df.at[idx,"new_value"])
                                z = abs(v - mean) / std
                                if z > 3.0:   # >3 std dev from mean for this record type
                                    scores.at[idx] = 8.0
                                    rationale.at[idx] = (
                                        f"Rule 4 — Change Control Drift [HIGH]: "
                                        f"new_value '{v}' deviates {z:.1f} standard deviations "
                                        f"from the expected range for '{rec_type}' records "
                                        f"(mean={mean:.2f}, σ={std:.2f}). "
                                        "May indicate manual override of a validated setpoint "
                                        "without Change Control per 21 CFR Part 820.70(b)."
                                    )
                            except (ValueError, TypeError):
                                pass
            except Exception:
                pass
        return scores, rationale

    r4_sc, r4_rat              = _rule4_scores(df)
    df["score_rule4_drift"]    = r4_sc
    df["rule4_rationale"]      = r4_rat

    # ── Composite Risk Score ──────────────────────────────────────────────────
    # Original 6 dimensions + 4 named rules, weighted
    weights = {
        "score_temporal":          0.07,
        "score_velocity":          0.10,
        "score_privilege":         0.12,
        "score_record":            0.11,
        "score_del_recreate":      0.10,
        "score_gap":               0.08,
        # Named AI skill rules get higher weight — they are explicit, named violations
        "score_rule1_vague_rationale": 0.10,
        "score_rule2_burst":           0.10,
        "score_rule3_admin_conflict":  0.14,   # Critical rule = highest weight
        "score_rule4_drift":           0.08,
    }
    df["Risk_Score"] = sum(df[c]*w for c,w in weights.items()).round(2)

    def _tier(s):
        if s >= 6: return "Critical"
        if s >= 4: return "High"
        if s >= 2: return "Medium"
        return "Low"
    df["Risk_Tier"] = df["Risk_Score"].apply(_tier)

    # ── Triggered Rules summary column ───────────────────────────────────────
    # Lists which named rules fired for each event — useful for the Excel output
    def _triggered(row):
        fired = []
        if row.get("score_rule1_vague_rationale", 0) > 0:
            fired.append("Rule 1 — Vague Rationale [HIGH]")
        if row.get("score_rule2_burst", 0) > 0:
            fired.append("Rule 2 — Contemporaneous Burst [MEDIUM]")
        if row.get("score_rule3_admin_conflict", 0) > 0:
            fired.append("Rule 3 — Admin/GxP Conflict [CRITICAL]")
        if row.get("score_rule4_drift", 0) > 0:
            fired.append("Rule 4 — Change Control Drift [HIGH]")
        return "; ".join(fired) if fired else ""
    df["Triggered_Rules"] = df.apply(_triggered, axis=1)

    # ── Combined rationale (all fired rules concatenated) ────────────────────
    def _combined_rat(row):
        parts = [r for r in [
            row.get("rule1_rationale",""),
            row.get("rule2_rationale",""),
            row.get("rule3_rationale",""),
            row.get("rule4_rationale",""),
        ] if r]
        return " | ".join(parts)
    df["Rule_Rationale"] = df.apply(_combined_rat, axis=1)

    return df.sort_values("Risk_Score", ascending=False).reset_index(drop=True)


def at_generate_justifications(top_df: pd.DataFrame, model_id: str) -> pd.DataFrame:
    """LLM writes a 3-sentence justification for each of the top events."""
    from litellm import completion as _comp
    justifications = []
    total = len(top_df)
    for rank, (_, row) in enumerate(top_df.iterrows(), 1):
        prompt = f"""You are a GxP audit specialist writing a Periodic Review Report.

Event #{rank}/{total} escalated from audit trail analysis:
  Timestamp:   {row.get('timestamp','Unknown')}
  User:        {row.get('user_id','Unknown')}
  Action:      {row.get('action_type','Unknown')}
  Record ID:   {row.get('record_id','Unknown')}
  Record Type: {row.get('record_type','Unknown')}
  Role:        {row.get('role','Unknown')}
  Comments:    {row.get('comments','—')}
  New Value:   {row.get('new_value','—')}
  Risk Score:  {row.get('Risk_Score',0):.1f}/10  ({row.get('Risk_Tier','Unknown')})

Named AI Skill Rules triggered: {row.get('Triggered_Rules','None')}
Rule rationale: {row.get('Rule_Rationale','')}

Dimension scores: Temporal={row.get('score_temporal',0):.1f}  Velocity={row.get('score_velocity',0):.1f}  Privilege={row.get('score_privilege',0):.1f}  Record={row.get('score_record',0):.1f}  Del-Recreate={row.get('score_del_recreate',0):.1f}  Gap={row.get('score_gap',0):.1f}
Named rules: Rule1={row.get('score_rule1_vague_rationale',0):.1f}  Rule2={row.get('score_rule2_burst',0):.1f}  Rule3={row.get('score_rule3_admin_conflict',0):.1f}  Rule4={row.get('score_rule4_drift',0):.1f}

Write exactly 3 sentences:
1. Which named rules or dimensions triggered and why they matter for this specific event
2. The exact compliance risk — cite the rule name (e.g. Rule 1 — Vague Rationale) AND
   the regulation (21 CFR Part 11 §11.10(e), §11.300, §11.10(d), 21 CFR Part 211.68,
   EU Annex 11 Clause 9, or ALCOA+ principle as appropriate to the rule triggered)
3. The precise action the reviewer must take

Professional GxP language. No bullets. No headers. Plain paragraph only."""
        try:
            resp = _comp(model=model_id, stream=False, temperature=0.2,
                         max_tokens=250,
                         messages=[
                             {"role":"system","content":
                              "GxP audit specialist. Concise. Cite regulations. Never vague."},
                             {"role":"user","content":prompt}
                         ])
            text = resp.choices[0].message.content.strip()
        except Exception as e:
            text = (f"Risk score {row.get('Risk_Score',0):.1f}/10. "
                    f"Manual review required per 21 CFR Part 11 §11.10(e). "
                    f"Error generating AI justification: {str(e)[:60]}")
        justifications.append(text)
    top_df = top_df.copy()
    top_df["AI_Justification"] = justifications
    return top_df


def at_build_excel(top_df, scored_df, system_name, r_start, r_end, fname) -> bytes:
    """Build 3-sheet evidence workbook for Section 9.1.6 of Periodic Review Report."""
    output = io.BytesIO()
    wb     = Workbook()
    navy   = "0A1628"
    thin   = Side(style="thin", color="1E3A5F")
    bdr    = Border(left=thin,right=thin,top=thin,bottom=thin)
    hdr_f  = Font(bold=True,color="FFFFFF",name="Calibri",size=10)
    hdr_fl = PatternFill("solid",fgColor=navy)
    TIER_COLORS = {
        "Critical":("450A0A","FCA5A5"),
        "High":    ("431407","FDBA74"),
        "Medium":  ("1A1A05","FDE68A"),
        "Low":     ("052019","6EE7B7"),
    }

    total      = len(scored_df)
    n_esc      = len(top_df)
    n_crit     = int((scored_df["Risk_Tier"]=="Critical").sum())
    n_high     = int((scored_df["Risk_Tier"]=="High").sum())
    n_med      = int((scored_df["Risk_Tier"]=="Medium").sum())
    n_low      = int((scored_df["Risk_Tier"]=="Low").sum())
    pct_clear  = round((total-n_esc)/total*100,1) if total>0 else 0

    # Sheet 1 — Summary
    ws = wb.active; ws.title = "Summary"
    ws.sheet_properties.tabColor = "059669"
    ws.column_dimensions["A"].width = 42
    ws.column_dimensions["B"].width = 55

    ws.merge_cells("A1:B1")
    t = ws.cell(row=1,column=1,value="AUDIT TRAIL INTELLIGENCE — EVIDENCE PACKAGE")
    t.font = Font(bold=True,color="38BDF8",name="Calibri",size=12)
    t.fill = PatternFill("solid",fgColor=navy)
    t.alignment = Alignment(horizontal="center")

    rows = [
        ("System Name",          system_name,      None,    True),
        ("Review Period",         f"{r_start} → {r_end}", None, False),
        ("Source File",           fname,            None,    False),
        ("Analysis Date",         str(datetime.date.today()), None, False),
        ("Regulatory Basis",      "21 CFR Part 11 §11.10(e); EU Annex 11 Clause 9", None, False),
        ("── Statistical Summary ──","",            navy,    False),
        ("Total Events Analysed", total,            None,    True),
        ("Events Auto-Cleared",   total-n_esc,      "052019",True),
        ("% Auto-Cleared",        f"{pct_clear}%",  "052019",True),
        ("Events Escalated",      n_esc,            None,    True),
        ("── Risk Distribution ──","",              navy,    False),
        ("Critical (≥6.0)",       n_crit,           "450A0A",True),
        ("High (4.0–5.9)",        n_high,           "431407",True),
        ("Medium (2.0–3.9)",      n_med,            "1A1A05",False),
        ("Low (<2.0)",            n_low,            "052019",False),
        ("── Reviewer Statement ──","",             navy,    False),
        ("Statement",
         f"AI-assisted audit trail review identified the {n_esc} highest-risk events "
         f"from {total:,} total entries using 6-dimension anomaly scoring. "
         f"{pct_clear}% auto-cleared as low risk. All {n_esc} events reviewed and "
         f"dispositioned per 21 CFR Part 11 §11.10(e) and EU Annex 11 Clause 9.",
         None, False),
    ]
    for i,(lbl,val,fill,bold) in enumerate(rows,2):
        c1 = ws.cell(row=i,column=1,value=lbl)
        c2 = ws.cell(row=i,column=2,value=val)
        c1.font = Font(color="94A3B8",name="Calibri",size=10)
        c2.font = Font(color="FFFFFF",bold=bold,name="Calibri",size=10)
        c1.border = c2.border = bdr
        if fill:
            f = PatternFill("solid",fgColor=fill)
            c1.fill = c2.fill = f

    # Sheet 2 — Top 20
    ws2 = wb.create_sheet("Top_20_Escalated")
    ws2.sheet_properties.tabColor = "DC2626"
    disp = ["Rank","Risk_Score","Risk_Tier","Triggered_Rules",
            "timestamp","user_id","action_type","record_id","record_type","role",
            "comments","new_value",
            "Rule_Rationale",
            "score_temporal","score_velocity","score_privilege",
            "score_record","score_del_recreate","score_gap",
            "score_rule1_vague_rationale","score_rule2_burst",
            "score_rule3_admin_conflict","score_rule4_drift",
            "AI_Justification","Reviewer_Disposition","Reviewer_Notes"]
    top_out = top_df.copy().reset_index(drop=True)
    top_out.insert(0,"Rank",range(1,len(top_out)+1))
    top_out["Reviewer_Disposition"] = "[ ] Justified  [ ] Escalate to CAPA  [ ] False Positive"
    top_out["Reviewer_Notes"]       = ""
    for ci,cn in enumerate(disp,1):
        c = ws2.cell(row=1,column=ci,value=cn.replace("_"," "))
        c.font=hdr_f; c.fill=hdr_fl; c.border=bdr
        c.alignment=Alignment(horizontal="center",wrap_text=True)
    for ri,(_, row) in enumerate(top_out.iterrows(),2):
        tier = str(row.get("Risk_Tier","Low"))
        bg,_ = TIER_COLORS.get(tier,(None,"FFFFFF"))
        rf   = PatternFill("solid",fgColor=bg) if bg else None
        for ci,cn in enumerate(disp,1):
            val = row.get(cn,"")
            if isinstance(val,float) and not pd.isnull(val):
                val = round(val,2)
            c = ws2.cell(row=ri,column=ci,value=val)
            c.border=bdr; c.font=Font(color="FFFFFF",name="Calibri",size=9)
            c.alignment=Alignment(vertical="top",wrap_text=True)
            if rf and cn not in ("AI_Justification","Reviewer_Disposition","Reviewer_Notes"):
                c.fill=rf
    col_widths = {"Rank":6,"Risk_Score":10,"Risk_Tier":12,
                  "Triggered_Rules":45,
                  "timestamp":20,"user_id":18,"action_type":18,
                  "record_id":18,"record_type":18,"role":16,
                  "comments":30,"new_value":18,
                  "Rule_Rationale":60,
                  "score_temporal":11,"score_velocity":11,
                  "score_privilege":11,"score_record":11,
                  "score_del_recreate":14,"score_gap":11,
                  "score_rule1_vague_rationale":14,"score_rule2_burst":12,
                  "score_rule3_admin_conflict":14,"score_rule4_drift":12,
                  "AI_Justification":60,
                  "Reviewer_Disposition":40,"Reviewer_Notes":30}
    for ci,cn in enumerate(disp,1):
        ws2.column_dimensions[get_column_letter(ci)].width = col_widths.get(cn,15)
    ws2.auto_filter.ref = ws2.dimensions
    ws2.freeze_panes    = "A2"

    # Sheet 3 — All scored
    ws3 = wb.create_sheet("All_Events_Scored")
    ws3.sheet_properties.tabColor = "1E3A5F"
    all_cols = [c for c in scored_df.columns if not c.startswith("_")]
    for ci,cn in enumerate(all_cols,1):
        c = ws3.cell(row=1,column=ci,value=cn.replace("_"," "))
        c.font=hdr_f; c.fill=hdr_fl; c.border=bdr
    for ri,(_, row) in enumerate(scored_df[all_cols].iterrows(),2):
        tier = str(row.get("Risk_Tier","Low"))
        bg,_ = TIER_COLORS.get(tier,(None,"FFFFFF"))
        rf   = PatternFill("solid",fgColor=bg) if (bg and tier in ("Critical","High")) else None
        for ci,cn in enumerate(all_cols,1):
            val = row.get(cn,"")
            if isinstance(val,float) and not pd.isnull(val): val=round(val,2)
            c = ws3.cell(row=ri,column=ci,value=val)
            c.border=bdr; c.font=Font(color="FFFFFF",name="Calibri",size=9)
            if rf: c.fill=rf
    for ci in range(1,len(all_cols)+1):
        ws3.column_dimensions[get_column_letter(ci)].width=18
    ws3.auto_filter.ref=ws3.dimensions; ws3.freeze_panes="A2"

    wb.save(output)
    return output.getvalue()


def show_periodic_review(user: str, role: str, model_id: str):
    """
    Periodic Review landing page — shows 3 module cards.
    Clicking a live module opens it; coming-soon modules show a placeholder.
    """
    active = st.session_state.get("pr_active_module")

    # ── If a sub-module is open, show it with a Back button ──────────────────
    if active == "audit_trail":
        bc, _ = st.columns([2, 8])
        with bc:
            if st.button("← Back to Periodic Review", key="pr_back_btn"):
                st.session_state["pr_active_module"] = None
                st.rerun()
        show_audit_trail(user, role, model_id)
        return

    if active in ("access_review", "report_drafter"):
        bc, _ = st.columns([2, 8])
        with bc:
            if st.button("← Back to Periodic Review", key="pr_back_btn2"):
                st.session_state["pr_active_module"] = None
                st.rerun()
        label = "User Access Review Intelligence" if active == "access_review" \
                else "Periodic Review Report Drafter"
        st.title(f"🚧 {label}")
        st.info("This module is coming soon. It will be available in the next release.")
        return

    # ── Landing page ──────────────────────────────────────────────────────────
    st.title("📋 Periodic Review")
    st.markdown(
        "<p style='color:#94a3b8;margin-top:-12px;'>Select a module below. "
        "Each module covers a mandatory section of your Periodic Review Report "
        "per 21 CFR Part 11, EU Annex 11, and SOP-418.</p>",
        unsafe_allow_html=True
    )
    st.markdown("<br>", unsafe_allow_html=True)

    # ── Module cards ──────────────────────────────────────────────────────────
    modules = [
        {
            "key":     "audit_trail",
            "number":  "Module 1",
            "title":   "Audit Trail Intelligence",
            "section": "SOP-418 §9.1.6 · 21 CFR Part 11 §11.10(e) · EU Annex 11 Cl. 9",
            "desc":    (
                "Reduce 10,000 audit log entries to the 20 highest-risk events. "
                "Scores every event across 6 risk dimensions — velocity bursts, "
                "off-hours activity, admin privilege abuse, delete-recreate patterns, "
                "audit trail gaps, and sensitive record manipulation. "
                "Outputs a signed evidence package ready to attach as Appendix to "
                "your Periodic Review Report."
            ),
            "bullets": [
                "🔴 Velocity bursts & bulk modifications",
                "🔴 Admin privilege on GxP records",
                "🔴 Audit trail disable / gap detection",
                "🟡 Off-hours & weekend activity",
                "🟡 Delete → Recreate same record",
            ],
            "status":  "live",
            "btn_label": "Launch Module 1 →",
            "color":   "#0284c7",
            "bg":      "#0c1f36",
            "border":  "#1e3a5f",
        },
        {
            "key":     "access_review",
            "number":  "Module 2",
            "title":   "User Access Review Intelligence",
            "section": "SOP-418 §9.1.5 · 21 CFR Part 11 §11.300",
            "desc":    (
                "Upload your user access list CSV. The tool flags dormant accounts, "
                "admin roles assigned to non-admin functions, accounts active before "
                "training was completed, delta from last approved review, and shared "
                "account fingerprinting across IP addresses."
            ),
            "bullets": [
                "🔴 Dormant accounts still active",
                "🔴 Admin roles on non-admin functions",
                "🔴 New accounts since last review",
                "🟡 Shared account / credential reuse",
                "🟡 Account created and used same day",
            ],
            "status":  "coming_soon",
            "btn_label": "Coming Soon",
            "color":   "#475569",
            "bg":      "#0a1628",
            "border":  "#1e293b",
        },
        {
            "key":     "report_drafter",
            "number":  "Module 3",
            "title":   "Periodic Review Report Drafter",
            "section": "SOP-418 §9.2 · All sections",
            "desc":    (
                "Upload your company's Periodic Review SOP and the outputs from "
                "Modules 1 and 2. The tool drafts a complete Periodic Review Report "
                "using your own company terminology, SOP references, and CAPA system "
                "name. Derives the fit-for-use conclusion from finding severity. "
                "Produces a Word document 70% ready for approval."
            ),
            "bullets": [
                "📄 Company-specific terminology extraction",
                "📄 Auto-populates §9.1.5 and §9.1.6 from modules",
                "📄 Derives Fit / Fit with Restrictions / Not Fit conclusion",
                "📄 CAPA recommendations in your SOP language",
                "📄 Word document output ready for circulation",
            ],
            "status":  "coming_soon",
            "btn_label": "Coming Soon",
            "color":   "#475569",
            "bg":      "#0a1628",
            "border":  "#1e293b",
        },
    ]

    for mod in modules:
        live = mod["status"] == "live"
        bullets_html = "".join(
            f'<li style="color:#94a3b8;font-size:0.82rem;margin-bottom:3px;">{b}</li>'
            for b in mod["bullets"]
        )
        st.markdown(f"""
<div style="background:{mod['bg']};border:1.5px solid {mod['border']};
            border-left:4px solid {mod['color']};border-radius:10px;
            padding:22px 26px;margin-bottom:16px;font-family:'Inter',sans-serif;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;">
    <div style="flex:1;">
      <p style="margin:0;color:{mod['color']};font-size:0.7rem;letter-spacing:3px;
                text-transform:uppercase;">{mod['number']}
        {'&nbsp;&nbsp;<span style="background:#059669;color:white;padding:1px 6px;border-radius:3px;font-size:0.65rem;">LIVE</span>' if live else '&nbsp;&nbsp;<span style="background:#1e293b;color:#64748b;padding:1px 6px;border-radius:3px;font-size:0.65rem;">COMING SOON</span>'}
      </p>
      <p style="margin:4px 0 2px;font-size:1.2rem;font-weight:700;
                color:{'#e2e8f0' if live else '#475569'};">{mod['title']}</p>
      <p style="margin:0 0 10px;color:#475569;font-size:0.72rem;
                font-family:'Courier New',monospace;">{mod['section']}</p>
      <p style="margin:0 0 10px;color:{'#94a3b8' if live else '#334155'};
                font-size:0.83rem;line-height:1.5;">{mod['desc']}</p>
      <ul style="margin:0;padding-left:16px;">{bullets_html}</ul>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

        if live:
            _, btn_col, _ = st.columns([6, 3, 1])
            with btn_col:
                if st.button(mod["btn_label"], key=f"pr_open_{mod['key']}",
                             type="primary", use_container_width=True):
                    st.session_state["pr_active_module"] = mod["key"]
                    st.rerun()
        st.markdown("", unsafe_allow_html=True)


def show_audit_trail(user: str, role: str, model_id: str):
    """Render Periodic Review — Module 1: Audit Trail Intelligence."""
    st.title("🔍 Audit Trail Intelligence")
    st.markdown(
        "<p style='color:#94a3b8;margin-top:-12px;'>Periodic Review Module 1 — "
        "Reduce 10,000 audit log entries to the 20 highest-risk events, "
        "with documented statistical justification for Section 9.1.6 of your "
        "Periodic Review Report.</p>",
        unsafe_allow_html=True
    )

    # Reset button
    rc, _ = st.columns([2, 8])
    with rc:
        if st.button("🔄 New Analysis", key="at_reset_btn"):
            for k in ["at_raw_df","at_mapped_df","at_scored_df","at_top20_df",
                      "at_file_name","at_mapping_done","at_analysis_done","at_total_events"]:
                st.session_state[k] = _defaults.get(k)
            st.session_state["at_key_n"] = st.session_state.get("at_key_n",0) + 1
            st.rerun()

    # ── System metadata (always shown) ───────────────────────────────────────
    mc1, mc2, mc3 = st.columns(3)
    with mc1:
        st.session_state["at_system_name"] = st.text_input(
            "System Name", value=st.session_state.get("at_system_name",""),
            placeholder="e.g. DocuSign Part 11", key="at_sysname")
    with mc2:
        st.session_state["at_review_start"] = st.text_input(
            "Review Period Start", value=st.session_state.get("at_review_start",""),
            placeholder="e.g. 01-Jan-2022", key="at_rstart")
    with mc3:
        st.session_state["at_review_end"] = st.text_input(
            "Review Period End", value=st.session_state.get("at_review_end",""),
            placeholder="e.g. 31-Dec-2024", key="at_rend")

    st.markdown("---")

    # ── STEP 1: Upload ────────────────────────────────────────────────────────
    if not st.session_state.get("at_mapping_done"):
        st.markdown("### Step 1 — Upload Audit Trail Export")
        st.caption(
            "CSV or Excel export from any GxP system — Veeva Vault, SAP, "
            "MasterControl, LIMS, or any custom system. No integration required."
        )
        ck = st.session_state.get("at_key_n", 0)
        uploaded = st.file_uploader(
            "Audit Trail", type=["csv","xlsx","xls"],
            key=f"at_upload_{ck}", label_visibility="collapsed"
        )
        if uploaded:
            try:
                raw = uploaded.getvalue()
                if uploaded.name.lower().endswith(".csv"):
                    df = pd.read_csv(io.BytesIO(raw), dtype=str,
                                     low_memory=False).fillna("")
                else:
                    df = pd.read_excel(io.BytesIO(raw), dtype=str).fillna("")
                st.session_state["at_raw_df"]   = df
                st.session_state["at_file_name"] = uploaded.name
                st.success(f"✅ **{uploaded.name}** — "
                           f"**{len(df):,} rows** × **{len(df.columns)} columns**")
                with st.expander("Preview (first 10 rows)", expanded=False):
                    st.dataframe(df.head(10), use_container_width=True)
            except Exception as e:
                st.error(f"⛔ Could not read file: {e}")

        # ── Column mapper ─────────────────────────────────────────────────────
        if st.session_state.get("at_raw_df") is not None:
            st.markdown("### Step 2 — Map Your Columns")
            st.caption("Match your file's column names to the required fields. "
                       "★ = required.")
            df      = st.session_state["at_raw_df"]
            avail   = ["(not in file)"] + list(df.columns)
            mapping = {}
            cols3   = st.columns(3)
            for i,(field,desc) in enumerate(_AT_REQUIRED_COLS.items()):
                req  = field in ("timestamp","user_id","action_type")
                auto = "(not in file)"
                for col in df.columns:
                    cl = col.lower().replace(" ","_").replace("-","_")
                    if field in cl or cl in field:
                        auto = col; break
                with cols3[i%3]:
                    st.caption(desc)
                    mapping[field] = st.selectbox(
                        f"{'★ ' if req else ''}{field.replace('_',' ').title()}",
                        avail,
                        index=avail.index(auto) if auto in avail else 0,
                        key=f"at_map_{field}"
                    )

            req_ok = all(
                mapping.get(f,"(not in file)") != "(not in file)"
                for f in ("timestamp","user_id","action_type")
            )
            if not req_ok:
                st.warning("⚠️ Map the three required fields to continue.")
            else:
                _, bc, _ = st.columns([3,4,3])
                with bc:
                    if st.button("✅ Confirm Mapping & Continue",
                                 type="primary", use_container_width=True,
                                 key="at_confirm_map"):
                        rename = {v:k for k,v in mapping.items()
                                  if v != "(not in file)"}
                        mdf = df.rename(columns=rename)
                        for c in _AT_REQUIRED_COLS:
                            if c not in mdf.columns:
                                mdf[c] = ""
                        st.session_state["at_mapped_df"]   = mdf
                        st.session_state["at_column_map"]  = mapping
                        st.session_state["at_mapping_done"] = True
                        st.rerun()

    # ── STEP 2: Run analysis ──────────────────────────────────────────────────
    elif not st.session_state.get("at_analysis_done"):
        df = st.session_state["at_mapped_df"]
        st.success(f"✅ Mapping confirmed — **{len(df):,} events** ready")

        st.markdown("### Step 3 — Run Analysis")
        st.markdown(f"""
<div style="background:#0f172a;border:1px solid #1e293b;border-radius:10px;
            padding:16px 20px;margin-bottom:16px;">
  <p style="color:#38bdf8;font-size:0.72rem;letter-spacing:2px;
            text-transform:uppercase;margin:0 0 10px 0;">8 Detection Rules Active</p>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:4px;font-size:0.82rem;
              color:#94a3b8;">
    <div>🔴 Velocity bursts (same user + action)</div>
    <div>🔴 Off-hours &amp; weekend activity</div>
    <div>🔴 Admin privilege on GxP records</div>
    <div>🔴 Audit trail disable/enable events</div>
    <div>🔴 Delete → Recreate same record</div>
    <div>🟡 Timestamp gap (disable window)</div>
    <div>🟡 Sensitive record deletions</div>
    <div>🟡 Bulk modification spikes</div>
  </div>
</div>""", unsafe_allow_html=True)

        _, rc2, _ = st.columns([2,6,2])
        with rc2:
            run = st.button(
                f"🚀 Analyse {len(df):,} Events → Generate Top {_AT_TOP_N} Risk Report",
                type="primary", use_container_width=True, key="at_run_btn"
            )

        if run:
            prog   = st.progress(0)
            status = st.empty()
            with st.status("🔍 Audit Trail Analysis", expanded=True) as atstat:
                st.write("📊 Step 1: Parsing timestamps...")
                prog.progress(0.15)
                scored = at_score_events(df)

                st.write(f"⚡ Step 2: Scoring {len(scored):,} events across 6 dimensions...")
                prog.progress(0.45)
                top20  = scored.head(_AT_TOP_N).copy()

                st.write(f"🤖 Step 3: AI justifications for top {_AT_TOP_N} events...")
                prog.progress(0.65)
                top20  = at_generate_justifications(top20, model_id)

                prog.progress(0.90)
                st.write("📋 Step 4: Building evidence package...")
                st.session_state["at_scored_df"]     = scored
                st.session_state["at_top20_df"]      = top20
                st.session_state["at_total_events"]  = len(scored)
                st.session_state["at_analysis_done"] = True

                n_crit = int((scored["Risk_Tier"]=="Critical").sum())
                prog.progress(1.0)
                log_audit(user,"AT_ANALYSIS_COMPLETE","AUDIT_TRAIL",
                          new_value=f"{len(scored)} events, {n_crit} critical",
                          reason=f"System: {st.session_state.get('at_system_name','?')}")
                atstat.update(
                    label=f"✅ {len(scored):,} events analysed — "
                          f"{_AT_TOP_N} escalated — {n_crit} critical",
                    state="complete", expanded=False
                )
            prog.empty(); status.empty()
            st.rerun()

    # ── STEP 3: Results ───────────────────────────────────────────────────────
    else:
        scored = st.session_state["at_scored_df"]
        top20  = st.session_state["at_top20_df"]
        n_total = st.session_state["at_total_events"]
        n_esc   = len(top20)
        n_crit  = int((scored["Risk_Tier"]=="Critical").sum())
        n_high  = int((scored["Risk_Tier"]=="High").sum())
        n_med   = int((scored["Risk_Tier"]=="Medium").sum())
        n_low   = int((scored["Risk_Tier"]=="Low").sum())
        pct_clr = round((n_total-n_esc)/n_total*100,1) if n_total>0 else 0

        # Hero banner
        st.markdown(f"""
<div style="background:#0f172a;border:2px solid #38bdf8;border-radius:10px;
            padding:18px 24px;margin-bottom:18px;">
  <p style="margin:0;color:#475569;font-size:0.68rem;letter-spacing:3px;
            text-transform:uppercase;font-family:'Inter',sans-serif;">
    Analysis Complete — {st.session_state.get('at_system_name','System')}</p>
  <p style="margin:6px 0 4px;font-size:2.2rem;font-weight:800;color:#38bdf8;
            line-height:1;font-family:'Inter',sans-serif;">
    {n_total:,} events analysed</p>
  <p style="margin:0;font-size:0.88rem;color:#64748b;">
    <span style="color:#4ade80;font-weight:700;">{pct_clr}% auto-cleared</span>
    &nbsp;·&nbsp;
    <span style="color:#fca5a5;font-weight:700;">{n_esc} escalated for review</span>
    &nbsp;·&nbsp;
    <span style="color:#94a3b8;">{st.session_state.get('at_file_name','')}</span>
  </p>
</div>""", unsafe_allow_html=True)

        # Metrics
        c1,c2,c3,c4,c5 = st.columns(5)
        for col,val,label,color in [
            (c1, f"{n_total:,}", "Total Events",  "#38bdf8"),
            (c2, str(n_crit),   "🔴 Critical",    "#dc2626"),
            (c3, str(n_high),   "🟠 High",        "#ea580c"),
            (c4, str(n_med),    "🟡 Medium",      "#d97706"),
            (c5, f"{pct_clr}%", "✅ Auto-Cleared","#4ade80"),
        ]:
            col.metric(label, val)

        st.markdown("<br>", unsafe_allow_html=True)

        # Top 20 cards
        st.markdown(f"### Top {_AT_TOP_N} Highest-Risk Events")
        tier_colors = {
            "Critical":"#dc2626","High":"#ea580c",
            "Medium":"#d97706","Low":"#4ade80"
        }
        for rank,(_, row) in enumerate(top20.iterrows(),1):
            tier  = str(row.get("Risk_Tier","Medium"))
            score = float(row.get("Risk_Score",0))
            bc    = tier_colors.get(tier,"#d97706")
            triggered = str(row.get("Triggered_Rules",""))
            rule_rat  = str(row.get("Rule_Rationale",""))

            dims  = [
                ("Temporal",     row.get("score_temporal",0)),
                ("Velocity",     row.get("score_velocity",0)),
                ("Privilege",    row.get("score_privilege",0)),
                ("Record Type",  row.get("score_record",0)),
                ("Del-Recreate", row.get("score_del_recreate",0)),
                ("Gap",          row.get("score_gap",0)),
                ("Rule 1 Vague", row.get("score_rule1_vague_rationale",0)),
                ("Rule 2 Burst", row.get("score_rule2_burst",0)),
                ("Rule 3 Admin", row.get("score_rule3_admin_conflict",0)),
                ("Rule 4 Drift", row.get("score_rule4_drift",0)),
            ]
            dim_html = "".join([
                f'<div style="display:flex;align-items:center;gap:6px;margin-bottom:2px;">'
                f'<span style="color:{"#fbbf24" if "Rule" in dn else "#475569"};'
                f'font-size:0.67rem;width:78px;">{dn}</span>'
                f'<div style="flex:1;background:#1e293b;border-radius:2px;height:4px;">'
                f'<div style="background:{"#dc2626" if dv>=7 else "#ea580c" if dv>=5 else "#d97706" if dv>=3 else "#334155"};'
                f'height:4px;border-radius:2px;width:{min(dv/10*100,100):.0f}%;"></div></div>'
                f'<span style="color:#64748b;font-size:0.67rem;width:24px;text-align:right;">{dv:.1f}</span>'
                f'</div>'
                for dn,dv in dims
            ])

            # Triggered rules badges
            badges_html = ""
            if triggered:
                rule_badge_colors = {
                    "Rule 1": ("#7c3aed","#ede9fe"),
                    "Rule 2": ("#0369a1","#dbeafe"),
                    "Rule 3": ("#dc2626","#fee2e2"),
                    "Rule 4": ("#d97706","#fef3c7"),
                }
                for rule_label in triggered.split("; "):
                    key = rule_label[:6]
                    fg, bg2 = rule_badge_colors.get(key, ("#64748b","#1e293b"))
                    badges_html += (
                        f'<span style="background:{bg2};color:{fg};border:1px solid {fg}44;'
                        f'padding:2px 8px;border-radius:4px;font-size:0.68rem;'
                        f'margin-right:4px;margin-bottom:4px;display:inline-block;">'
                        f'{rule_label}</span>'
                    )

            st.markdown(f"""
<div style="background:#0f172a;border-left:3px solid {bc};border-radius:6px;
            padding:14px 18px;margin-bottom:8px;">
  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
    <span style="color:{bc};font-weight:700;">Event #{rank}
      <span style="background:{bc}22;border:1px solid {bc}44;color:{bc};
             padding:2px 8px;border-radius:4px;font-size:0.7rem;margin-left:8px;">
        {tier} · {score:.1f}/10</span></span>
    <span style="color:#334155;font-size:0.72rem;">
      {str(row.get('timestamp',''))}</span>
  </div>
  {f'<div style="margin-bottom:8px;">{badges_html}</div>' if badges_html else ''}
  <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:6px;
              font-size:0.8rem;margin-bottom:10px;">
    <div><span style="color:#475569;">User: </span>
         <span style="color:#e2e8f0;">{row.get('user_id','—')}</span></div>
    <div><span style="color:#475569;">Action: </span>
         <span style="color:#e2e8f0;">{row.get('action_type','—')}</span></div>
    <div><span style="color:#475569;">Role: </span>
         <span style="color:#e2e8f0;">{row.get('role','—')}</span></div>
    <div><span style="color:#475569;">Record ID: </span>
         <span style="color:#e2e8f0;">{row.get('record_id','—')}</span></div>
    <div><span style="color:#475569;">Record Type: </span>
         <span style="color:#e2e8f0;">{row.get('record_type','—')}</span></div>
    <div><span style="color:#475569;">Comment: </span>
         <span style="color:{'#fbbf24' if row.get('score_rule1_vague_rationale',0)>0 else '#e2e8f0'};">
           {str(row.get('comments','—'))[:60]}</span></div>
  </div>
  {f'<div style="background:#1a0f2e;border:1px solid #7c3aed44;border-radius:4px;padding:8px 12px;margin-bottom:10px;"><p style="color:#475569;font-size:0.67rem;text-transform:uppercase;letter-spacing:1px;margin:0 0 4px;">Rule Rationale</p><p style="color:#c4b5fd;font-size:0.79rem;line-height:1.4;margin:0;">{rule_rat[:300]}</p></div>' if rule_rat else ''}
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;">
    <div><p style="color:#475569;font-size:0.67rem;text-transform:uppercase;
             letter-spacing:1px;margin:0 0 5px;">Dimension Scores</p>
         {dim_html}</div>
    <div><p style="color:#475569;font-size:0.67rem;text-transform:uppercase;
             letter-spacing:1px;margin:0 0 5px;">AI Justification</p>
         <p style="color:#cbd5e1;font-size:0.81rem;line-height:1.5;margin:0;">
           {str(row.get('AI_Justification',''))}</p></div>
  </div>
</div>""", unsafe_allow_html=True)

        # Distribution table
        st.markdown("### Risk Distribution — Full Dataset")
        st.dataframe(pd.DataFrame({
            "Risk Tier":  ["Critical","High","Medium","Low"],
            "Count":      [n_crit,n_high,n_med,n_low],
            "% of Total": [round(v/n_total*100,1) for v in [n_crit,n_high,n_med,n_low]],
            "Escalated":  ["Yes","Yes" if n_high>0 else "No","No","No"],
        }), use_container_width=True, hide_index=True)

        # Download
        st.markdown("---")
        xlsx = at_build_excel(
            top20, scored,
            st.session_state.get("at_system_name","System"),
            st.session_state.get("at_review_start",""),
            st.session_state.get("at_review_end",""),
            st.session_state.get("at_file_name",""),
        )
        dl_c, inf_c = st.columns([4,5])
        with dl_c:
            st.download_button(
                "📥 Download Evidence Package (.xlsx)",
                data=xlsx,
                file_name=(f"AuditTrail_{st.session_state.get('at_system_name','System').replace(' ','_')}"
                           f"_{datetime.date.today()}.xlsx"),
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="at_download_btn", use_container_width=True,
            )
        with inf_c:
            st.markdown(
                "<p style='color:#475569;font-size:0.8rem;padding-top:8px;'>"
                "3-sheet workbook: <b style='color:#e2e8f0;'>Summary</b> · "
                "<b style='color:#e2e8f0;'>Top_20_Escalated</b> (reviewer "
                "disposition columns) · <b style='color:#e2e8f0;'>All_Events_Scored</b>"
                "<br>Attach to Periodic Review Report Section 9.1.6 as Appendix.</p>",
                unsafe_allow_html=True
            )

        st.markdown(f"""
<div style="background:#0f172a;border:1px solid #1e293b;border-radius:8px;
            padding:14px 20px;margin-top:14px;font-size:0.8rem;">
  <b style="color:#94a3b8;">Paste into Periodic Review Report Section 9.1.6:</b><br>
  <i style="color:#cbd5e1;">
  "AI-assisted audit trail review identified the {n_esc} highest-risk events from
  {n_total:,} total entries using 6-dimension anomaly scoring (Temporal, Velocity,
  Privilege, Record Sensitivity, Delete-Recreate, Gap Detection). {pct_clr}% of
  events were auto-cleared as low risk. All {n_esc} escalated events were reviewed
  by the undersigned and dispositioned as documented in Appendix A.
  Complies with 21 CFR Part 11 §11.10(e) and EU Annex 11 Clause 9."
  </i>
</div>""", unsafe_allow_html=True)


# =============================================================================
# 11. LOGIN
# =============================================================================

def show_login():
    # Disable browser autocomplete / password-save on all password inputs
    _inject_password_security()

    # Remove the sticky End Session button injected by show_app() into the
    # parent document body — Streamlit cannot clean up manually-injected DOM
    # elements on rerun, so we do it explicitly here on every login page render.
    _st_components.html("""
    <script>
    (function() {
        var btn = window.parent.document.getElementById('sticky-terminate-btn');
        if (btn) btn.parentNode.removeChild(btn);
    })();
    </script>
    """, height=0)

    left_space, center_content, right_space = st.columns([3, 4, 3])
    with center_content:
        st.markdown(
            '<div class="top-banner"><p class="banner-text-inner">GxP Validation — CSV Accelerator</p></div>',
            unsafe_allow_html=True
        )
        st.markdown("<br>", unsafe_allow_html=True)
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed",
                          key="login_username_field")
        p = st.text_input("Security Token", type="password", placeholder="Password",
                          label_visibility="collapsed", key="login_password_field")
        st.markdown("<br>", unsafe_allow_html=True)
        b_left, b_center, b_right = st.columns([1, 2, 1])
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                u_clean = sanitize_input(u, max_length=64)
                p_clean = sanitize_input(p, max_length=256)
                success, err_msg = authenticate_user(u_clean, p_clean)
                if success:
                    st.session_state.user_name     = u_clean
                    st.session_state.user_role     = get_user_role(u_clean)
                    st.session_state.authenticated = True
                    st.session_state.last_activity = datetime.datetime.utcnow()
                    log_audit(u_clean, "LOGIN_SUCCESS", "SESSION",
                              new_value=f"Role: {st.session_state.user_role}")
                    st.rerun()
                else:
                    st.error(err_msg or "Invalid credentials.")

        # ── Branding footer — small, italic, grey ───────────────────────────
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown(
            "<p style='text-align:center; font-style:italic; font-size:0.78rem; color:#94a3b8;'>"
          # ── MANUAL EDIT v29-custom — DO NOT OVERWRITE ──────────────
            "LLM-Powered GxP Validation</p>"
            "<p style='text-align:center; font-size:0.72rem; color:#b0bec5; margin-top:-6px;'>"
            "AI-driven validation accelerator that generates FRS, OQ tests, and Traceability matrices from URS and system documentation, while detecting validation gaps. "
            "</p>",
            unsafe_allow_html=True
          # ── END MANUAL EDIT ────────────────────────────────────────
        )


# =============================================================================
# 12. MAIN APPLICATION
# =============================================================================

def show_app():
    # Disable browser autocomplete / password-save on all password inputs (incl. admin panel)
    _inject_password_security()

    # Session timeout enforcement
    if not check_session_timeout():
        user = st.session_state.get("user_name", "unknown")
        log_audit(user, "SESSION_TIMEOUT", "SESSION",
                  reason=f"Inactivity exceeded {SESSION_TIMEOUT_MINUTES} min")
        st.session_state.clear()   # wipe everything — no stale results on re-login
        st.warning("⏱️ Session expired due to inactivity. Please log in again.")
        st.rerun()

    touch_session()

    user = st.session_state.get("user_name", "unknown")
    role = st.session_state.get("user_role", "Validator")

    # ── Sidebar ──
    with st.sidebar:
        st.markdown(f'<p class="sb-title">CSV Generator v{VERSION}</p>', unsafe_allow_html=True)
        st.divider()
        st.markdown('<p class="sb-sub">🔧 Analysis Mode</p>', unsafe_allow_html=True)
        _modes = [
            "New Validation",
            "Change Impact Analysis",
            "Periodic Review",
            "Delta Generation (coming soon)",
        ]
        app_mode = st.radio(
            "Mode", _modes,
            index=_modes.index(st.session_state.get("app_mode","New Validation"))
                  if st.session_state.get("app_mode","New Validation") in _modes else 0,
            label_visibility="collapsed",
            key="app_mode_radio",
        )
        st.session_state["app_mode"] = app_mode
        st.divider()
        st.markdown('<p class="sb-sub">🤖 AI Model</p>', unsafe_allow_html=True)

        engine_name = st.radio(
            "Model",
            list(MODELS.keys()),
            index=list(MODELS.keys()).index(st.session_state.selected_model),
            label_visibility="collapsed",
            key="model_radio"
        )
      
        # ── MANUAL EDIT v29-custom — DO NOT OVERWRITE ──────────────
        st.sidebar.markdown("<br><br>", unsafe_allow_html=True)
        st.session_state.selected_model = engine_name

        st.markdown('<p class="sb-sub">⚙️ Batch Size (reqs/pass)</p>', unsafe_allow_html=True)
        st.session_state["pass2_chunk_size"] = st.select_slider(
            "Batch size",
            options=[20, 40, 60],
            value=st.session_state.get("pass2_chunk_size", 40),
            label_visibility="collapsed",
            key="pass2_chunk_slider",
            help="20 = safer for rate-limited / small-context models. "
                 "60 = faster for large-context models (Gemini 1.5 Pro). "
                 "Default 40 works for all models."
        )

        st.markdown('<p class="sb-sub">📂 Upload system document like operational SOP or user guide, manual etc.</p>', unsafe_allow_html=True)
        # ── END MANUAL EDIT ────────────────────────────────────────
        # Dynamic key so New Analysis can reset the sidebar uploader too
        sys_up_key = f"sidebar_sys_uploader_{st.session_state.sys_uploader_key_n}"
        sidebar_sys = st.file_uploader(
            "SysContext", type="pdf", key=sys_up_key, label_visibility="collapsed"
        )
        if sidebar_sys is not None:
            raw = sidebar_sys.getvalue()
            if raw and b'%PDF' in raw[:1024]:
                # ── Content gate: reject non-system documents ─────────────────
                # Extract sample text and check for system-doc signals vs
                # personal/non-operational document signals.
                _sys_pages = []
                try:
                    import pdfplumber as _plb
                    with _plb.open(io.BytesIO(raw)) as _pdf:
                        for _pg in _pdf.pages[:4]:
                            _t = _pg.extract_text() or ""
                            if _t.strip():
                                _sys_pages.append(_t.lower())
                except Exception:
                    pass
                _sys_sample = " ".join(_sys_pages)

                # Positive signals — screen/procedural/system-doc language
                _SYS_POSITIVE = [
                    r'\bclick\b', r'\bnavigate\b', r'\bselect\b', r'\bdashboard\b',
                    r'\bscreen\b', r'\bbutton\b', r'\bmenu\b', r'\bfield\b',
                    r'\bworkflow\b', r'\bprocedure\b', r'\bconfigure\b',
                    r'\binstall\b', r'\blog.?in\b', r'\buser guide\b',
                    r'\bmanual\b', r'\bsop\b', r'\binstruction\b',
                    r'\bstep\s+\d\b', r'\bmodule\b', r'\btab\b', r'\bform\b',
                    r'\bsystem\b', r'\bapplication\b', r'\bsoftware\b',
                ]
                # Negative signals — personal/non-operational documents
                _SYS_NEGATIVE = [
                    r'\bwork experience\b', r'\bemployment history\b',
                    r'\bcurriculum vitae\b', r'\b\bcv\b\b',
                    r'\beducation\b.*\buniversity\b', r'\bdegree\b.*\bgraduat',
                    r'\bskills\b.*\bproficien', r'\breferences available\b',
                    r'\bdate of birth\b', r'\blinkedin\.com\b',
                    r'\binvoice\b', r'\bpurchase order\b',
                    r'\btotal due\b', r'\bremit payment\b',
                    r'\bdear\b.*\bsincerely\b',
                ]
                if _sys_sample:
                    _neg_hits = [p for p in _SYS_NEGATIVE
                                 if re.search(p, _sys_sample, re.IGNORECASE)]
                    _pos_hits = [p for p in _SYS_POSITIVE
                                 if re.search(p, _sys_sample, re.IGNORECASE)]

                    if _neg_hits:
                        st.sidebar.error(
                            "⛔ **Document rejected** — this appears to be a personal or "
                            "non-operational document (CV, invoice, letter, etc.). "
                            "Upload a system User Guide, operational SOP, or instruction manual."
                        )
                        st.session_state["sys_context_bytes"] = None
                        st.session_state["sys_context_name"]  = None
                    elif len(_pos_hits) < 3:
                        st.sidebar.warning(
                            f"⚠️ **Low system-doc signal** ({len(_pos_hits)} indicator(s)). "
                            "Expected a User Guide, SOP, or instruction manual with screen "
                            "names, workflow steps, or procedural language. "
                            "The document will be used but may not improve FRS quality."
                        )
                        st.session_state["sys_context_bytes"] = raw
                        st.session_state["sys_context_name"]  = sidebar_sys.name
                    else:
                        st.session_state["sys_context_bytes"] = raw
                        st.session_state["sys_context_name"]  = sidebar_sys.name
                else:
                    # Couldn't extract text (image-only PDF) — accept with warning
                    st.sidebar.warning(
                        "⚠️ Could not extract text from this PDF. "
                        "Ensure it is OCR-searchable for best results."
                    )
                    st.session_state["sys_context_bytes"] = raw
                    st.session_state["sys_context_name"]  = sidebar_sys.name
        elif sidebar_sys is None:
            st.session_state["sys_context_bytes"] = None
            st.session_state["sys_context_name"]  = None

        ctx_name = st.session_state.get("sys_context_name")
        if ctx_name:
            st.markdown(
                f'<p class="sb-filename">📄 {ctx_name}</p>',
                unsafe_allow_html=True
            )

        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {user} &nbsp;|&nbsp; Role: {role}</p>', unsafe_allow_html=True)

        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            log_audit(user, "LOGOUT", "SESSION")
            st.session_state.clear()   # wipe everything — no stale results on re-login
            st.rerun()

        if role == "Admin":
            with st.expander("🗄️ DB Status", expanded=False):
                st.markdown(f'<p class="sidebar-stats">📁 {DB_PATH}</p>', unsafe_allow_html=True)
                for table, count in db_diagnostics().items():
                    color = "#4ade80" if isinstance(count, int) and count > 0 else "#94a3b8"
                    st.markdown(
                        f'<p class="sidebar-stats" style="color:{color}">{table}: {count} rows</p>',
                        unsafe_allow_html=True
                    )

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
                    new_u_clean = sanitize_input(new_u, max_length=64)
                    new_p_clean = sanitize_input(new_p, max_length=256)
                    if new_u_clean and new_p_clean:
                        if len(new_p_clean) < 8:
                            st.warning("⚠️ Password must be at least 8 characters.")
                        else:
                            create_user(new_u_clean, new_p_clean, new_r)
                            log_audit(user, "USER_CREATED", "USER",
                                      new_value=f"{new_u_clean} ({new_r})",
                                      reason=f"Created by Admin: {user}")
                            st.success(f"User '{new_u_clean}' created with role: {new_r}.")
                    else:
                        st.warning("Username and password are required.")

    # ── End Session trigger — right-aligned via spacer column ───────────────
    _es_space, _es_col = st.columns([11, 3])
    with _es_col:
        if st.button("⏹ End Session", key="terminate_hidden_trigger"):
            log_audit(user, "LOGOUT", "SESSION", reason="Fixed top-right terminate button")
            st.session_state.clear()
            st.rerun()

    # ── Sticky End Session: only inject when page has results (scrollable) ──
    # Using Python session state is the only reliable way to gate this —
    # JS DOM scroll measurements inside an iframe are defeated by Streamlit's
    # rerun cycle. When results are present the page is always scrollable;
    # when no results are loaded the page fits the viewport.
    _has_results = bool(
        st.session_state.get("last_result") or
        st.session_state.get("esig_pending")
    )
    if _has_results:
        _st_components.html("""
    <script>
    (function() {
        var DOC = window.parent.document;
        var old = DOC.getElementById('sticky-terminate-btn');
        if (old) old.parentNode.removeChild(old);
        var btn = DOC.createElement('button');
        btn.id = 'sticky-terminate-btn';
        btn.innerHTML = '&#9209; End Session';
        Object.assign(btn.style, {
            position:     'fixed',
            top:          '58px',
            right:        '20px',
            zIndex:       '2147483647',
            background:   '#dc2626',
            color:        'white',
            border:       'none',
            borderRadius: '8px',
            padding:      '6px 16px',
            fontSize:     '0.82rem',
            fontWeight:   '600',
            cursor:       'pointer',
            boxShadow:    '0 2px 8px rgba(220,38,38,0.4)',
            fontFamily:   'inherit'
        });
        btn.onmouseover = function(){ this.style.background = '#b91c1c'; };
        btn.onmouseout  = function(){ this.style.background = '#dc2626'; };
        btn.onclick = function() {
            var all = DOC.querySelectorAll('button');
            for (var i = 0; i < all.length; i++) {
                if (all[i].innerText && all[i].innerText.trim().indexOf('\u23f9') === 0) {
                    all[i].click(); return;
                }
            }
        };
        DOC.body.appendChild(btn);
    })();
    </script>
        """, height=0)
    else:
        # No results — remove any stale sticky button (e.g. after New Analysis)
        _st_components.html("""
    <script>
    (function() {
        var old = window.parent.document.getElementById('sticky-terminate-btn');
        if (old) old.parentNode.removeChild(old);
    })();
    </script>
        """, height=0)

    # IP capture on every authenticated load
    _capture_client_ip()

    # ── Mode routing ──────────────────────────────────────────────────────────
    _mode = st.session_state.get("app_mode", "New Validation")

    if _mode == "Change Impact Analysis":
        show_change_impact(user, role, MODELS[st.session_state.selected_model])
        return

    if _mode == "Periodic Review":
        show_periodic_review(user, role, MODELS[st.session_state.selected_model])
        return

    if _mode == "Delta Generation (coming soon)":
        st.title("Delta Generation")
        st.info("🚧 Coming soon. Select another mode in the sidebar to continue.")
        return

    # ── Main area — New Validation ────────────────────────────────────────────
    st.title("Auto-Generate Validation Package")

    # Dynamic key allows the file-uploader widget to be fully reset after a
    # completed run. Incrementing uploader_key_n forces Streamlit to mount
    # a brand-new widget instance, which clears any retained file state.
    uploader_key = f"main_sop_uploader_{st.session_state.uploader_key_n}"
    sop_widget = st.file_uploader(
        "Upload URS (The 'What')", type="pdf", key=uploader_key
    )

    if sop_widget is not None:
        is_valid_upload, upload_err = validate_upload(sop_widget)
        if not is_valid_upload:
            st.error(upload_err)
            st.session_state.sop_file_bytes = None
            st.session_state.sop_file_name  = None
        else:
            raw_bytes = sop_widget.getvalue()
            if raw_bytes and b'%PDF' in raw_bytes[:1024]:
                new_file = (st.session_state.sop_file_name != sop_widget.name)
                if new_file:
                    # New file — run Stage 0+1 heuristic immediately (free, instant)
                    st.session_state.pop("last_result", None)
                    st.session_state.pop("doc_validation_msg", None)
                    try:
                        pages       = extract_pages(raw_bytes)
                        full_text   = "\n".join(pages) if pages else ""
                        sample      = "\n\n".join(pages[:2]).lower() if pages else ""
                        pos_hits    = [p for p in _URS_POSITIVE if re.search(p, sample, re.IGNORECASE)]
                        neg_hits    = [p for p in _URS_NEGATIVE if re.search(p, sample, re.IGNORECASE)]
                        shall_count = len(re.findall(r'\b(shall|must)\b', full_text, re.IGNORECASE))

                        if len(full_text.strip()) < 300:
                            st.session_state["doc_validation_msg"] = (
                                "error",
                                "⛔ **Document rejected:** too little extractable text. "
                                "The PDF may be image-only or corrupt. Upload a text-based URS."
                            )
                            st.session_state.sop_file_bytes = None
                            st.session_state.sop_file_name  = None
                        elif neg_hits:
                            matched = [p.replace(r'\b','').replace('\\','') for p in neg_hits[:3]]
                            st.session_state["doc_validation_msg"] = (
                                "error",
                                f"⛔ **Document rejected:** non-URS content detected "
                                f"({', '.join(matched)}). Upload a URS, SRS, or SOP."
                            )
                            st.session_state.sop_file_bytes = None
                            st.session_state.sop_file_name  = None
                        elif shall_count < 2:
                            st.session_state["doc_validation_msg"] = (
                                "error",
                                f"⛔ **Document rejected:** only {shall_count} 'shall'/'must' "
                                f"statement(s) found. A URS must contain requirement statements. "
                                f"Upload a valid User Requirements Specification."
                            )
                            st.session_state.sop_file_bytes = None
                            st.session_state.sop_file_name  = None
                        elif len(pos_hits) < 3:
                            st.session_state["doc_validation_msg"] = (
                                "warning",
                                f"⚠️ **Low URS signal** ({len(pos_hits)} indicator(s), "
                                f"{shall_count} shall/must). "
                                f"The AI will perform a deeper content check at Run Analysis."
                            )
                            st.session_state.sop_file_bytes = raw_bytes
                            st.session_state.sop_file_name  = sop_widget.name
                        else:
                            st.session_state["doc_validation_msg"] = (
                                "success",
                                f"✅ **Pre-screen passed** — {len(pos_hits)} URS indicator(s), "
                                f"{shall_count} requirement statement(s). "
                                f"AI deep-check runs at analysis time."
                            )
                            st.session_state.sop_file_bytes = raw_bytes
                            st.session_state.sop_file_name  = sop_widget.name
                    except Exception:
                        st.session_state.sop_file_bytes = raw_bytes
                        st.session_state.sop_file_name  = sop_widget.name
                else:
                    # Same file retained (e.g. model change rerun)
                    if st.session_state.sop_file_bytes is None:
                        st.session_state.sop_file_bytes = raw_bytes
            else:
                st.error("⚠️ Uploaded file does not appear to be a valid PDF. Please try again.")
                st.session_state.sop_file_bytes = None
                st.session_state.sop_file_name  = None
    else:
        st.session_state.sop_file_bytes = None
        st.session_state.sop_file_name  = None
        st.session_state.pop("last_result", None)
        st.session_state.pop("doc_validation_msg", None)

    # Show document validation banner
    val_msg = st.session_state.get("doc_validation_msg")
    if val_msg:
        level, msg = val_msg
        if   level == "error":   st.error(msg)
        elif level == "warning": st.warning(msg)
        elif level == "success": st.success(msg)



    is_ready = st.session_state.sop_file_bytes is not None

    # Retained-file banner — only shown when file is retained but uploader widget is empty
    if is_ready and sop_widget is None and st.session_state.sop_file_name:
        st.info(
            f"📎 Retained: **{st.session_state.sop_file_name}** — model change did not clear the file."
        )

    # ── MANUAL EDIT v29-custom — DO NOT OVERWRITE ──────────────
    st.markdown("<br>", unsafe_allow_html=True)
    # ── END MANUAL EDIT ────────────────────────────────────────


    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        file_bytes = st.session_state.sop_file_bytes
        file_name  = st.session_state.sop_file_name or "unknown.pdf"
        model_id   = MODELS[st.session_state.selected_model]
        sys_ctx    = st.session_state.get("sys_context_bytes", None)

        # ── Stage 2: LLM document pre-flight ─────────────────────────────────
        # Stage 1 already ran on upload (heuristic, free). Stage 2 uses the
        # selected model to confirm this is genuinely a URS/SRS/SOP.
        # This is the only place an API call is made before the main pipeline.
        with st.spinner("🔍 Validating document type..."):
            is_valid_doc, validation_msg = validate_urs_document(file_bytes, model_id)

        if not is_valid_doc:
            st.error(validation_msg)
            log_audit(user, "DOCUMENT_REJECTED", "URS_FILE",
                      new_value=file_name,
                      reason=f"Document validation failed: {validation_msg[:300]}")
            st.session_state.sop_file_bytes = None
            st.session_state.sop_file_name  = None
            st.session_state["doc_validation_msg"] = (
                "error",
                f"⛔ **Document rejected** — not a valid URS/SRS/SOP. "
                f"Please upload a requirements specification document."
            )
            st.stop()

        log_audit(user, "ANALYSIS_INITIATED", "URS_FILE",
                  new_value=file_name,
                  reason=(
                      f"Model: {st.session_state.selected_model} | Prompt: {PROMPT_VERSION} | "
                      f"Temp: {TEMPERATURE} | Validation: {validation_msg[:100]}"
                      + (f" | Guide: {st.session_state.get('sys_context_name','')}"
                         if st.session_state.get("sys_context_name") else "")
                  ))
        st.info(f"⚙️ Two-pass analysis started — {st.session_state.selected_model} — chunk size: {CHUNK_SIZE} pages")

        progress_bar = st.progress(0)
        status_text  = st.empty()

        # ── Chain-of-thought status container — wraps the real work ──────────
        # st.status stays "running" while the with block executes, collapses to
        # "complete" on success or "error" on exception. Each st.write() call
        # appends a live step log that the user can read during the wait.
        with st.status("🔍 Running GxP Validation Pipeline...", expanded=True) as cot_status:
          try:
            # ── Parser Quality Indicator ─────────────────────────────────────
            # Warn early if the document has low text density (many images,
            # scanned pages) so the user knows to expect lower extraction quality.
            st.write("📑 Parsing URS document structure and page layout...")
            _pq_pages = []
            try:
                import pdfplumber as _plumber
                with _plumber.open(io.BytesIO(file_bytes)) as _pdf:
                    _total_pg   = len(_pdf.pages)
                    _image_pg   = sum(1 for pg in _pdf.pages
                                      if len(pg.images or []) > 0
                                      and len((pg.extract_text() or "").strip()) < 100)
                    _text_chars = sum(len((pg.extract_text() or "").strip())
                                      for pg in _pdf.pages)
                _avg_density = _text_chars / max(_total_pg, 1)
                if _image_pg > _total_pg * 0.4 or _avg_density < 200:
                    st.warning(
                        f"⚠️ **Parser Quality Warning** — {_image_pg}/{_total_pg} pages appear "
                        f"image-heavy or have low text density (avg {int(_avg_density)} chars/page). "
                        "Ensure the URS is OCR-searchable for 100% extraction accuracy. "
                        "Scanned PDFs without OCR will produce incomplete results."
                    )
                else:
                    st.write(f"✅ Document quality check passed — "
                             f"{_total_pg} pages, avg {int(_avg_density)} chars/page")
            except Exception:
                st.write("📑 Document quality check skipped (pdfplumber unavailable)")

            st.write("🔒 Applying GxP document integrity checks (ALCOA+)...")
            if st.session_state.get("sys_context_name"):
                st.write(f"📖 Loading System User Guide: "
                         f"{st.session_state.get('sys_context_name')} ...")

            # ── Step 1: Two-pass AI analysis ─────────────────────────────────
            st.write(f"🔬 Pass 1 — Extracting URS requirements using "
                     f"{st.session_state.selected_model}...")
            urs_df, frs_df, oq_df, trace_df, gap_df = run_segmented_analysis(
                file_bytes, model_id, progress_bar, status_text, sys_ctx
            )

            st.write("🏗️ Pass 2 — Mapping Functional Requirements to system architecture...")
            st.write("🧪 Pass 2 — Risk-adjusted OQ test cases (High ≥3 | Med ≥2 | Low ≥1)...")
            st.write("📊 Building traceability matrix (URS → FRS → OQ)...")
            if st.session_state.get("sys_context_name"):
                st.write("🔀 Pass 3 — Bidirectional URS ↔ User Guide cross-source gap analysis...")

            # ── Guard: if the AI returned nothing useful, stop cleanly ────────
            if urs_df.empty and frs_df.empty:
                progress_bar.empty()
                status_text.empty()
                cot_status.update(label="❌ Pipeline aborted — no output", state="error")
                st.error(
                    "⚠️ No requirements were extracted. This usually means:\n"
                    "- **API quota exceeded** — check your API key billing/limits\n"
                    "- **Rate limit** — wait a minute and try again\n"
                    "- **Model unavailable** — try a different AI Model\n\n"
                    "The error detail is shown above."
                )
                log_audit(user, "ANALYSIS_ABORTED", "URS_FILE",
                          reason="Empty AI output — possible rate limit or quota error")
                return

            # ── URS Accountability Check ───────────────────────────────────────
            if not urs_df.empty and "Req_ID" in urs_df.columns:
                urs_ids_all  = set(urs_df["Req_ID"].dropna().astype(str).str.strip())
                frs_urs_refs = set()
                if not frs_df.empty and "Source_URS_Ref" in frs_df.columns:
                    frs_urs_refs = set(frs_df["Source_URS_Ref"].dropna().astype(str).str.strip())
                uncovered_urs = urs_ids_all - frs_urs_refs
                if uncovered_urs:
                    log_audit(user, "URS_FRS_GAP_DETECTED", "URS_FILE",
                              new_value=f"{len(uncovered_urs)} URS IDs missing FRS",
                              reason=f"IDs: {', '.join(sorted(uncovered_urs)[:20])}")

            # ── Step 2: Deterministic validation ──────────────────────────────
            st.write("🛡️ Running deterministic validation rules R0–R6...")
            status_text.text("🔍 Running deterministic validation rules R1–R5...")
            gap_df, det_df = run_deterministic_validation(frs_df, oq_df, gap_df, urs_df)
            for _df in [gap_df, det_df, trace_df]:
                _df.fillna("N/A", inplace=True)
                _df.replace("", "N/A", inplace=True)

            # ── Step 3: Persist documents ──────────────────────────────────────
            id_urs   = save_document("URS_Extraction", urs_df.to_csv(index=False),  user, file_name)
            id_frs   = save_document("FRS",            frs_df.to_csv(index=False),  user, file_name)
            id_oq    = save_document("OQ",             oq_df.to_csv(index=False),   user, file_name)
            id_trace = save_document("Traceability",   trace_df.to_csv(index=False),user, file_name)
            id_gap   = save_document("Gap_Analysis",   gap_df.to_csv(index=False),  user, file_name)
            id_det   = save_document("Det_Validation", det_df.to_csv(index=False),  user, file_name)
            doc_ids  = (f"URS:{id_urs}, FRS:{id_frs}, OQ:{id_oq}, "
                        f"Trace:{id_trace}, Gap:{id_gap}, Det:{id_det}")

            # ── Step 4: AI generation log ──────────────────────────────────────
            log_ai_generation(
                user, st.session_state.selected_model,
                PROMPT_VERSION, TEMPERATURE, file_name,
                document_ids_used=doc_ids
            )

            # ── Step 5: Audit entries ──────────────────────────────────────────
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

            # ── Step 6: Confidence summary ─────────────────────────────────────
            frs_review = 0
            oq_review  = 0
            if not frs_df.empty and "Confidence_Flag" in frs_df.columns:
                frs_review = int(frs_df["Confidence_Flag"].astype(str).str.contains("Review").sum())
            if not oq_df.empty and "Confidence_Flag" in oq_df.columns:
                oq_review = int(oq_df["Confidence_Flag"].astype(str).str.contains("Review").sum())

            # ── Step 7: Build audit log and dashboard ──────────────────────────
            ver_frs = get_next_doc_version("FRS") - 1
            ver_oq  = get_next_doc_version("OQ")  - 1
            audit_df     = build_audit_log_sheet(
                user, file_name, st.session_state.selected_model,
                frs_df, oq_df, gap_df, det_df, ver_frs, ver_oq, doc_ids,
                sys_context_name=st.session_state.get("sys_context_name") or ""
            )
            dashboard_df = build_dashboard_sheet(
                frs_df, oq_df, gap_df, det_df, trace_df, file_name,
                st.session_state.selected_model
            )

            gap_sheet_included = not gap_df.empty
            dataframes = {
                "Dashboard":      dashboard_df,
                "URS_Extraction": urs_df,
                "FRS":            frs_df,
                "OQ":             oq_df,
                "Traceability":   trace_df,
                "Det_Validation": det_df,
                "Audit_Log":      audit_df,
            }
            if gap_sheet_included:
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

            st.write("📋 Compiling signed validation workbook...")
            xlsx_bytes_presig = build_styled_excel(
                dataframes,
                user=user,
                file_name=file_name,
                model_name=st.session_state.selected_model,
                sys_context_name=st.session_state.get("sys_context_name") or "",
                dashboard_df=dashboard_df,
            )

            doc_hash = hashlib.sha256(xlsx_bytes_presig).hexdigest()
            log_audit(user, "WORKBOOK_BUILT", "VALIDATION_PACKAGE",
                      new_value=f"Validation_Package_{datetime.date.today()}.xlsx",
                      reason=f"doc_ids={doc_ids} | hash={doc_hash[:16]}...")

            status_text.empty()
            progress_bar.empty()

            covered = partial_cov = 0
            if not trace_df.empty and "Coverage_Status" in trace_df.columns:
                covered     = int((trace_df["Coverage_Status"] == "Covered").sum())
                partial_cov = int((trace_df["Coverage_Status"] == "Partial").sum())
            total_reqs = len(frs_df)
            has_tests  = covered + partial_cov
            cov_pct    = round(has_tests / total_reqs * 100, 1) if total_reqs > 0 else 0.0

            cot_status.update(
                label=f"✅ Pipeline complete — {len(urs_df)} requirements, "
                      f"{len(oq_df)} tests, {len(gap_df)+len(det_df)} issues",
                state="complete", expanded=False
            )

            st.session_state["esig_pending"] = {
                "xlsx_bytes_presig": xlsx_bytes_presig,
                "doc_hash":          doc_hash,
                "dataframes":        dataframes,
                "frs_review":        frs_review,
                "oq_review":         oq_review,
                "total_reqs":        len(frs_df),
                "total_tests":       len(oq_df),
                "total_urs":         len(urs_df),
                "covered":           covered,
                "cov_pct":           cov_pct,
                "gap_count":         len(gap_df),
                "det_count":         len(det_df),
                "non_testable_count": int(
                    det_df[det_df["Gap_Type"].astype(str) == "Non_Testable_Requirement"].shape[0]
                    if not det_df.empty and "Gap_Type" in det_df.columns else 0
                ),
                "file_name":         file_name,
                "model_name":        st.session_state.selected_model,
                "doc_ids":           doc_ids,
            }
            st.rerun()

          except Exception as e:
            err_msg = str(e)
            log_audit(user, "ANALYSIS_ERROR", "URS_FILE", reason=err_msg[:500])
            cot_status.update(label="❌ Pipeline failed — see error below", state="error")
            if "Pass 1" in err_msg or "Pass 2" in err_msg or "ALCOA+" in err_msg or "segment" in err_msg.lower():
                st.error(f"🛑 **GxP Fail-Stop Protocol Activated**\n\n{err_msg}")
            else:
                st.error(f"❌ Engineering Error: {err_msg}")
                import traceback
                st.error(traceback.format_exc())

    # ── Determine what to display: signed result or pending preview ──────────
    # Preview is shown from either state so the user sees tables immediately.
    # The e-sig form only appears inline when they click a download button.
    _display = st.session_state.get("last_result") or st.session_state.get("esig_pending")

    if _display:
        r            = _display
        is_signed    = "last_result" in st.session_state
        pending      = st.session_state.get("esig_pending")

        # ── Signature confirmation banner (only after signing) ────────────────
        if is_signed and r.get("sig_id"):
            st.success(
                f"✅ Electronically signed — SIG-{r['sig_id']:06d} | "
                f"{r['sig_meaning']} | {r['sig_timestamp'][:19]} UTC"
            )

        # ── Validation Package Summary ─────────────────────────────────────────
        fully_covered = r.get("covered", 0)
        total_reqs    = r["total_reqs"]
        cov_pct       = r["cov_pct"]
        gap_total     = r["gap_count"] + r["det_count"]
        cov_status    = "✅ PASS" if cov_pct >= 80 else ("⚠️ REVIEW" if cov_pct >= 60 else "❌ FAIL")

        st.markdown(f"""
<div style="background:#0f172a;border-radius:12px;padding:20px 28px;margin-bottom:18px;
            border-left:5px solid #2563eb;font-family:'Inter',sans-serif;">
  <p style="color:#94a3b8;font-size:0.75rem;letter-spacing:3px;text-transform:uppercase;
            margin:0 0 10px 0;">Validation Package Summary</p>
  <hr style="border:none;border-top:1px solid #1e293b;margin:0 0 14px 0;">
  <table style="width:100%;border-collapse:collapse;color:white;font-size:0.92rem;">
    <tr>
      <td style="padding:4px 0;color:#94a3b8;width:55%;">📋 Requirements extracted (URS)</td>
      <td style="padding:4px 0;font-weight:700;color:#e2e8f0;">{r["total_urs"]}</td>
    </tr>
    <tr>
      <td style="padding:4px 0;color:#94a3b8;">📐 FRS requirements generated</td>
      <td style="padding:4px 0;font-weight:700;color:#e2e8f0;">{total_reqs}</td>
    </tr>
    <tr>
      <td style="padding:4px 0;color:#94a3b8;">🧪 OQ test cases generated</td>
      <td style="padding:4px 0;font-weight:700;color:#e2e8f0;">{r["total_tests"]}</td>
    </tr>
    <tr>
      <td style="padding:4px 0;color:#94a3b8;">✅ Fully covered requirements</td>
      <td style="padding:4px 0;font-weight:700;color:#4ade80;">{fully_covered}</td>
    </tr>
    <tr>
      <td style="padding:4px 0;color:#94a3b8;">📊 Traceability coverage</td>
      <td style="padding:4px 0;font-weight:700;
                 color:{'#4ade80' if cov_pct >= 80 else ('#facc15' if cov_pct >= 60 else '#f87171')};">
        {cov_pct}% &nbsp; {cov_status}
      </td>
    </tr>
    <tr>
      <td style="padding:4px 0;color:#94a3b8;">⚠️ Gaps detected (AI + deterministic)</td>
      <td style="padding:4px 0;font-weight:700;
                 color:{'#f87171' if gap_total > 0 else '#4ade80'};">
        {gap_total}
      </td>
    </tr>
  </table>
</div>
""", unsafe_allow_html=True)

        col1, col2, col3, col4, col5, col6 = st.columns(6)
        col1.metric("📄 URS Requirements", r["total_urs"])
        col2.metric("📋 FRS Requirements", r["total_reqs"])
        col3.metric("🧪 OQ Test Cases",    r["total_tests"])
        col4.metric("📊 Coverage",          f"{r['cov_pct']}%")
        col5.metric("⚠️ Issues (AI+Det)",   r["gap_count"] + r["det_count"])
        col6.metric("🚫 Non-Testable",
                    f"{round(r.get('non_testable_count',0)/r['total_urs']*100,1) if r['total_urs']>0 else 0}%",
                    delta=f"{r.get('non_testable_count',0)} reqs",
                    delta_color="inverse")

        # ── Hero metric: Unmitigated GxP Risks ───────────────────────────────
        # Counts High-Risk FRS items with zero OQ test coverage — the single
        # most important compliance signal. Red = active regulatory exposure.
        _unmitigated = 0
        _dfs = r.get("dataframes", {})
        _frs_df = _dfs.get("FRS", pd.DataFrame())
        _oq_df  = _dfs.get("OQ",  pd.DataFrame())
        if not _frs_df.empty and "Risk" in _frs_df.columns:
            _high_risk_ids = set(
                _frs_df[_frs_df["Risk"].astype(str).str.lower() == "high"]
                .get("ID", pd.Series(dtype=str)).astype(str).str.strip()
            )
            if _high_risk_ids:
                if not _oq_df.empty and "Requirement_Link" in _oq_df.columns:
                    _tested_ids = set(_oq_df["Requirement_Link"].astype(str).str.strip())
                    _unmitigated = len(_high_risk_ids - _tested_ids)
                else:
                    _unmitigated = len(_high_risk_ids)

        _hero_color  = "#dc2626" if _unmitigated > 0 else "#059669"
        _hero_icon   = "🔴" if _unmitigated > 0 else "🟢"
        _hero_label  = "CRITICAL — Regulatory Exposure" if _unmitigated > 0 else "All High-Risk Requirements Covered"
        _hero_detail = (
            f"{_unmitigated} High-Risk FRS item(s) have zero OQ test coverage. "
            "Per GAMP 5, high-risk requirements require ≥3 test cases. "
            "This package will fail a regulatory audit as-is — add OQ tests before signing."
            if _unmitigated > 0 else
            "All High-Risk FRS requirements have OQ test coverage. "
            "No unmitigated regulatory exposure detected."
        )
        st.markdown(f"""
<div style="background:{'#1a0505' if _unmitigated > 0 else '#052019'};
            border:2px solid {_hero_color};border-radius:12px;
            padding:16px 24px;margin:12px 0 8px 0;
            font-family:'Inter',sans-serif;">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:6px;">
    <span style="font-size:2rem;">{_hero_icon}</span>
    <div>
      <p style="margin:0;color:#94a3b8;font-size:0.72rem;letter-spacing:2px;
                text-transform:uppercase;">Unmitigated GxP Risks</p>
      <p style="margin:0;font-size:2rem;font-weight:800;color:{_hero_color};
                line-height:1;">{_unmitigated}</p>
    </div>
    <div style="margin-left:16px;border-left:1px solid #334155;padding-left:16px;">
      <p style="margin:0;color:{'#fca5a5' if _unmitigated > 0 else '#6ee7b7'};
                font-size:0.85rem;font-weight:600;">{_hero_label}</p>
      <p style="margin:4px 0 0 0;color:#94a3b8;font-size:0.76rem;">{_hero_detail}</p>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

        # ── Non-Testable % hero card ──────────────────────────────────────────
        _nt_count = r.get("non_testable_count", 0)
        _nt_pct   = round(_nt_count / r["total_urs"] * 100, 1) if r["total_urs"] > 0 else 0.0
        _nt_color = "#dc2626" if _nt_pct >= 20 else ("#d97706" if _nt_pct >= 10 else "#059669")
        _nt_bg    = "#1a0505"  if _nt_pct >= 20 else ("#1a1000" if _nt_pct >= 10 else "#052019")
        _nt_status = (
            "CRITICAL — URS requires significant rewriting before validation"
            if _nt_pct >= 20 else
            "WARNING — Some requirements need measurable acceptance criteria"
            if _nt_pct >= 10 else
            "PASS — URS is sufficiently testable for validation"
        )
        _nt_detail = (
            f"{_nt_count} of {r['total_urs']} requirements contain ambiguous or non-testable language "
            f"(e.g. 'user-friendly', 'fast', 'should'). "
            "These cannot be objectively validated — a direct 21 CFR Part 11 compliance risk. "
            "See Det_Validation tab → Rule R3d for specific terms and remediation guidance."
            if _nt_count > 0 else
            "All URS requirements contain measurable, testable language. No ambiguous terms detected."
        )
        st.markdown(f"""
<div style="background:{_nt_bg};border:2px solid {_nt_color};border-radius:12px;
            padding:16px 24px;margin:8px 0 12px 0;font-family:'Inter',sans-serif;">
  <div style="display:flex;align-items:center;gap:12px;">
    <span style="font-size:2rem;">{'🔴' if _nt_pct >= 20 else ('🟡' if _nt_pct >= 10 else '🟢')}</span>
    <div>
      <p style="margin:0;color:#94a3b8;font-size:0.72rem;letter-spacing:2px;
                text-transform:uppercase;">Non-Testable Requirements</p>
      <p style="margin:0;font-size:2rem;font-weight:800;color:{_nt_color};line-height:1;">
        {_nt_pct}%
        <span style="font-size:1rem;font-weight:400;color:#64748b;margin-left:8px;">
          ({_nt_count} of {r['total_urs']} requirements)
        </span>
      </p>
    </div>
    <div style="margin-left:16px;border-left:1px solid #334155;padding-left:16px;">
      <p style="margin:0;font-size:0.85rem;font-weight:600;
                color:{'#fca5a5' if _nt_pct >= 20 else ('#fde68a' if _nt_pct >= 10 else '#6ee7b7')};">
        {_nt_status}</p>
      <p style="margin:4px 0 0 0;color:#94a3b8;font-size:0.76rem;">{_nt_detail}</p>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

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
                f"⚠️ Gap Analysis: **{r['gap_count']}** AI-detected gap(s) "
                "— see Gap_Analysis tab in the workbook."
            )
        elif r["det_count"] == 0:
            st.success("✅ No AI gaps and no deterministic issues. Review Traceability for partial coverage details.")

        with st.expander("📋 Preview Generated Sheets", expanded=True):
            for sheet_name, df in r["dataframes"].items():
                st.markdown(f"**{sheet_name}** — {len(df)} rows")
                st.dataframe(df, use_container_width=True)

        # ── Download / Sign buttons ────────────────────────────────────────────
        st.markdown("---")

        if is_signed:
            # ── Already signed: real download buttons + New Analysis far right ─
            dl1, dl2, _spacer, clear_col = st.columns([5, 5, 1, 2])
            with dl1:
                st.download_button(
                    label="📥 Download Signed Workbook (.xlsx)",
                    data=r["xlsx_bytes"],
                    file_name=f"Validation_Package_{r['file_name'].replace('.pdf','')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="download_xlsx_btn",
                    use_container_width=True,
                )
            with dl2:
                st.download_button(
                    label="📄 Download Signed Summary (.pdf)",
                    data=r["pdf_bytes"],
                    file_name=f"Validation_Summary_{r['file_name'].replace('.pdf','')}.pdf",
                    mime="application/pdf",
                    key="download_pdf_btn",
                    use_container_width=True,
                )
            # _spacer is empty — pushes New Analysis to the far right
            with clear_col:
                if st.button("🔄 New Analysis", key="clear_results_btn",
                             use_container_width=True,
                             help="Clear results and upload a new URS document"):
                    st.session_state["uploader_key_n"]     = st.session_state.get("uploader_key_n", 0) + 1
                    st.session_state["sys_uploader_key_n"] = st.session_state.get("sys_uploader_key_n", 0) + 1
                    st.session_state.sop_file_bytes  = None
                    st.session_state.sop_file_name   = None
                    st.session_state["sys_context_bytes"] = None
                    st.session_state["sys_context_name"]  = None
                    st.session_state.pop("last_result",        None)
                    st.session_state.pop("esig_pending",       None)
                    st.session_state.pop("show_esig_form",     None)
                    st.session_state.pop("esig_target",        None)
                    st.session_state.pop("doc_validation_msg", None)
                    log_audit(user, "NEW_ANALYSIS_STARTED", "SESSION",
                              reason="User cleared previous results and sidebar guide to start a new analysis")
                    st.rerun()

        else:
            # ── Not yet signed: Sign & Download triggers + New Analysis far right
            show_form = st.session_state.get("show_esig_form", False)

            if not show_form:
                st.info(
                    "🔏 **Electronic signature required** (21 CFR Part 11). "
                    "Click a download button below to sign and release your package."
                )
                btn1, btn2, _spacer, clear_col = st.columns([5, 5, 1, 2])
                with btn1:
                    if st.button("🔏 Sign & Download Excel (.xlsx)",
                                 key="trigger_esig_xlsx", use_container_width=True,
                                 type="primary"):
                        st.session_state["show_esig_form"] = True
                        st.session_state["esig_target"]    = "xlsx"
                        st.rerun()
                with btn2:
                    if st.button("🔏 Sign & Download PDF (.pdf)",
                                 key="trigger_esig_pdf", use_container_width=True,
                                 type="primary"):
                        st.session_state["show_esig_form"] = True
                        st.session_state["esig_target"]    = "pdf"
                        st.rerun()
                # _spacer empty — pushes New Analysis to the far right
                with clear_col:
                    if st.button("🔄 New Analysis", key="clear_results_btn",
                                 use_container_width=True,
                                 help="Clear results and upload a new URS document"):
                        st.session_state["uploader_key_n"]     = st.session_state.get("uploader_key_n", 0) + 1
                        st.session_state["sys_uploader_key_n"] = st.session_state.get("sys_uploader_key_n", 0) + 1
                        st.session_state.sop_file_bytes  = None
                        st.session_state.sop_file_name   = None
                        st.session_state["sys_context_bytes"] = None
                        st.session_state["sys_context_name"]  = None
                        st.session_state.pop("last_result",        None)
                        st.session_state.pop("esig_pending",       None)
                        st.session_state.pop("show_esig_form",     None)
                        st.session_state.pop("esig_target",        None)
                        st.session_state.pop("doc_validation_msg", None)
                        log_audit(user, "NEW_ANALYSIS_STARTED", "SESSION",
                                  reason="User cleared previous results to start a new analysis")
                        st.rerun()

            # ── Inline e-sig form (appears below preview when triggered) ──────
            if show_form and pending:
                p = pending
                st.markdown("""
<div class="esig-container">
  <p class="esig-title">🔏 Electronic Signature Required</p>
  <p class="esig-subtitle">
    21 CFR Part 11 §11.200 — Two-component non-biometric signature.<br>
    Re-enter your password to sign and release this validation package.
  </p>
</div>
""", unsafe_allow_html=True)

                with st.form("esig_form", clear_on_submit=False):
                    st.markdown(
                        f'<p class="esig-field-label">Signer</p>'
                        f'<div class="esig-user-display">👤 &nbsp; {user}'
                        f' &nbsp;&nbsp;|&nbsp;&nbsp; {role}</div>',
                        unsafe_allow_html=True
                    )
                    st.markdown(
                        '<p class="esig-field-label">Password (re-enter to verify identity)</p>',
                        unsafe_allow_html=True)
                    esig_password = st.text_input(
                        "Password", type="password",
                        placeholder="Enter your login password",
                        label_visibility="collapsed",
                        key="esig_password_input"
                    )
                    st.markdown('<p class="esig-field-label">Meaning of Signature</p>',
                                unsafe_allow_html=True)
                    esig_meaning = st.selectbox(
                        "Meaning", ESIG_MEANINGS, index=0,
                        label_visibility="collapsed",
                        key="esig_meaning_select"
                    )
                    st.markdown(
                        '<p class="esig-warning">⚠️ By submitting this signature you confirm '
                        'that the information in this validation package is accurate and '
                        'complete. This action is recorded and cannot be undone.</p>',
                        unsafe_allow_html=True
                    )
                    col_sign, col_cancel = st.columns([2, 1])
                    with col_sign:
                        submitted = st.form_submit_button(
                            "✍️ Sign & Release Package",
                            use_container_width=True, type="primary"
                        )
                    with col_cancel:
                        cancelled = st.form_submit_button("✖ Cancel",
                                                          use_container_width=True)

                if cancelled:
                    st.session_state["show_esig_form"] = False
                    st.session_state.pop("esig_target", None)
                    log_audit(user, "ESIG_CANCELLED", "VALIDATION_PACKAGE",
                              reason="User cancelled e-signature — package not released")
                    st.info("Signature cancelled. Review the preview and click a download "
                            "button when ready to sign.")
                    st.rerun()

                if submitted:
                    if not esig_password:
                        st.error("⛔ Password is required to sign.")
                    else:
                        conn_v = db_connect()
                        stored = conn_v.execute(
                            "SELECT password_hash FROM users WHERE username=?", (user,)
                        ).fetchone()
                        conn_v.close()

                        if not stored or not verify_password(esig_password, stored[0]):
                            log_audit(user, "ESIG_IDENTITY_FAILED", "VALIDATION_PACKAGE",
                                      reason="E-sig identity verification failed — wrong password")
                            st.error(
                                "⛔ Identity verification failed. The password does not match "
                                "your account. This attempt has been recorded in the audit trail."
                            )
                        else:
                            sig_ts = datetime.datetime.utcnow().isoformat()
                            sig_id = log_esignature(
                                user          = user,
                                role          = role,
                                action        = "GENERATED_VALIDATION_PACKAGE",
                                meaning       = esig_meaning,
                                document_hash = p["doc_hash"],
                                document_name = p["file_name"],
                                model_used    = p["model_name"],
                                prompt_ver    = PROMPT_VERSION,
                                ip_address    = st.session_state.get("user_ip", ""),
                                doc_ids       = p["doc_ids"],
                            )
                            log_audit(user, "ESIG_APPLIED", "VALIDATION_PACKAGE",
                                      new_value=f"SIG-{sig_id:06d}",
                                      reason=(f"Meaning: {esig_meaning} | "
                                              f"Hash: {p['doc_hash'][:16]}... | "
                                              f"Model: {p['model_name']}"))

                            # ── Build signed Excel ────────────────────────────
                            import openpyxl
                            wb_final = openpyxl.load_workbook(
                                filename=io.BytesIO(p["xlsx_bytes_presig"])
                            )
                            build_signature_sheet(
                                wb            = wb_final,
                                user          = user,
                                role          = role,
                                meaning       = esig_meaning,
                                document_hash = p["doc_hash"],
                                document_name = p["file_name"],
                                model_used    = p["model_name"],
                                signature_id  = sig_id,
                                timestamp     = sig_ts,
                            )
                            final_buf = io.BytesIO()
                            wb_final.save(final_buf)
                            xlsx_bytes_final = final_buf.getvalue()

                            # ── Build signed PDF ──────────────────────────────
                            pdf_bytes_final = build_pdf_bytes(
                                r             = p,
                                sig_id        = sig_id,
                                sig_meaning   = esig_meaning,
                                sig_timestamp = sig_ts,
                                user          = user,
                                role          = role,
                            )

                            st.session_state["last_result"] = {
                                **p,
                                "xlsx_bytes":    xlsx_bytes_final,
                                "pdf_bytes":     pdf_bytes_final,
                                "sig_id":        sig_id,
                                "sig_meaning":   esig_meaning,
                                "sig_timestamp": sig_ts,
                            }
                            st.session_state.pop("esig_pending",   None)
                            st.session_state.pop("show_esig_form", None)
                            st.session_state.pop("esig_target",    None)
                            st.rerun()


# =============================================================================
# 13. ROUTER
# =============================================================================
if not st.session_state.authenticated:
    show_login()
else:
    show_app()