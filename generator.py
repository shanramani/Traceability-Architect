"""
Validation Doc Assist — v15.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Changes over v13:
  1. 8-PAGE SEGMENTED PROCESSING  — prevents "Stream ended unexpectedly" on large SOPs
  2. REDUNDANT-HEADER FILTERING   — df[df.iloc[:,0].astype(str) != df.columns[0]]
  3. RUN ANALYSIS BUTTON CSS      — exact spec: transition 0.2s, translateY(-2px),
                                    brightness(1.1), active snap-back, disabled guard
  4. DB SCHEMA ALIGNMENT          — ai_gen_log table (not ai_generation_log) matches spec
  5. ALL OTHER CSS / BRANDING     — unchanged from v13
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

# pdfplumber: soft import — works locally with poppler, gracefully skipped on Streamlit Cloud
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
VERSION        = "15.0"
PROMPT_VERSION = "v5.0-segmented"
CHUNK_SIZE     = 8          # pages per AI call — prevents token overflow / stream crash
DB_PATH        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation_app.db")

st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    return "Thousand Oaks, USA"

# =============================================================================
# 2. DATABASE  — schema matches spec exactly
#    users | audit_log | documents | ai_gen_log
# =============================================================================

def db_connect():
    return sqlite3.connect(DB_PATH)

def db_migrate():
    try:
        conn = db_connect()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    UNIQUE NOT NULL,
                password_hash TEXT    NOT NULL,
                role          TEXT    DEFAULT 'analyst'
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS audit_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user        TEXT,
                action      TEXT,
                object_type TEXT,
                object_id   TEXT,
                timestamp   TEXT
            )
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_type   TEXT,
                version    INTEGER,
                content    TEXT,
                created_by TEXT,
                created_at TEXT
            )
        """)

        # Spec table name: ai_gen_log (not ai_generation_log)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_gen_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                model          TEXT,
                prompt_version TEXT,
                timestamp      TEXT,
                generated_by   TEXT
            )
        """)

        # Safe migration: add role column to existing users tables that predate this schema
        user_cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "role" not in user_cols:
            conn.execute("ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'analyst'")

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


def log_audit(user: str, action: str, object_type: str, object_id: str = ""):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO audit_log (user,action,object_type,object_id,timestamp) VALUES (?,?,?,?,?)",
            (user, action, object_type, object_id, datetime.datetime.utcnow().isoformat())
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"Audit log write failed: {e}")


def log_ai_generation(user: str, model: str, prompt_version: str):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO ai_gen_log (model,prompt_version,timestamp,generated_by) VALUES (?,?,?,?)",
            (model, prompt_version, datetime.datetime.utcnow().isoformat(), user)
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"AI gen log write failed: {e}")


def save_document(doc_type: str, version: int, content: str, created_by: str):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO documents (doc_type,version,content,created_by,created_at) VALUES (?,?,?,?,?)",
            (doc_type, version, content, created_by, datetime.datetime.utcnow().isoformat())
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"Document save failed: {e}")

# =============================================================================
# 3. AUTHENTICATION  (bcrypt preferred, sha256 fallback)
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

def create_user(username: str, plain_password: str, role: str = "analyst"):
    pw_hash = hash_password(plain_password)
    conn    = db_connect()
    try:
        conn.execute(
            "INSERT INTO users (username,password_hash,role) VALUES (?,?,?)",
            (username, pw_hash, role)
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    finally:
        conn.close()

def authenticate_user(username: str, password: str) -> bool:
    if not username:
        return False
    try:
        conn  = db_connect()
        row   = conn.execute("SELECT password_hash FROM users WHERE username=?", (username,)).fetchone()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()
        if row:
            return verify_password(password, row[0])
        if count == 0:
            create_user(username, password, role="admin")
            log_audit(username, "FIRST_RUN_ACCOUNT_CREATED", "USER")
            return True
        return False
    except Exception:
        return bool(username)

# =============================================================================
# 4. PDF EXTRACTION  (pdfplumber tables + PyPDFLoader fallback)
#    Returns a list of page-text strings for segmented processing
# =============================================================================

def extract_pages(file_bytes: bytes) -> list[str]:
    """
    Extract pages using pdfplumber (with table support) when available,
    falling back to PyPDFLoader on Streamlit Cloud where poppler may not exist.
    """
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
            pages_text = []  # fall through to PyPDFLoader

    # PyPDFLoader fallback (always works on Streamlit Cloud)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
        tmp.write(file_bytes); tmp_path = tmp.name
    try:
        lc_pages   = PyPDFLoader(tmp_path).load()
        pages_text = [f"--- Page {i+1} ---\n{p.page_content}"
                      for i, p in enumerate(lc_pages)]
    finally:
        if os.path.exists(tmp_path): os.remove(tmp_path)

    return pages_text

# =============================================================================
# 5. SEGMENTED AI ANALYSIS  (8-page chunks)
#    Each chunk returns 3 raw CSV blocks separated by |||
#    Results are concatenated with redundant-header filtering
# =============================================================================

SYSTEM_PROMPT = (
    "You are a Principal Validation Engineer specializing in GAMP 5 and 21 CFR Part 11. "
    "You output ONLY structured CSV data — no explanations, no markdown, no preamble. "
    "Always wrap field values that contain commas in double-quotes. "
    "The SOP text may contain [TABLE N] blocks in pipe-delimited format. "
    "Extract requirements from both prose AND table cells."
)

def build_chunk_prompt(chunk_text: str, chunk_index: int, total_chunks: int) -> str:
    return f"""
SOP CONTENT — Segment {chunk_index + 1} of {total_chunks}:
{chunk_text}

TASK: Parse this segment into exactly 3 CSV datasets separated by |||.
Output ONLY raw CSV rows — include the header row in EVERY response.
Wrap any field value containing a comma in double-quotes.

Dataset 1 (FRS): ID,Requirement_Description,Priority,GxP_Impact
Dataset 2 (OQ):  Test_ID,Requirement_Link,Test_Step,Expected_Result
Dataset 3 (Traceability): Req_ID,Test_ID,Gap_Analysis
  - If a requirement has NO corresponding test, leave Test_ID blank and
    begin Gap_Analysis with exactly: [GAP]

Separate each dataset with exactly: |||
"""

def _safe_parse_chunk(raw: str) -> tuple[str, str, str]:
    """Split one chunk's LLM output into (frs_csv, oq_csv, trace_csv)."""
    raw   = re.sub(r'^```[a-zA-Z]*\n?', '', raw, flags=re.MULTILINE)
    raw   = re.sub(r'```\s*$',          '', raw, flags=re.MULTILINE)
    parts = re.split(r'\s*\|\|\|\s*', raw.strip())
    while len(parts) < 3:
        parts.append("")
    return parts[0].strip(), parts[1].strip(), parts[2].strip()

def _csv_to_df(csv_text: str) -> pd.DataFrame:
    """Parse a CSV string defensively; return empty DataFrame on failure."""
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
    """
    Remove rows where the first column value equals the first column header.
    This strips repeated header rows injected by the LLM between chunks.
    Pattern: df[df.iloc[:, 0].astype(str) != df.columns[0]]
    """
    if df.empty or len(df.columns) == 0:
        return df
    return df[df.iloc[:, 0].astype(str) != df.columns[0]].reset_index(drop=True)

def run_segmented_analysis(
    file_bytes: bytes,
    model_id: str,
    progress_bar,
    status_text
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Core segmented processing engine.
    1. Extract all pages
    2. Group into CHUNK_SIZE-page segments
    3. Call LLM per segment with stream=False
    4. Accumulate and deduplicate results
    Returns (frs_df, oq_df, trace_df)
    """
    all_pages = extract_pages(file_bytes)
    chunks    = [all_pages[i:i + CHUNK_SIZE] for i in range(0, len(all_pages), CHUNK_SIZE)]
    total     = len(chunks)

    frs_frames, oq_frames, trace_frames = [], [], []

    for idx, chunk_pages in enumerate(chunks):
        chunk_text = "\n\n".join(chunk_pages)
        status_text.text(f"🔍 Analysing segment {idx + 1} of {total}  ({len(chunk_pages)} pages)...")
        progress_bar.progress((idx) / total)

        try:
            response = completion(
                model=model_id,
                stream=False,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": build_chunk_prompt(chunk_text, idx, total)}
                ]
            )
            raw = response.choices[0].message.content or ""
        except Exception as e:
            st.warning(f"⚠️ Segment {idx+1} failed ({e}) — skipping.")
            continue

        frs_csv, oq_csv, trace_csv = _safe_parse_chunk(raw)

        frs_df   = _csv_to_df(frs_csv)
        oq_df    = _csv_to_df(oq_csv)
        trace_df = _csv_to_df(trace_csv)

        if not frs_df.empty:   frs_frames.append(frs_df)
        if not oq_df.empty:    oq_frames.append(oq_df)
        if not trace_df.empty: trace_frames.append(trace_df)

    progress_bar.progress(1.0)
    status_text.text("✅ All segments processed — compiling workbook...")

    def _combine(frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        combined = pd.concat(frames, ignore_index=True)
        combined = _remove_duplicate_headers(combined)
        combined.dropna(how='all', inplace=True)
        return combined

    frs_final   = _combine(frs_frames)
    oq_final    = _combine(oq_frames)
    trace_final = _combine(trace_frames)

    # Enforce [GAP] prefix in Python — never rely on LLM alone
    if not trace_final.empty and "Gap_Analysis" in trace_final.columns and "Test_ID" in trace_final.columns:
        mask = trace_final["Test_ID"].isna() | (trace_final["Test_ID"].astype(str).str.strip() == "")
        trace_final.loc[mask, "Gap_Analysis"] = trace_final.loc[mask, "Gap_Analysis"].apply(
            lambda v: v if str(v).startswith("[GAP]") else f"[GAP] {v}"
        )

    return frs_final, oq_final, trace_final

# =============================================================================
# 6. AUDIT LOG  (Python-owned — never hallucinated)
# =============================================================================

def build_audit_log(user: str, file_name: str, model_name: str,
                    frs_df: pd.DataFrame, oq_df: pd.DataFrame) -> pd.DataFrame:
    now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    return pd.DataFrame([
        {"Action": "SESSION_LOGIN",       "User": user, "Timestamp": now_str,
         "Change_Description": "User authenticated successfully"},
        {"Action": "SOP_UPLOADED",        "User": user, "Timestamp": now_str,
         "Change_Description": f"SOP file uploaded: {file_name}"},
        {"Action": "AI_ANALYSIS_STARTED", "User": user, "Timestamp": now_str,
         "Change_Description": f"GAMP-5 segmented analysis initiated — model: {model_name}"},
        {"Action": "FRS_GENERATED",       "User": user, "Timestamp": now_str,
         "Change_Description": f"{len(frs_df)} requirements extracted across all segments"},
        {"Action": "OQ_GENERATED",        "User": user, "Timestamp": now_str,
         "Change_Description": f"{len(oq_df)} test cases generated across all segments"},
        {"Action": "TRACEABILITY_BUILT",  "User": user, "Timestamp": now_str,
         "Change_Description": "RTM compiled; [GAP] flags enforced for untestable requirements"},
        {"Action": "WORKBOOK_EXPORTED",   "User": user, "Timestamp": now_str,
         "Change_Description": f"Validation_Package_{datetime.date.today()}.xlsx generated"},
    ])

# =============================================================================
# 7. EXCEL STYLING  (auto-size columns, tab colors, freeze, filter)
# =============================================================================

SHEET_COLORS = {
    "FRS":          {"header_fill": "2563EB", "tab_color": "2563EB"},
    "OQ":           {"header_fill": "059669", "tab_color": "059669"},
    "Traceability": {"header_fill": "7C3AED", "tab_color": "7C3AED"},
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
        cell = ws.cell(row=1, column=col)
        cell.font = header_font; cell.fill = header_fill
        cell.alignment = header_align; cell.border = border

    for row in range(2, max_row + 1):
        for col in range(1, max_col + 1):
            cell = ws.cell(row=row, column=col)
            cell.border = border
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if row % 2 == 0:
                cell.fill = alt_fill

    ws.auto_filter.ref             = ws.dimensions
    ws.freeze_panes                = "A2"
    ws.row_dimensions[1].height    = 30
    ws.sheet_properties.tabColor   = colors["tab_color"]

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


def build_styled_excel(dataframes: dict[str, pd.DataFrame]) -> bytes:
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
# 8. SESSION STATE
# =============================================================================
_defaults = {
    "authenticated":  False,
    "selected_model": "Gemini 1.5 Pro",
    "location":       get_location(),
    "user_name":      "",
    "sop_file_bytes": None,
    "sop_file_name":  None,
}
for _k, _v in _defaults.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

db_migrate()

# =============================================================================
# 9. CSS  — All existing branding preserved.
#           run_analysis_btn updated to exact spec:
#             transition: all 0.2s ease-in-out
#             hover: translateY(-2px) + brightness(1.1) + deeper shadow
#             active: translateY(0px) snap-back
#             disabled: locked grey
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

    /* ── Login inputs ── */
    [data-testid="stTextInput"] {
        width: 25% !important;
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

    /* ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
       RUN ANALYSIS — iOS-inspired styling
       ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ */

    /* 1. Base state (iOS SF Blue) */
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

    /* 2. Gentle hover (soft lift) */
    div.stButton > button[key="run_analysis_btn"]:hover:not(:disabled) {
        background-color: #0063CC !important;
        transform: translateY(-1px) scale(1.02) !important;
        box-shadow: 0 5px 15px rgba(0, 122, 255, 0.25) !important;
        filter: none !important;
        cursor: pointer !important;
    }

    /* 3. Pressed state (tactile sink) */
    div.stButton > button[key="run_analysis_btn"]:active {
        transform: scale(0.96) !important;
        background-color: #0051A8 !important;
        box-shadow: 0 1px 4px rgba(0, 0, 0, 0.1) !important;
        transition: all 0.1s ease !important;
    }

    /* 4. Disabled — GxP safety lock (iOS System Gray) */
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
# 10. LOGIN
# =============================================================================

def show_login():
    left_space, center_content, right_space = st.columns([3, 4, 3])
    with center_content:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>',
                    unsafe_allow_html=True)
        st.markdown("<h1 style='text-align: center;  font-size: 1.5rem;'>🛡️ LLM-Powered GxP Validation </h1>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
        p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
        st.markdown("<br>", unsafe_allow_html=True)
        b_left, b_center, b_right = st.columns([1, 2, 1])
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                if authenticate_user(u, p):
                    st.session_state.user_name     = u
                    st.session_state.authenticated = True
                    log_audit(u, "LOGIN", "SESSION")
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

# =============================================================================
# 11. MAIN APPLICATION
# =============================================================================

def show_app():
    user = st.session_state.get("user_name", "unknown")

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
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {user}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)

        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            log_audit(user, "LOGOUT", "SESSION")
            st.session_state.authenticated = False
            st.rerun()

        with st.expander("🗄️ DB Status", expanded=False):
            st.markdown(f'<p class="sidebar-stats">📁 {DB_PATH}</p>', unsafe_allow_html=True)
            for table, count in db_diagnostics().items():
                color = "#4ade80" if count > 0 else "#94a3b8"
                st.markdown(
                    f'<p class="sidebar-stats" style="color:{color}">{table}: {count} rows</p>',
                    unsafe_allow_html=True
                )

    # ── Main area ──
    st.title("Auto-Generate Validation Package")

    sop_widget = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    if sop_widget is not None:
        raw_bytes = sop_widget.getvalue()
        # Validate it's a real PDF — %PDF signature can appear within first 1024 bytes
        if raw_bytes and b'%PDF' in raw_bytes[:1024]:
            st.session_state.sop_file_bytes = raw_bytes
            st.session_state.sop_file_name  = sop_widget.name
        else:
            st.error("⚠️ Uploaded file does not appear to be a valid PDF. Please try again.")
            st.session_state.sop_file_bytes = None

    is_ready = st.session_state.sop_file_bytes is not None
    if is_ready and sop_widget is None:
        st.info(f"📎 Retained: **{st.session_state.sop_file_name}** — model change did not clear the file.")

    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        file_bytes  = st.session_state.sop_file_bytes
        file_name   = st.session_state.sop_file_name or "unknown.pdf"
        model_id    = MODELS[st.session_state.selected_model]

        log_audit(user, "INITIATE_ANALYSIS", "SOP", file_name)
        st.info(f"⚙️ Segmented analysis started — {st.session_state.selected_model} — chunk size: {CHUNK_SIZE} pages")

        progress_bar = st.progress(0)
        status_text  = st.empty()

        try:
            frs_df, oq_df, trace_df = run_segmented_analysis(
                file_bytes, model_id, progress_bar, status_text
            )

            log_ai_generation(user, st.session_state.selected_model, PROMPT_VERSION)
            save_document("SOP_PROCESSED", 1, f"file={file_name} pages=all", user)

            audit_df = build_audit_log(user, file_name, st.session_state.selected_model, frs_df, oq_df)

            dataframes = {
                "FRS":          frs_df,
                "OQ":           oq_df,
                "Traceability": trace_df,
                "Audit_Log":    audit_df,
            }

            xlsx_bytes = build_styled_excel(dataframes)
            log_audit(user, "GENERATE_WORKBOOK", "VALIDATION_PACKAGE", file_name)

            status_text.empty()
            progress_bar.empty()
            st.success("✅ Validation Package generated successfully.")

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
            log_audit(user, "ANALYSIS_ERROR", "SOP", str(e)[:200])
            st.error(f"❌ Engineering Error: {str(e)}")

# =============================================================================
# 12. ROUTER
# =============================================================================
if not st.session_state.authenticated:
    show_login()
else:
    show_app()