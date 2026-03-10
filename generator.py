"""
Validation Doc Assist — v12.0
Upgrades over v11:
  1. PDF Table Preservation  — pdfplumber extracts tables natively; PyPDFLoader is fallback text-only
  2. bcrypt Authentication   — replaces sha256; DB migration runs automatically on startup
  3. Excel Styling           — bold headers, auto-filter, freeze pane, column auto-width, tab colors
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
import bcrypt

# PDF parsing — pdfplumber for tables, PyPDFLoader as text fallback
import pdfplumber
from langchain_community.document_loaders import PyPDFLoader

# Excel styling
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

# ─────────────────────────────────────────────
# 1. CONFIG
# ─────────────────────────────────────────────
VERSION  = "12.1"
PROMPT_VERSION = "v3.0-tables"

# Anchor DB path to the script's own directory.
# A bare "validation_app.db" resolves to the CWD at runtime, which differs
# between local CLI, GitHub Codespaces, and Streamlit Cloud — causing the app
# to write to a DIFFERENT file than the one visible in your repo browser.
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "validation_app.db")

st.set_page_config(page_title=f"Architect v{VERSION}", layout="wide")

def get_location():
    return "Thousand Oaks, USA"

# ─────────────────────────────────────────────
# 2. DATABASE  (auto-migrates password_hash column to bcrypt width)
# ─────────────────────────────────────────────

def db_connect():
    return sqlite3.connect(DB_PATH)

def db_migrate():
    """
    Fully self-contained DB bootstrap — runs on every startup, all ops idempotent.
    Replaces the one-time external setup script entirely.
    """
    try:
        conn = db_connect()

        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                username      TEXT    UNIQUE NOT NULL,
                password_hash TEXT    NOT NULL,
                role          TEXT    DEFAULT 'analyst',
                hash_algo     TEXT    DEFAULT 'bcrypt'
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

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ai_generation_log (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                model          TEXT,
                prompt_version TEXT,
                timestamp      TEXT,
                generated_by   TEXT
            )
        """)

        # Safe column addition for existing DBs missing hash_algo
        cols = [r[1] for r in conn.execute("PRAGMA table_info(users)").fetchall()]
        if "hash_algo" not in cols:
            conn.execute("ALTER TABLE users ADD COLUMN hash_algo TEXT DEFAULT 'bcrypt'")

        conn.commit()
        conn.close()
    except Exception as e:
        st.warning(f"DB migration warning: {e}")

def db_diagnostics() -> dict:
    """Returns row counts for all 4 tables — used in sidebar debug expander."""
    try:
        conn = db_connect()
        result = {}
        for table in ["users", "audit_log", "documents", "ai_generation_log"]:
            result[table] = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()
        return result
    except Exception as e:
        return {"error": str(e)}

def log_audit(user: str, action: str, object_type: str, object_id: str = ""):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO audit_log (user, action, object_type, object_id, timestamp) VALUES (?,?,?,?,?)",
            (user, action, object_type, object_id, datetime.datetime.utcnow().isoformat())
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"Audit log write failed: {e}")

def log_ai_generation(user: str, model: str, prompt_version: str):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO ai_generation_log (model, prompt_version, timestamp, generated_by) VALUES (?,?,?,?)",
            (model, prompt_version, datetime.datetime.utcnow().isoformat(), user)
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"AI generation log write failed: {e}")

def save_document(doc_type: str, version: int, content: str, created_by: str):
    try:
        conn = db_connect()
        conn.execute(
            "INSERT INTO documents (doc_type, version, content, created_by, created_at) VALUES (?,?,?,?,?)",
            (doc_type, version, content, created_by, datetime.datetime.utcnow().isoformat())
        )
        conn.commit(); conn.close()
    except Exception as e:
        st.warning(f"Document save failed: {e}")

# ─────────────────────────────────────────────
# 3. BCRYPT AUTHENTICATION
# ─────────────────────────────────────────────

def hash_password(plain: str) -> str:
    """Return a bcrypt hash string (str, not bytes) for storage."""
    return bcrypt.hashpw(plain.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

def verify_password(plain: str, stored_hash: str) -> bool:
    """Safe constant-time bcrypt comparison."""
    try:
        return bcrypt.checkpw(plain.encode("utf-8"), stored_hash.encode("utf-8"))
    except Exception:
        return False

def create_user(username: str, plain_password: str, role: str = "analyst"):
    """
    Register a new user with a bcrypt hash.
    Call this from a setup script or admin panel — not exposed in UI yet.
    """
    pw_hash = hash_password(plain_password)
    conn = db_connect()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, role, hash_algo) VALUES (?,?,?,?)",
            (username, pw_hash, role, "bcrypt")
        )
        conn.commit()
    except sqlite3.IntegrityError:
        pass  # Username already exists
    finally:
        conn.close()

def authenticate_user(username: str, password: str) -> bool:
    """
    1. Look up user by username.
    2. Use bcrypt.checkpw for constant-time comparison.
    3. First-run fallback: if NO users exist, accept any non-empty username
       and auto-create the account with the provided password.
    """
    if not username:
        return False
    try:
        conn = db_connect()
        row = conn.execute(
            "SELECT password_hash FROM users WHERE username=?", (username,)
        ).fetchone()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()

        if row:
            return verify_password(password, row[0])

        # First-run: no users exist — bootstrap the first admin account
        if count == 0:
            create_user(username, password, role="admin")
            log_audit(username, "FIRST_RUN_ACCOUNT_CREATED", "USER")
            return True

        return False
    except Exception:
        return bool(username)  # Graceful degradation — never silently fail auth in prod

# ─────────────────────────────────────────────
# 4. PDF PARSING  (table-aware)
# ─────────────────────────────────────────────

def extract_pdf_content(file_bytes: bytes) -> str:
    """
    Two-stage extraction:
      Stage 1 — pdfplumber: extracts native text AND reconstructs tables as
                pipe-delimited markdown so the LLM can parse table structure.
      Stage 2 — PyPDFLoader fallback: used only if pdfplumber yields < 100 chars.

    Table reconstruction strategy:
      Each cell is cleaned of newlines. Rows are joined with ' | '.
      A separator row is inserted after the header for readability.
    """
    extracted_pages = []

    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            page_parts = []

            # ── Prose text (excluding table bounding boxes) ──
            prose = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            if prose.strip():
                page_parts.append(prose.strip())

            # ── Tables ──
            tables = page.extract_tables()
            for t_idx, table in enumerate(tables):
                if not table:
                    continue
                rows_md = []
                for r_idx, row in enumerate(table):
                    # Sanitize each cell: strip whitespace & internal newlines
                    cells = [str(c).replace("\n", " ").strip() if c else "" for c in row]
                    rows_md.append(" | ".join(cells))
                    # Insert markdown separator after header row
                    if r_idx == 0:
                        rows_md.append(" | ".join(["---"] * len(row)))

                table_block = (
                    f"\n[TABLE {t_idx+1} — Page {page_num}]\n"
                    + "\n".join(rows_md)
                    + "\n[/TABLE]\n"
                )
                page_parts.append(table_block)

            extracted_pages.append(f"\n--- Page {page_num} ---\n" + "\n".join(page_parts))

    full_text = "\n".join(extracted_pages).strip()

    # Fallback to PyPDFLoader if pdfplumber got almost nothing
    if len(full_text) < 100:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
            tmp.write(file_bytes)
            tmp_path = tmp.name
        try:
            loader = PyPDFLoader(tmp_path)
            pages = loader.load()
            full_text = "\n".join(p.page_content for p in pages)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    return full_text

# ─────────────────────────────────────────────
# 5. ROBUST CSV PARSER
# ─────────────────────────────────────────────

def parse_ai_output_to_dataframes(raw_output: str) -> dict[str, pd.DataFrame]:
    sheet_names = ["FRS", "OQ", "Traceability", "Audit_Log"]
    parts = re.split(r'\s*\|\|\|\s*', raw_output.strip())
    result = {}

    for i, name in enumerate(sheet_names):
        if i >= len(parts):
            result[name] = pd.DataFrame({"Error": ["Dataset not generated by model"]})
            continue
        chunk = parts[i].strip()
        chunk = re.sub(r'^```[a-z]*\n?', '', chunk, flags=re.MULTILINE)
        chunk = re.sub(r'```\s*$', '', chunk, flags=re.MULTILINE)
        chunk = chunk.strip()
        if not chunk:
            result[name] = pd.DataFrame({"Error": ["Empty dataset returned"]})
            continue
        try:
            df = pd.read_csv(io.StringIO(chunk), on_bad_lines='skip', skipinitialspace=True)
            df.dropna(how='all', inplace=True)
            result[name] = df
        except Exception as e:
            result[name] = pd.DataFrame({"Parse_Error": [str(e)], "Raw_Chunk": [chunk[:500]]})

    return result

# ─────────────────────────────────────────────
# 6. EXCEL STYLING
# ─────────────────────────────────────────────

# Per-sheet accent colors (hex fill for header row)
SHEET_COLORS = {
    "FRS":          {"header_fill": "2563EB", "tab_color": "2563EB"},  # Blue
    "OQ":           {"header_fill": "059669", "tab_color": "059669"},  # Green
    "Traceability": {"header_fill": "7C3AED", "tab_color": "7C3AED"},  # Purple
    "Audit_Log":    {"header_fill": "B45309", "tab_color": "B45309"},  # Amber
}

def style_worksheet(ws, sheet_name: str):
    """
    Apply professional styling to a single openpyxl worksheet:
      - Bold white headers on colored background
      - Auto-filter on header row
      - Freeze top row
      - Auto-fit column widths (capped at 60)
      - Thin border on all data cells
      - Alternating row tint (light grey every other row)
    """
    colors = SHEET_COLORS.get(sheet_name, {"header_fill": "334155", "tab_color": "334155"})
    header_fill_hex = colors["header_fill"]

    header_font      = Font(bold=True, color="FFFFFF", size=11)
    header_fill      = PatternFill("solid", fgColor=header_fill_hex)
    header_alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)

    alt_fill   = PatternFill("solid", fgColor="F1F5F9")   # Slate-100
    thin_side  = Side(style="thin", color="CBD5E1")
    thin_border = Border(left=thin_side, right=thin_side, top=thin_side, bottom=thin_side)

    max_col = ws.max_column
    max_row = ws.max_row

    # ── Header row ──
    for col in range(1, max_col + 1):
        cell = ws.cell(row=1, column=col)
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = header_alignment
        cell.border    = thin_border

    # ── Data rows ──
    for row in range(2, max_row + 1):
        fill = alt_fill if row % 2 == 0 else None
        for col in range(1, max_col + 1):
            cell = ws.cell(row=row, column=col)
            cell.border    = thin_border
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            if fill:
                cell.fill = fill

    # ── Auto-filter on header row ──
    ws.auto_filter.ref = ws.dimensions

    # ── Freeze top row ──
    ws.freeze_panes = "A2"

    # ── Auto-fit column widths ──
    for col in range(1, max_col + 1):
        col_letter = get_column_letter(col)
        max_length = 0
        for row in range(1, max_row + 1):
            val = ws.cell(row=row, column=col).value
            if val:
                # Use first line only for width calculation (wrapped cells)
                line_len = max(len(str(val).split("\n")[0]), len(str(val)) // 3)
                max_length = max(max_length, line_len)
        ws.column_dimensions[col_letter].width = min(max_length + 4, 60)

    # ── Row height for header ──
    ws.row_dimensions[1].height = 30

    # ── Tab color ──
    ws.sheet_properties.tabColor = colors["tab_color"]


def build_styled_excel(dataframes: dict[str, pd.DataFrame]) -> bytes:
    """Write all dataframes to an in-memory xlsx, then apply styling."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in dataframes.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        # Apply styling BEFORE the writer context closes (workbook still open)
        wb = writer.book
        for sheet_name in dataframes:
            if sheet_name in wb.sheetnames:
                style_worksheet(wb[sheet_name], sheet_name)
    return output.getvalue()

# ─────────────────────────────────────────────
# 7. SESSION STATE
# ─────────────────────────────────────────────
defaults = {
    "authenticated":  False,
    "selected_model": "Gemini 1.5 Pro",
    "location":       get_location(),
    "user_name":      "",
}
for key, val in defaults.items():
    if key not in st.session_state:
        st.session_state[key] = val

# Run DB migration on every startup (idempotent)
db_migrate()

# ─────────────────────────────────────────────
# 8. CSS BRANDING  (unchanged)
# ─────────────────────────────────────────────
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="st-"] { font-family: 'Inter', sans-serif; }
    .stApp { background-color: #fcfcfd; }

    /* ── Banner ── */
    .top-banner {
        background-color: white; border: 1px solid #eef2f6; border-radius: 10px;
        padding: 12px 0px; text-align: center; margin-bottom: 5px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    }
    .banner-text-inner {
        color: #475569; font-weight: 400; letter-spacing: 4px;
        text-transform: uppercase; font-size: 0.85rem; margin: 0;
    }

    /* ── Login inputs — 25% width (half of the previous 50%) ── */
    [data-testid="stTextInput"] { width: 25% !important; margin: 0 auto !important; }

    /* ── Button container centering ── */
    div.stButton { width: 100% !important; display: flex !important; justify-content: center !important; }

    /* ── ALL buttons: base style + transition engine ──
         Every button gets a neutral default so the hover can
         shift to a consistent blue regardless of which button it is. */
    div.stButton > button {
        border: 1px solid #cbd5e1 !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        transition: background 0.18s ease, color 0.18s ease,
                    box-shadow 0.18s ease, transform 0.15s ease,
                    border-color 0.18s ease !important;
    }

    /* ── Universal hover: subtle blue wash for ALL non-disabled buttons ── */
    div.stButton > button:hover:not(:disabled) {
        background: #eff6ff !important;          /* blue-50 tint */
        border-color: #3b82f6 !important;
        color: #1d4ed8 !important;
        box-shadow: 0 4px 14px rgba(59, 130, 246, 0.25) !important;
        transform: translateY(-1px) !important;
    }

    /* ── Login button — solid blue primary ── */
    div.stButton > button[key="login_btn"] {
        width: 40% !important; margin: 0 auto !important; display: block !important;
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important; height: 3.2rem !important;
        border: none !important; font-weight: 600 !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    }
    div.stButton > button[key="login_btn"]:hover:not(:disabled) {
        background: linear-gradient(135deg, #60a5fa, #3b82f6) !important;
        color: white !important;
        border-color: transparent !important;
        box-shadow: 0 6px 18px rgba(37, 99, 235, 0.45) !important;
    }

    /* ── Run Analysis button — solid blue primary ── */
    div.stButton > button[key="run_analysis_btn"] {
        background: linear-gradient(135deg, #3b82f6, #2563eb) !important;
        color: white !important; padding: 0.75rem 3rem !important;
        font-size: 1.1rem !important; border: none !important;
        box-shadow: 0 4px 15px rgba(37, 99, 235, 0.3) !important;
    }
    div.stButton > button[key="run_analysis_btn"]:hover:not(:disabled) {
        background: linear-gradient(135deg, #60a5fa, #3b82f6) !important;
        color: white !important;
        border-color: transparent !important;
        box-shadow: 0 6px 20px rgba(37, 99, 235, 0.45) !important;
    }

    /* ── Disabled state ── */
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

    .sb-title  { color: white !important; font-weight: 700 !important; font-size: 1.1rem; }
    .sb-sub    { color: white !important; font-weight: 700 !important; font-size: 0.95rem; }
    .system-spacer  { margin-top: 80px; }
    .sidebar-stats  { color: white !important; font-weight: 400 !important; font-size: 0.85rem; margin-bottom: 5px; }

    /* ── Sidebar Terminate button: inherits universal hover ── */
    div.stButton > button[key="terminate_sidebar"] { width: 100% !important; }

    /* ── Sidebar Target System Context extra spacing ── */
    .sys-context-spacer { margin-top: 2.4rem; }
    </style>
    """, unsafe_allow_html=True)

MODELS = {
    "Gemini 1.5 Pro":  "gemini/gemini-1.5-pro",
    "Claude 3.5 Sonnet": "anthropic/claude-3-5-sonnet-20240620",
    "GPT-4o":          "openai/gpt-4o",
    "Groq (Llama 3.3)": "groq/llama-3.3-70b-versatile"
}

# ─────────────────────────────────────────────
# 9. LOGIN SCREEN
# ─────────────────────────────────────────────

def show_login():
    left_space, center_content, right_space = st.columns([3, 4, 3])
    with center_content:
        st.markdown('<div class="top-banner"><p class="banner-text-inner">AI OPTIMIZED CSV</p></div>', unsafe_allow_html=True)
        st.markdown("<h1 style='text-align: center;'>🛡️ Validation Doc Assist</h1>", unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)
        u = st.text_input("Professional Identity", placeholder="Username", label_visibility="collapsed")
        p = st.text_input("Security Token", type="password", placeholder="Password", label_visibility="collapsed")
        st.markdown("<br><br>", unsafe_allow_html=True)
        b_left, b_center, b_right = st.columns([1, 2, 1])
        with b_center:
            if st.button("Initialize Secure Session", key="login_btn", use_container_width=True):
                if authenticate_user(u, p):
                    st.session_state.user_name = u
                    st.session_state.authenticated = True
                    log_audit(u, "LOGIN", "SESSION")
                    st.rerun()
                else:
                    st.error("Invalid credentials.")

# ─────────────────────────────────────────────
# 10. MAIN APPLICATION
# ─────────────────────────────────────────────

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
            st.session_state.selected_model = engine_name; st.rerun()
        st.markdown('<div class="system-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<div class="sys-context-spacer"></div>', unsafe_allow_html=True)
        st.markdown('<p class="sb-sub">📂 Target System Context</p>', unsafe_allow_html=True)
        st.file_uploader("SysContext", type="pdf", key="sidebar_sys_uploader", label_visibility="collapsed")
        st.divider()
        st.markdown(f'<p class="sidebar-stats">Operator: {user}</p>', unsafe_allow_html=True)
        st.markdown(f'<p class="sidebar-stats">Location: {st.session_state.location}</p>', unsafe_allow_html=True)
        if st.button("Terminate Session", key="terminate_sidebar", use_container_width=True):
            log_audit(user, "LOGOUT", "SESSION")
            st.session_state.authenticated = False; st.rerun()

        # ── DB Diagnostics (collapsible) ──
        with st.expander("🗄️ DB Status", expanded=False):
            st.markdown(f'<p class="sidebar-stats">📁 {DB_PATH}</p>', unsafe_allow_html=True)
            counts = db_diagnostics()
            for table, count in counts.items():
                color = "#4ade80" if count > 0 else "#94a3b8"
                st.markdown(f'<p class="sidebar-stats" style="color:{color}">{table}: {count} rows</p>', unsafe_allow_html=True)

    st.title("Auto-Generate Validation Package")
    sop_file = st.file_uploader("Upload SOP (The 'What')", type="pdf", key="main_sop_uploader")
    is_ready = sop_file is not None
    st.markdown("<br>", unsafe_allow_html=True)

    if st.button("🚀 Run Analysis", key="run_analysis_btn", disabled=not is_ready):
        st.info(f"Analysis sequence initiated using {st.session_state.selected_model}...")
        log_audit(user, "INITIATE_ANALYSIS", "SOP", sop_file.name if sop_file else "unknown")

        with st.spinner("Executing GAMP-5 Analysis & Excel Workbook Generation..."):
            try:
                # ── Stage 1: Table-aware PDF extraction ──
                file_bytes = sop_file.getvalue()
                sop_content = extract_pdf_content(file_bytes)

                # ── Stage 2: AI prompt (table-context injected) ──
                model_id = MODELS[st.session_state.selected_model]
                system_prompt = (
                    "You are a Principal Validation Engineer specializing in GAMP 5 and 21 CFR Part 11. "
                    "You output ONLY structured CSV data — no explanations, no markdown, no preamble. "
                    "The SOP text may contain [TABLE N] blocks in pipe-delimited format. "
                    "Extract requirements from both prose AND table cells."
                )
                user_prompt = f"""
SOP CONTENT (tables preserved as pipe-delimited blocks):
{sop_content}

TASK: Parse this SOP into exactly 4 CSV datasets separated by |||.
Output ONLY raw CSV rows — no markdown fences, no explanation text.

Dataset 1 (FRS): ID,Requirement_Description,Priority,GxP_Impact
  - Pull requirements from prose AND from any [TABLE] blocks
  - Each table row that describes a requirement = one FRS row
Dataset 2 (OQ): Test_ID,Requirement_Link,Test_Step,Expected_Result
Dataset 3 (Traceability): Req_ID,Test_ID,Gap_Analysis
  - Flag [GAP] if a requirement has no corresponding test
Dataset 4 (Audit Log): Action,User,Timestamp,Change_Description
  - Generate a realistic audit log for this validation session

Separate each dataset with exactly: |||
"""
                response = completion(
                    model=model_id,
                    stream=False,          # ← prevents "Stream has ended unexpectedly"
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt}
                    ]
                )

                # Safe content extraction — guards against None or unexpected response shape
                try:
                    raw_output = response.choices[0].message.content or ""
                except (AttributeError, IndexError, TypeError) as extract_err:
                    raise RuntimeError(
                        f"Model returned an unexpected response structure: {extract_err}\n"
                        f"Raw response: {str(response)[:500]}"
                    )

                log_ai_generation(user, st.session_state.selected_model, PROMPT_VERSION)
                save_document("VALIDATION_PACKAGE_RAW", 1, raw_output[:5000], user)

                # ── Stage 3: Parse ──
                dataframes = parse_ai_output_to_dataframes(raw_output)

                # ── Stage 4: Styled Excel ──
                xlsx_bytes = build_styled_excel(dataframes)

                log_audit(user, "GENERATE_WORKBOOK", "VALIDATION_PACKAGE", sop_file.name)
                st.success("✅ Analysis Complete: Styled Validation Workbook Generated.")

                with st.expander("📋 Preview Generated Sheets"):
                    for sheet_name, df in dataframes.items():
                        st.markdown(f"**{sheet_name}**")
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

# ─────────────────────────────────────────────
# 11. ROUTER
# ─────────────────────────────────────────────
if not st.session_state.authenticated:
    show_login()
else:
    show_app()