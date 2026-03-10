import sqlite3

conn = sqlite3.connect("validation_app.db")
cursor = conn.cursor()

# Users table
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE,
    password_hash TEXT,
    role TEXT
)
""")

# Audit trail
cursor.execute("""
CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user TEXT,
    action TEXT,
    object_type TEXT,
    object_id TEXT,
    timestamp TEXT
)
""")

# Document versions
cursor.execute("""
CREATE TABLE IF NOT EXISTS documents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    doc_type TEXT,
    version INTEGER,
    content TEXT,
    created_by TEXT,
    created_at TEXT
)
""")

# AI generation log
cursor.execute("""
CREATE TABLE IF NOT EXISTS ai_generation_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    model TEXT,
    prompt_version TEXT,
    timestamp TEXT,
    generated_by TEXT
)
""")

conn.commit()
conn.close()

print("Database initialized")