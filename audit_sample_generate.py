import csv
import random
from datetime import datetime, timedelta

def generate_audit_trail(filename="labware_comprehensive_test.csv"):
    """
    Generates a 1,000-line high-fidelity LIMS audit trail.
    Injects specific triggers for all Critical, High, Medium, and Low rules.
    """

    # --- CONFIGURATION & LISTS ---
    users = ["analyst_jones", "analyst_brown", "analyst_x", "analyst_y", "analyst_z", "jsmith"]
    admins = ["admin_sys", "dba_prod", "superuser_lims"]
    roles = {u: "Analyst" for u in users}
    roles.update({a: "Admin" for a in admins})

    record_types = ["RESULTS", "BATCH", "SAMPLE_DATA", "BATCH_RELEASE", "TEST", "USER_SESSION", "AUDIT_TRAIL"]
    actions = ["INSERT", "UPDATE", "DELETE", "RESULT_INSERT", "LOGIN", "MODIFY_CONFIG"]
    
    # Standard holidays for Rule 12 (Federal Holidays)
    holidays = [
        (1, 1),   # New Year
        (6, 19),  # Juneteenth
        (7, 4),   # July 4
        (11, 11), # Veterans
        (12, 25), # Christmas
        # Dynamic holidays (2026 examples)
        (1, 19),  # MLK
        (2, 16),  # Presidents
        (5, 25),  # Memorial
        (9, 7),   # Labor
        (10, 12), # Columbus
        (11, 26)  # Thanksgiving
    ]

    base_time = datetime(2026, 3, 23, 8, 0, 0) # Start on a Monday
    rows = []

    # Helper to format timestamp
    def ts_fmt(dt): return dt.strftime("%Y-%m-%d %H:%M:%S")

    # --- 1. GENERATE BASELINE DATA (ROUTINE ACTIVITY) ---
    for i in range(1000):
        current_dt = base_time + timedelta(minutes=i * 5)
        user = random.choice(users)
        rows.append({
            "timestamp": ts_fmt(current_dt),
            "user_id": user,
            "action_type": "UPDATE",
            "record_type": "RESULTS",
            "role": roles[user],
            "record_id": f"RES-{1000 + i}",
            "comments": "Standard value entry per SOP-01",
            "new_value": str(round(random.uniform(7.0, 7.5), 2))
        })

    # --- 2. INJECT SPECIFIC RED FLAG SCENARIOS (OVERWRITING INDEXES) ---

    # RULE 3: Admin/GxP Conflict (CRITICAL)
    rows[10] = {
        "timestamp": ts_fmt(base_time + timedelta(hours=1)),
        "user_id": "admin_sys",
        "action_type": "INSERT",
        "record_type": "BATCH_RELEASE",
        "role": "Admin",
        "record_id": "BATCH-999",
        "comments": "Urgent release",
        "new_value": "RELEASED"
    }

    # RULE 5: Failed Login -> Data Manipulation (CRITICAL)
    login_fail_time = base_time + timedelta(hours=2)
    rows[20] = {"timestamp": ts_fmt(login_fail_time), "user_id": "analyst_x", "action_type": "LOGIN_FAILED", "record_type": "USER_SESSION", "role": "Analyst", "record_id": "SES-01", "comments": "Wrong password", "new_value": ""}
    rows[21] = {"timestamp": ts_fmt(login_fail_time + timedelta(minutes=3)), "user_id": "analyst_x", "action_type": "LOGIN_FAILED", "record_type": "USER_SESSION", "role": "Analyst", "record_id": "SES-01", "comments": "Wrong password", "new_value": ""}
    rows[22] = {"timestamp": ts_fmt(login_fail_time + timedelta(minutes=6)), "user_id": "analyst_x", "action_type": "LOGIN_FAILED", "record_type": "USER_SESSION", "role": "Analyst", "record_id": "SES-01", "comments": "Wrong password", "new_value": ""}
    rows[23] = {"timestamp": ts_fmt(login_fail_time + timedelta(minutes=8)), "user_id": "analyst_x", "action_type": "LOGIN", "record_type": "USER_SESSION", "role": "Analyst", "record_id": "SES-01", "comments": "Success", "new_value": ""}
    rows[24] = {"timestamp": ts_fmt(login_fail_time + timedelta(minutes=15)), "user_id": "analyst_x", "action_type": "DELETE", "record_type": "RESULTS", "role": "Analyst", "record_id": "RES-5050", "comments": "Cleaning up error", "new_value": ""}

    # AUDIT INTEGRITY BREACH (CRITICAL)
    rows[50] = {
        "timestamp": ts_fmt(base_time + timedelta(hours=5)),
        "user_id": "dba_prod",
        "action_type": "UPDATE",
        "record_type": "AUDIT_TRAIL",
        "role": "Admin",
        "record_id": "SYS-001",
        "comments": "System maintenance",
        "new_value": "DISABLED"
    }

    # DELETE -> RECREATE (CRITICAL)
    rows[100] = {"timestamp": ts_fmt(base_time + timedelta(hours=10)), "user_id": "analyst_y", "action_type": "DELETE", "record_type": "RESULTS", "role": "Analyst", "record_id": "RES-8888", "comments": "Error", "new_value": ""}
    rows[105] = {"timestamp": ts_fmt(base_time + timedelta(hours=10, minutes=15)), "user_id": "analyst_y", "action_type": "INSERT", "record_type": "RESULTS", "role": "Analyst", "record_id": "RES-8888", "comments": "Correction", "new_value": "7.2"}

    # RULE 1: Vague Rationale (HIGH)
    rows[150]["comments"] = "" # Empty
    rows[151]["comments"] = "fixed" # Vague
    rows[152]["comments"] = "ok" # Short

    # RULE 4: Change Control Drift (HIGH)
    # Most RESULTS are 7.0-7.5. Let's make one huge.
    rows[200] = {**rows[200], "record_type": "RESULTS", "new_value": "147.3", "comments": "Outlier test"}

    # RULE 2: Contemporaneous Burst (MEDIUM)
    burst_time = base_time + timedelta(hours=15)
    for j in range(12):
        rows[300 + j] = {
            "timestamp": ts_fmt(burst_time + timedelta(seconds=j * 10)),
            "user_id": "analyst_jones",
            "action_type": "RESULT_INSERT",
            "record_type": "RESULTS",
            "role": "Analyst",
            "record_id": f"RES-BURST-{j}",
            "comments": "Bulk upload",
            "new_value": "7.2"
        }

    # OFF-HOURS & DEEP NIGHT (MEDIUM)
    rows[400] = {**rows[400], "timestamp": ts_fmt(base_time.replace(hour=23, minute=30)), "comments": "Late night check"}
    rows[401] = {**rows[401], "timestamp": ts_fmt(base_time.replace(hour=2, minute=14)), "comments": "Insomnia entry"}

    # HOLIDAY (MEDIUM)
    holiday_dt = datetime(2026, 7, 4, 11, 0, 0)
    rows[500] = {**rows[500], "timestamp": ts_fmt(holiday_dt), "comments": "Holiday work"}

    # WEEKEND (LOW)
    weekend_dt = datetime(2026, 3, 28, 10, 0, 0) # Saturday
    rows[600] = {**rows[600], "timestamp": ts_fmt(weekend_dt), "comments": "Weekend catchup"}

    # TIMESTAMP GAP (LOW)
    # Corrected: removed invalid 'hour' and 'minute' args from timedelta
    rows[700]["timestamp"] = ts_fmt(base_time + timedelta(days=2) + timedelta(hours=9))
    rows[701]["timestamp"] = ts_fmt(base_time + timedelta(days=2) + timedelta(hours=12)) # 3 hour gap

    # SENSITIVE RECORD ACCESS (LOW)
    rows[800] = {
        "timestamp": ts_fmt(base_time + timedelta(days=3)),
        "user_id": "analyst_jones",
        "action_type": "SELECT",
        "record_type": "AUDIT_TRAIL",
        "role": "Analyst",
        "record_id": "N/A",
        "comments": "Reviewing logs",
        "new_value": ""
    }

    # --- WRITE TO CSV ---
    fieldnames = ["timestamp", "user_id", "action_type", "record_type", "role", "record_id", "comments", "new_value"]
    with open(filename, mode='w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        # Sort by timestamp to keep chronology realistic
        rows.sort(key=lambda x: x['timestamp'])
        writer.writerows(rows)

    print(f"SUCCESS: Generated 1000 lines.")
    print(f"Target file: {filename}")

if __name__ == "__main__":
    generate_audit_trail()