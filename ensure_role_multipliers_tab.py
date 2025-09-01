#!/usr/bin/env python3
# ensure_role_multipliers_tab.py
# Creates/refreshes a 'role_multipliers' tab in your Google Sheet from a CSV template.
import os, sys, json, pandas as pd

SHEET_ID = os.getenv("SHEET_ID", "").strip()
TAB = os.getenv("SHEET_TAB_ROLE_MULT", "role_multipliers").strip()
TEMPLATE_PATH = os.getenv("ROLE_MULT_TEMPLATE", "role_multipliers_template.csv").strip()
FORCE = os.getenv("FORCE_ROLE_MULTIPLIERS", "0").strip() == "1"

if not SHEET_ID:
    print("ERROR: SHEET_ID missing", file=sys.stderr); sys.exit(1)
if not os.path.exists(TEMPLATE_PATH):
    print(f"ERROR: template not found at {TEMPLATE_PATH}", file=sys.stderr); sys.exit(2)

def _get_creds(scopes):
    from google.oauth2.service_account import Credentials
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        return Credentials.from_service_account_file(cred_path, scopes=scopes)
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON is empty")
    return Credentials.from_service_account_info(json.loads(raw), scopes=scopes)

def _open_sheet():
    import gspread
    creds = _get_creds(["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def upsert(df, title):
    from gspread_dataframe import set_with_dataframe
    sh = _open_sheet()
    try:
        ws = sh.worksheet(title)
        if FORCE:
            ws.clear()
            set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
            print(f"Refreshed tab: {title} ({len(df)} rows)")
        else:
            print(f"Tab '{title}' already exists; set FORCE_ROLE_MULTIPLIERS=1 to overwrite.")
    except Exception:
        ws = sh.add_worksheet(title=title, rows=str(max(200, len(df)+50)), cols=str(max(20, len(df.columns)+5)))
        set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
        print(f"Created tab: {title} ({len(df)} rows)")

df = pd.read_csv(TEMPLATE_PATH)
need = ["role_type","role_name","points","rebounds","assists","threes","steals","ftm"]
missing = [c for c in need if c not in df.columns]
if missing:
    print(f"ERROR: template missing columns: {missing}", file=sys.stderr); sys.exit(3)
upsert(df, TAB)
print("Done ensure_role_multipliers_tab")
