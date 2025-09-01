#!/usr/bin/env python3
"""
ensure_player_game_log_tab.py
Creates or updates a 'player_game_log' tab in your Google Sheet from a CSV template.
Retries on transient Google API errors (429/500/502/503/504).

Env vars:
  SHEET_ID                       (required)
  GOOGLE_SERVICE_ACCOUNT_JSON    (required)  # full JSON content
  LOG_TEMPLATE                   (default: player_game_log_template.csv)
  SHEET_TAB_LOGS                 (default: player_game_log)
  FORCE_LOGS                     (default: '0')  # set to '1' to overwrite if tab exists
  GSHEET_RETRY_ATTEMPTS          (default: 6)
  GSHEET_RETRY_BASE              (default: 2.0)
"""
import os, sys, json, time, random, pandas as pd

SHEET_ID = os.getenv("SHEET_ID", "").strip()
TAB = os.getenv("SHEET_TAB_LOGS", "player_game_log").strip()
TEMPLATE_PATH = os.getenv("LOG_TEMPLATE", "player_game_log_template.csv").strip()
FORCE = os.getenv("FORCE_LOGS", "0").strip() == "1"

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

RETRY_CODES = {429, 500, 502, 503, 504}
def _should_retry_apierror(e):
    try:
        from gspread import exceptions as gex
        if isinstance(e, gex.APIError):
            resp = getattr(e, "response", None)
            if resp is not None:
                code = getattr(resp, "status_code", None)
                if code in RETRY_CODES: return True, code
            msg = " ".join(str(x) for x in e.args)
            for c in RETRY_CODES:
                if str(c) in msg: return True, c
            return True, None
    except Exception:
        pass
    return False, None

def with_retries(fn, *args, **kwargs):
    attempts = int(os.getenv("GSHEET_RETRY_ATTEMPTS","6"))
    base = float(os.getenv("GSHEET_RETRY_BASE","2.0"))
    for i in range(1, attempts+1):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            do_retry, code = _should_retry_apierror(e)
            if not do_retry:
                raise
            sleep_s = base ** min(i, 6) + random.uniform(0, 0.5*base)
            print(f"[retry {i}/{attempts}] transient Google API error ({code}); sleeping {sleep_s:.1f}s...", file=sys.stderr)
            time.sleep(sleep_s)
    return fn(*args, **kwargs)

def _open_sheet():
    import gspread
    creds = _get_creds(["https://www.googleapis.com/auth/spreadsheets"])
    return with_retries(lambda: gspread.authorize(creds).open_by_key(SHEET_ID))

def upsert(df: pd.DataFrame, title: str):
    from gspread_dataframe import set_with_dataframe
    sh = _open_sheet()
    try:
        ws = with_retries(sh.worksheet, title)
        if FORCE:
            with_retries(ws.clear)
            with_retries(set_with_dataframe, ws, df, include_index=False, include_column_header=True, resize=True)
            print(f"Refreshed tab: {title} ({len(df)} rows)")
        else:
            print(f"Tab '{title}' already exists; set FORCE_LOGS=1 to overwrite. (No changes)")
    except Exception:
        rows = str(max(200, len(df)+50)); cols = str(max(20, len(df.columns)+5))
        ws = with_retries(sh.add_worksheet, title=title, rows=rows, cols=cols)
        with_retries(set_with_dataframe, ws, df, include_index=False, include_column_header=True, resize=True)
        print(f"Created tab: {title} ({len(df)} rows)")

df = pd.read_csv(TEMPLATE_PATH)
need = ["date","player","team","min","pts","reb","ast","stl","blk","tov","fga","fg3a","fta"]
missing = [c for c in need if c not in df.columns]
if missing:
    print(f"ERROR: template missing columns: {missing}", file=sys.stderr); sys.exit(3)

upsert(df, TAB)
print("Done ensure_player_game_log_tab (with retries)")
