#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_wnba_to_gsheets_v4.py
- Writes outputs back to Google Sheets (Sheets-only flow)
- Always writes: player_prop_projections (from ./out/player_prop_projections.csv)
- Optionally writes suggestions if present
"""

import os, sys, json, pandas as pd

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
OUT = os.getenv("OUTPUT_DIR", "./out").strip()

TAB_PROJ = os.getenv("SHEET_TAB_PROJECTIONS", "player_prop_projections").strip()
TAB_SUGG = os.getenv("SHEET_TAB_SUGG", "suggestions").strip()

if not SHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
    print("ERROR: Missing SHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON", file=sys.stderr)
    sys.exit(1)

def _get_creds(scopes):
    from google.oauth2.service_account import Credentials
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        return Credentials.from_service_account_file(cred_path, scopes=scopes)
    return Credentials.from_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=scopes)

def _open_sheet():
    import gspread
    creds = _get_creds(["https://www.googleapis.com/auth/spreadsheets"])
    return gspread.authorize(creds).open_by_key(SHEET_ID)

def upsert_tab(df: pd.DataFrame, title: str):
    import gspread
    from gspread_dataframe import set_with_dataframe
    sh = _open_sheet()
    try:
        ws = sh.worksheet(title); ws.clear()
    except Exception:
        rows=max(len(df),200); cols=max(len(df.columns),20)
        ws = sh.add_worksheet(title=title, rows=str(rows+50), cols=str(cols+5))
    set_with_dataframe(ws, df.reset_index(drop=False) if df.index.name else df, include_index=False, include_column_header=True, resize=True)
    print(f"Wrote tab: {title} ({len(df)} rows)")

proj_csv = os.path.join(OUT, "player_prop_projections.csv")
if not os.path.exists(proj_csv):
    print("ERROR: player_prop_projections.csv not found in ./out", file=sys.stderr); sys.exit(2)
proj = pd.read_csv(proj_csv)
upsert_tab(proj, TAB_PROJ)

sugg_csv = os.path.join(OUT, "prop_suggestions_allteams.csv")
if os.path.exists(sugg_csv):
    try:
        sugg = pd.read_csv(sugg_csv)
        if not sugg.empty:
            upsert_tab(sugg, TAB_SUGG)
    except Exception as e:
        print("WARN: could not push suggestions:", e)

print("Done.")
