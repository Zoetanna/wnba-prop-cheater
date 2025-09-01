#!/usr/bin/env python3
import os, sys, json, pandas as pd
SHEET_ID = os.getenv("SHEET_ID","").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
OUT = os.getenv("OUTPUT_DIR","./out").strip()
TAB = os.getenv("SHEET_TAB_PROJECTIONS","player_prop_projections").strip()
CSV = os.path.join(OUT, "player_prop_projections.csv")
if not SHEET_ID or not os.path.exists(CSV): print("ERR: missing SHEET_ID or projections CSV", file=sys.stderr); sys.exit(1)

def _get_creds(scopes):
    from google.oauth2.service_account import Credentials
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        return Credentials.from_service_account_file(cred_path, scopes=scopes)
    return Credentials.from_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=scopes)
def _open(): import gspread; return gspread.authorize(_get_creds(["https://www.googleapis.com/auth/spreadsheets"])).open_by_key(SHEET_ID)
def upsert(df, title):
    from gspread_dataframe import set_with_dataframe
    sh=_open()
    try: ws=sh.worksheet(title); ws.clear()
    except Exception: ws=sh.add_worksheet(title=title, rows=str(max(200,len(df)+50)), cols=str(max(20,len(df.columns)+5)))
    set_with_dataframe(ws, df, include_index=False, include_column_header=True, resize=True)
df=pd.read_csv(CSV); upsert(df, TAB); print("Pushed", len(df), "rows to", TAB)
