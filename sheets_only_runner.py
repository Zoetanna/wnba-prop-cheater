#!/usr/bin/env python3
import os, sys, json, pandas as pd, numpy as np

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
OUT = os.getenv("OUTPUT_DIR", "./out")
os.makedirs(OUT, exist_ok=True)

def _get_creds(scopes):
    from google.oauth2.service_account import Credentials
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if cred_path and os.path.exists(cred_path):
        return Credentials.from_service_account_file(cred_path, scopes=scopes)
    if GOOGLE_SERVICE_ACCOUNT_JSON:
        return Credentials.from_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), scopes=scopes)
    raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_APPLICATION_CREDENTIALS")

def _read_tab(tab):
    import gspread
    from gspread_dataframe import get_as_dataframe
    creds = _get_creds(["https://www.googleapis.com/auth/spreadsheets.readonly"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.worksheet(tab)
    df = get_as_dataframe(ws, evaluate_formulas=True, header=0)
    return df.dropna(how="all")

def _optional_tab(tab):
    try: return _read_tab(tab)
    except Exception: return None

if not SHEET_ID: print("ERROR: SHEET_ID missing", file=sys.stderr); sys.exit(1)

lines = _read_tab("lines")
opp   = _read_tab("opponent_per100_last6")
pace  = _read_tab("pace_last6")

lines.columns = [str(c).strip().lower() for c in lines.columns]
need = {"player","team","opponent","prop","line"}
if not need.issubset(set(lines.columns)):
    print("ERROR: 'lines' missing columns:", sorted(list(need-set(lines.columns))), file=sys.stderr); sys.exit(2)

opp.columns = [str(c).strip() for c in opp.columns]
if "TEAM_NAME" not in opp.columns:
    if "Team" in opp.columns: opp.rename(columns={"Team":"TEAM_NAME"}, inplace=True)
    else: print("ERROR: 'opponent_per100_last6' must include TEAM_NAME", file=sys.stderr); sys.exit(2)

pace.columns = [str(c).strip().upper() for c in pace.columns]
if not {"TEAM_NAME","PACE"}.issubset(set(pace.columns)):
    print("ERROR: 'pace_last6' must include TEAM_NAME and PACE", file=sys.stderr); sys.exit(2)

lines.to_csv(os.path.join(OUT, "lines_sheet.csv"), index=False)
opp.to_csv(os.path.join(OUT, "opponent_general_per100_last6.csv"), index=False)
pace[["TEAM_NAME","PACE"]].to_csv(os.path.join(OUT, "pace_last6.csv"), index=False)

for tab, fname in [
    ("four_factors_last6", "four_factors_all_last6.csv"),
    ("archetypes", "archetypes.csv"),
    ("players_baseline", "players_baseline.csv"),
    ("on_off", "on_off.csv"),
    ("status_rest", "status_rest.csv"),
    ("positional_defense", "positional_defense.csv"),
]:
    df = _optional_tab(tab)
    if df is not None and not df.dropna(how="all").empty:
        df.to_csv(os.path.join(OUT, fname), index=False)

print("Sheets-only runner completed. Context exported to ./out")
