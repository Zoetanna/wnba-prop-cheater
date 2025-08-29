#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
push_wnba_to_gsheets_v3.py
- Preferred: read CSVs produced by the full-fat runner and push to Google Sheets
- Fallback: if CSVs are missing, fetch data like v2
- Also writes suggestions (if prop_suggestions_allteams.csv present)

ENV
---
# Google Sheets
SHEET_ID="your_sheet_id"
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account",...}'
SHEET_TAB_FF="four_factors_last6"
SHEET_TAB_OPP="opponent_per100_last6"
SHEET_TAB_PACE="pace_last6"
SHEET_TAB_SUGG="suggestions"

# Runner outputs
OUTPUT_DIR="./out"   # directory that contains runner CSV outputs (downloaded artifact path)

# Fallback fetch (if outputs missing)
SEASON=2025
SEASON_TYPE="Regular Season"
LAST_N_GAMES=6
SEASON_SEGMENT="Last 6 Games"
"""

import os, sys, json, time, random, traceback
import pandas as pd
import numpy as np

SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./out").strip()

SHEET_TAB_FF = os.getenv("SHEET_TAB_FF", "four_factors_last6").strip()
SHEET_TAB_OPP = os.getenv("SHEET_TAB_OPP", "opponent_per100_last6").strip()
SHEET_TAB_PACE = os.getenv("SHEET_TAB_PACE", "pace_last6").strip()
SHEET_TAB_SUGG = os.getenv("SHEET_TAB_SUGG", "suggestions").strip()

SEASON = os.getenv("SEASON", "2025")
SEASON_TYPE = os.getenv("SEASON_TYPE", "Regular Season")
LAST_N_GAMES = int(os.getenv("LAST_N_GAMES", "6"))
SEASON_SEGMENT = os.getenv("SEASON_SEGMENT", "Last 6 Games")

def log(m): print(f"[push_v3] {m}", flush=True)

if not SHEET_ID or not GOOGLE_SERVICE_ACCOUNT_JSON:
    log("ERROR: Missing SHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON")
    sys.exit(1)

# Try to read outputs
ff_csv  = os.path.join(OUTPUT_DIR, "four_factors_all_last6.csv")
opp_csv = os.path.join(OUTPUT_DIR, "opponent_general_per100_last6.csv")
pace_csv= os.path.join(OUTPUT_DIR, "pace_last6.csv")
sugg_csv= os.path.join(OUTPUT_DIR, "prop_suggestions_allteams.csv")

def read_csv_safe(path):
    try:
        if os.path.exists(path):
            df = pd.read_csv(path)
            log(f"Loaded {os.path.basename(path)} [{len(df)} rows]")
            return df
    except Exception as e:
        log(f"WARN: Could not read {path}: {e}")
    return None

df_ff = read_csv_safe(ff_csv)
df_opp = read_csv_safe(opp_csv)
df_pace = read_csv_safe(pace_csv)
df_sugg = read_csv_safe(sugg_csv)

# Fallback fetch if any missing
if df_ff is None or df_opp is None or df_pace is None:
    log("One or more outputs missing; fetching from stats.wnba.com as fallback...")
    import requests
    ENDPOINT_TEAM_DASH_LASTN = "https://stats.wnba.com/stats/teamdashboardbylastngames"
    ENDPOINT_LEAGUE_DASH_TEAM = "https://stats.wnba.com/stats/leaguedashteamstats"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://stats.wnba.com",
        "Referer": "https://stats.wnba.com/",
    }
    def _rs_to_df(rs): return pd.DataFrame(rs["rowSet"], columns=rs["headers"])
    def _request_json(url, params, retries=6, backoff=2.0):
        last=None; headers=HEADERS.copy()
        for i in range(retries):
            if i>=2:
                headers["x-nba-stats-origin"]="stats"
                headers["x-nba-stats-token"]="true"
            try:
                r=requests.get(url, headers=headers, params=params, timeout=60); last=r
                if r.status_code==200:
                    try: return r.json()
                    except Exception as e: log(f"JSON decode error attempt {i+1}: {e}")
                else:
                    log(f"HTTP {r.status_code} on attempt {i+1}")
            except Exception as e:
                log(f"Request exception attempt {i+1}: {e}")
            time.sleep(backoff*(1+0.25*random.random()))
        raise RuntimeError("Stats API unavailable after retries")

    # League FF (Last 6)
    params_ff = {"Conference":"","DateFrom":"","DateTo":"","Division":"","GameScope":"","GameSegment":"","LastNGames":0,"LeagueID":"10","Location":"","MeasureType":"Four Factors","Month":0,"OpponentTeamID":0,"Outcome":"","PORound":0,"PaceAdjust":"N","PerMode":"PerGame","Period":0,"PlayerExperience":"","PlayerPosition":"","PlusMinus":"N","Rank":"N","Season":SEASON,"SeasonSegment":SEASON_SEGMENT,"SeasonType":SEASON_TYPE,"ShotClockRange":"","StarterBench":"","TeamID":0,"TwoWay":0,"VsConference":"","VsDivision":""}
    jff = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params_ff)
    df_ff = _rs_to_df(jff["resultSets"][0])

    # Opponent (Per100, Last 6)
    params_opp = {"Conference":"","DateFrom":"","DateTo":"","Division":"","GameScope":"","GameSegment":"","LastNGames":0,"LeagueID":"10","Location":"","MeasureType":"Opponent","Month":0,"OpponentTeamID":0,"Outcome":"","PORound":0,"PaceAdjust":"N","PerMode":"Per100Poss","Period":0,"PlayerExperience":"","PlayerPosition":"","PlusMinus":"N","Rank":"N","Season":SEASON,"SeasonSegment":SEASON_SEGMENT,"SeasonType":SEASON_TYPE,"ShotClockRange":"","StarterBench":"","TeamID":0,"TwoWay":0,"VsConference":"","VsDivision":""}
    jopp = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params_opp)
    df_opp = _rs_to_df(jopp["resultSets"][0])

    # Advanced (PACE, Last 6)
    params_adv = {"Conference":"","DateFrom":"","DateTo":"","Division":"","GameScope":"","GameSegment":"","LastNGames":0,"LeagueID":"10","Location":"","MeasureType":"Advanced","Month":0,"OpponentTeamID":0,"Outcome":"","PORound":0,"PaceAdjust":"N","PerMode":"PerGame","Period":0,"PlayerExperience":"","PlayerPosition":"","PlusMinus":"N","Rank":"N","Season":SEASON,"SeasonSegment":SEASON_SEGMENT,"SeasonType":SEASON_TYPE,"ShotClockRange":"","StarterBench":"","TeamID":0,"TwoWay":0,"VsConference":"","VsDivision":""}
    jadv = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params_adv)
    df_pace = _rs_to_df(jadv["resultSets"][0])

# ---- Google Sheets write ----
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import set_with_dataframe

    creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
    creds = Credentials.from_service_account_info(creds_info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
except Exception as e:
    log(f"ERROR auth/open sheet: {e}")
    sys.exit(2)

def upsert_sheet(df: pd.DataFrame, title: str):
    import gspread
    from gspread_dataframe import set_with_dataframe
    try:
        try:
            ws = sh.worksheet(title)
            ws.clear()
        except gspread.exceptions.WorksheetNotFound:
            rows = max(len(df), 100)
            cols = max(len(df.columns), 26)
            ws = sh.add_worksheet(title=title, rows=str(rows+50), cols=str(cols+5))
        set_with_dataframe(ws, df.reset_index(drop=False) if df.index.name else df, include_index=False, include_column_header=True, resize=True)
        log(f"Wrote tab: {title} ({len(df)} rows)")
    except Exception as e:
        log(f"ERROR writing tab '{title}': {e}")
        raise

# Minimal normalization for each table
if df_ff is not None:
    upsert_sheet(df_ff, SHEET_TAB_FF)
if df_opp is not None:
    keep_cols=["TEAM_ID","TEAM_NAME","GP","W","L","W_PCT","OPP_FGM","OPP_FGA","OPP_FG_PCT","OPP_FG3M","OPP_FG3A","OPP_FG3_PCT","OPP_FTM","OPP_FTA","OPP_FT_PCT","OPP_OREB","OPP_DREB","OPP_REB","OPP_AST","OPP_TOV","OPP_STL","OPP_BLK","OPP_BLKA","OPP_PF","OPP_PFD","OPP_PTS"]
    try:
        df_opp2 = df_opp[keep_cols].copy()
    except Exception:
        df_opp2 = df_opp
    upsert_sheet(df_opp2, SHEET_TAB_OPP)
if df_pace is not None:
    if "TEAM_NAME" in df_pace.columns and "PACE" in df_pace.columns:
        df_pace2 = df_pace[["TEAM_NAME","PACE"]].copy()
    else:
        df_pace2 = df_pace
    upsert_sheet(df_pace2, SHEET_TAB_PACE)
if df_sugg is not None:
    upsert_sheet(df_sugg, SHEET_TAB_SUGG)

log("Done.")
