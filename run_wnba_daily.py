#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
WNBA PROPs — League-wide Daily Runner (FULL-FAT, Pace-Aware)

Pulls league-wide team context for the 2025 Regular Season and generates
pace-aware, role-aware player prop suggestions from your pasted lines (CSV or Google Sheets).

Data sources (stats.wnba.com APIs):
- teamdashboardbylastngames  (MeasureType=Four Factors)           → Team + Opponent Four Factors (Last 6)
- leaguedashteamstats        (MeasureType=Opponent, Per100Poss)   → Opponent allowances per 100 (Last 6)
- leaguedashteamstats        (MeasureType=Four Factors)           → League FF means (Last 6)
- leaguedashteamstats        (MeasureType=Advanced)               → **PACE** (Last 6)

Outputs (in OUTPUT_DIR):
- four_factors_all_last6.csv
- opponent_general_per100_last6.csv
- pace_last6.csv
- prop_suggestions_allteams.csv          (if lines provided)
- wnba_props_plays_card_allteams.png     (if lines provided, optional filters applied)
- (optional) wnba_props_over_only.png, wnba_props_under_only.png
- run_summary.txt

ENV VARS (tune without editing code)
------------------------------------
# Required season context
SEASON=2025
SEASON_TYPE="Regular Season"
LAST_N_GAMES=6
SEASON_SEGMENT="Last 6 Games"

# Output directory
OUTPUT_DIR="./out"

# Lines input (choose ONE)
LINES_CSV="/abs/path/to/lines.csv"
# or Google Sheets (Service Account JSON as a single-line env string)
SHEET_ID="your_google_sheet_id"
SHEET_TAB="lines"
GOOGLE_SERVICE_ACCOUNT_JSON='{"type":"service_account", ... }'

# Optional player role mapping
# CSV columns: team, role, player
ARCHETYPES_CSV="/abs/path/to/archetypes.csv"

# Optional execution filters and knobs
TEAMS_INCLUDE=""                 # Comma-separated team names to **include** (e.g., "Las Vegas Aces,Atlanta Dream"); default = all
TEAMS_EXCLUDE=""                 # Comma-separated team names to **exclude**
PACE_FAST_THRESH=1.05            # pace_factor ≥ this ⇒ add positive signal
PACE_SLOW_THRESH=0.95            # pace_factor ≤ this ⇒ add negative signal
MIN_CONF_FOR_CARD="-"           # picks below this confidence are hidden on card; one of: -, +, ++, +++ (UNDER versions included automatically)
MAKE_OVER_UNDER_CARDS="0"        # "1" to export separate OVER and UNDER cards
MAX_PLAYS_PER_TEAM="4"           # cap rows per team on plays card
"""


import os, time, json, random
import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


# --------------- CONFIG ---------------
SEASON = os.getenv("SEASON", "2025")
SEASON_TYPE = os.getenv("SEASON_TYPE", "Regular Season")
LAST_N_GAMES = int(os.getenv("LAST_N_GAMES", "6"))
SEASON_SEGMENT = os.getenv("SEASON_SEGMENT", "Last 6 Games")

OUTPUT_DIR = os.getenv("OUTPUT_DIR", "./out")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Inputs
LINES_CSV = os.getenv("LINES_CSV", "").strip()
SHEET_ID = os.getenv("SHEET_ID", "").strip()
SHEET_TAB = os.getenv("SHEET_TAB", "lines").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
ARCHETYPES_CSV = os.getenv("ARCHETYPES_CSV", "").strip()

# Filters & knobs
TEAMS_INCLUDE = [t.strip() for t in os.getenv("TEAMS_INCLUDE", "").split(",") if t.strip()]
TEAMS_EXCLUDE = [t.strip() for t in os.getenv("TEAMS_EXCLUDE", "").split(",") if t.strip()]
PACE_FAST_THRESH = float(os.getenv("PACE_FAST_THRESH", "1.05"))
PACE_SLOW_THRESH = float(os.getenv("PACE_SLOW_THRESH", "0.95"))
MIN_CONF_FOR_CARD = os.getenv("MIN_CONF_FOR_CARD", "-").strip().upper()
MAKE_OVER_UNDER_CARDS = os.getenv("MAKE_OVER_UNDER_CARDS", "0").strip() == "1"
MAX_PLAYS_PER_TEAM = int(os.getenv("MAX_PLAYS_PER_TEAM", "4"))

# Google Sheets toggle
USE_SHEETS = False
try:
    import gspread
    from google.oauth2.service_account import Credentials
    from gspread_dataframe import get_as_dataframe
    if SHEET_ID and GOOGLE_SERVICE_ACCOUNT_JSON:
        USE_SHEETS = True
except Exception:
    gspread = None

# --------------- STATS API ---------------
ENDPOINT_TEAM_DASH_LASTN = "https://stats.wnba.com/stats/teamdashboardbylastngames"
ENDPOINT_LEAGUE_DASH_TEAM = "https://stats.wnba.com/stats/leaguedashteamstats"

BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://stats.wnba.com",
    "Referer": "https://stats.wnba.com/",
    "Connection": "keep-alive",
}

def _rs_to_df(rs: dict) -> pd.DataFrame:
    return pd.DataFrame(rs["rowSet"], columns=rs["headers"])

def _request_json(url: str, params: dict, retries: int = 5, backoff: float = 1.8):
    """
    Resilient GET with exponential-ish backoff. On later attempts, add x-nba headers.
    """
    headers = BASE_HEADERS.copy()
    last = None
    for i in range(retries):
        if i >= 2:
            headers["x-nba-stats-origin"] = "stats"
            headers["x-nba-stats-token"] = "true"
        r = requests.get(url, headers=headers, params=params, timeout=60)
        last = r
        if r.status_code == 200:
            try:
                return r.json()
            except Exception:
                pass
        # Respectful sleep
        time.sleep(backoff * (1 + 0.25 * random.random()))
    # If we’re here, raise the last error
    last.raise_for_status()


# --------------- HELPERS ---------------
def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# --------------- LEAGUE + TEAM PULLS ---------------
def get_all_teams(SEASON, SEASON_TYPE) -> pd.DataFrame:
    params = {
        "Conference":"", "DateFrom":"", "DateTo":"", "Division":"", "GameScope":"", "GameSegment":"",
        "LastNGames":0, "LeagueID":"10", "Location":"", "MeasureType":"Base", "Month":0,
        "OpponentTeamID":0, "Outcome":"", "PORound":0, "PaceAdjust":"N", "PerMode":"PerGame", "Period":0,
        "PlayerExperience":"", "PlayerPosition":"", "PlusMinus":"N", "Rank":"N",
        "Season": SEASON, "SeasonType": SEASON_TYPE, "SeasonSegment":"", "ShotClockRange":"",
        "StarterBench":"", "TeamID":0, "TwoWay":0, "VsConference":"", "VsDivision":"",
    }
    data = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params)
    df = _rs_to_df(data["resultSets"][0])
    teams = df[["TEAM_ID","TEAM_NAME"]].drop_duplicates().reset_index(drop=True)

    # Apply include/exclude filters if provided
    if TEAMS_INCLUDE:
        teams = teams[teams["TEAM_NAME"].isin(TEAMS_INCLUDE)].copy()
    if TEAMS_EXCLUDE:
        teams = teams[~teams["TEAM_NAME"].isin(TEAMS_EXCLUDE)].copy()
    return teams


def fetch_team_four_factors(team_id: int) -> dict:
    params = {
        "LastNGames": LAST_N_GAMES, "LeagueID": "10", "MeasureType": "Four Factors",
        "Month": 0, "OpponentTeamID": 0, "PaceAdjust": "N", "PerMode": "PerGame", "Period": 0,
        "PlusMinus": "N", "Rank": "N", "Season": SEASON, "SeasonType": SEASON_TYPE, "TeamID": team_id,
        "Outcome": "","Location": "","SeasonSegment": "","DateFrom": "","DateTo": "","GameSegment": "","ShotClockRange": "",
        "AheadBehind": "","ContextFilter": "","ContextMeasure": "FGA",
    }
    data = _request_json(ENDPOINT_TEAM_DASH_LASTN, params)
    team_tbl = opp_tbl = None
    for rs in data.get("resultSets", []):
        n = (rs.get("name") or "").lower()
        if n == "overallteamdashboard":
            team_tbl = _rs_to_df(rs)
        elif n == "overallopponentdashboard":
            opp_tbl = _rs_to_df(rs)
    # Fallback to first two sets if names missing
    if team_tbl is None or opp_tbl is None:
        sets = data.get("resultSets", [])
        if team_tbl is None and sets: team_tbl = _rs_to_df(sets[0])
        if opp_tbl is None and len(sets) > 1: opp_tbl = _rs_to_df(sets[1])

    T = team_tbl.iloc[0].to_dict(); O = opp_tbl.iloc[0].to_dict()
    return {
        "eFG%": float(T.get("EFG_PCT", 0)) * 100,
        "TOV%": float(T.get("TM_TOV_PCT", 0)) * 100,
        "OREB%": float(T.get("OREB_PCT", 0)) * 100,
        "FTA Rate": float(T.get("FTA_RATE", 0)),
        "opp eFG%": float(O.get("EFG_PCT", 0)) * 100,
        "opp TOV%": float(O.get("TM_TOV_PCT", 0)) * 100,
        "opp OREB%": float(O.get("OREB_PCT", 0)) * 100,
        "opp FTA Rate": float(O.get("FTA_RATE", 0)),
        "GP": int(T.get("GP", LAST_N_GAMES)), "W": int(T.get("W", 0)), "L": int(T.get("L", 0)),
    }


def league_opponent_general_last6_per100() -> pd.DataFrame:
    params = {
        "Conference": "","DateFrom": "","DateTo": "","Division": "","GameScope": "","GameSegment": "",
        "LastNGames": 0,"LeagueID": "10","Location": "","MeasureType": "Opponent","Month": 0,
        "OpponentTeamID": 0,"Outcome": "","PORound": 0,"PaceAdjust": "N","PerMode": "Per100Poss","Period": 0,
        "PlayerExperience": "","PlayerPosition": "","PlusMinus": "N","Rank": "N","Season": SEASON,
        "SeasonSegment": SEASON_SEGMENT,"SeasonType": SEASON_TYPE,"ShotClockRange": "",
        "StarterBench": "","TeamID": 0,"TwoWay": 0,"VsConference": "","VsDivision": "",
    }
    data = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params)
    return _rs_to_df(data["resultSets"][0])


def league_four_factors_last6() -> pd.DataFrame:
    params = {
        "Conference": "","DateFrom": "","DateTo": "","Division": "","GameScope": "","GameSegment": "",
        "LastNGames": 0,"LeagueID": "10","Location": "","MeasureType": "Four Factors","Month": 0,
        "OpponentTeamID": 0,"Outcome": "","PORound": 0,"PaceAdjust": "N","PerMode": "PerGame","Period": 0,
        "PlayerExperience": "","PlayerPosition": "","PlusMinus": "N","Rank": "N","Season": SEASON,
        "SeasonSegment": SEASON_SEGMENT,"SeasonType": SEASON_TYPE,"ShotClockRange": "",
        "StarterBench": "","TeamID": 0,"TwoWay": 0,"VsConference": "","VsDivision": "",
    }
    data = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params)
    df = _rs_to_df(data["resultSets"][0])
    keep = ["TEAM_ID","TEAM_NAME","EFG_PCT","TM_TOV_PCT","OREB_PCT","FTA_RATE"]
    df = df[keep].copy()
    for c in ["EFG_PCT","TM_TOV_PCT","OREB_PCT","FTA_RATE"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def league_advanced_last6() -> pd.DataFrame:
    """Advanced board (PACE in PerGame context, Last 6)."""
    params = {
        "Conference": "","DateFrom": "","DateTo": "","Division": "","GameScope": "","GameSegment": "",
        "LastNGames": 0,"LeagueID": "10","Location": "","MeasureType": "Advanced","Month": 0,
        "OpponentTeamID": 0,"Outcome": "","PORound": 0,"PaceAdjust": "N","PerMode": "PerGame","Period": 0,
        "PlayerExperience": "","PlayerPosition": "","PlusMinus": "N","Rank": "N","Season": SEASON,
        "SeasonSegment": SEASON_SEGMENT,"SeasonType": SEASON_TYPE,"ShotClockRange": "",
        "StarterBench": "","TeamID": 0,"TwoWay": 0,"VsConference": "","VsDivision": "",
    }
    data = _request_json(ENDPOINT_LEAGUE_DASH_TEAM, params)
    return _rs_to_df(data["resultSets"][0])


# --------------- RUN PULLS ---------------
log("Fetching team list...")
teams_df = get_all_teams(SEASON, SEASON_TYPE)
team_map = dict(zip(teams_df["TEAM_NAME"], teams_df["TEAM_ID"]))
log(f"Teams: {len(team_map)}")

log("Pulling Four Factors (team + opp) for each team...")
ff_rows = []
for name, tid in team_map.items():
    try:
        row = {"Team": name, **fetch_team_four_factors(int(tid))}
        ff_rows.append(row)
        time.sleep(1.15)  # be nice
    except Exception as e:
        log(f"FF error on {name}: {e}")

df_ff_all = pd.DataFrame(ff_rows).set_index("Team").sort_index()
df_ff_all.to_csv(os.path.join(OUTPUT_DIR, "four_factors_all_last6.csv"), float_format="%.5f")
log("Saved four_factors_all_last6.csv")

log("Pulling league Four Factors means (Last 6)...")
league_ff = league_four_factors_last6()
league_means_ff = {
    "eFG%": league_ff["EFG_PCT"].mean() * 100,
    "TOV%": league_ff["TM_TOV_PCT"].mean() * 100,
    "OREB%": league_ff["OREB_PCT"].mean() * 100,
    "FTA Rate": league_ff["FTA_RATE"].mean(),
}

log("Pulling Opponent General (Per 100, Last 6) league-wide...")
opp_gen = league_opponent_general_last6_per100()
keep_cols = ["TEAM_ID","TEAM_NAME","GP","W","L","W_PCT","OPP_FGM","OPP_FGA","OPP_FG_PCT","OPP_FG3M","OPP_FG3A","OPP_FG3_PCT",
             "OPP_FTM","OPP_FTA","OPP_FT_PCT","OPP_OREB","OPP_DREB","OPP_REB","OPP_AST","OPP_TOV","OPP_STL","OPP_BLK","OPP_BLKA","OPP_PF","OPP_PFD","OPP_PTS"]
opp_gen = opp_gen[keep_cols].copy()
opp_gen.to_csv(os.path.join(OUTPUT_DIR, "opponent_general_per100_last6.csv"), index=False, float_format="%.5f")
df_opp_all = opp_gen.set_index("TEAM_NAME").sort_index()
league_means_opp = opp_gen.drop(columns=["TEAM_ID","TEAM_NAME"]).apply(pd.to_numeric, errors="coerce").mean(numeric_only=True)
league_stds_opp  = opp_gen.drop(columns=["TEAM_ID","TEAM_NAME"]).apply(pd.to_numeric, errors="coerce").std(numeric_only=True).replace(0, np.nan)
log("Saved opponent_general_per100_last6.csv")

log("Pulling PACE (Advanced, Last 6)...")
adv = league_advanced_last6()
if "PACE" in adv.columns:
    adv["PACE"] = pd.to_numeric(adv["PACE"], errors="coerce")
df_pace = adv[["TEAM_NAME","PACE"]].dropna().set_index("TEAM_NAME").sort_index()
league_pace = float(df_pace["PACE"].mean()) if not df_pace.empty else np.nan
df_pace.to_csv(os.path.join(OUTPUT_DIR, "pace_last6.csv"))
log("Saved pace_last6.csv")


# --------------- LINES INGEST ---------------
lines_df = pd.DataFrame()
if USE_SHEETS and gspread is not None:
    try:
        creds_info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
        scope = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        creds = Credentials.from_service_account_info(creds_info, scopes=scope)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)
        ws = sh.worksheet(SHEET_TAB)
        lines_df = get_as_dataframe(ws, evaluate_formulas=True, header=0).dropna(how="all")
        log(f"Loaded lines from Google Sheets: {len(lines_df)} rows")
    except Exception as e:
        log(f"Sheets ingest error: {e}")
elif LINES_CSV:
    try:
        lines_df = pd.read_csv(LINES_CSV)
        log(f"Loaded lines CSV: {len(lines_df)} rows")
    except Exception as e:
        log(f"CSV ingest error: {e}")

if not lines_df.empty:
    lines_df.columns = [str(c).strip().lower() for c in lines_df.columns]
    for col in ["player","team","opponent","prop"]:
        if col in lines_df.columns: lines_df[col] = lines_df[col].astype(str).str.strip()


# --------------- ARCHETYPES (optional) ---------------
TEAM_ARCHETYPES = {}
if ARCHETYPES_CSV and os.path.exists(ARCHETYPES_CSV):
    try:
        arch = pd.read_csv(ARCHETYPES_CSV)
        arch.columns = [c.strip().lower() for c in arch.columns]
        for _, r in arch.iterrows():
            team = str(r.get("team","")).strip()
            role = str(r.get("role","")).strip()
            player = str(r.get("player","")).strip()
            if team and role and player:
                TEAM_ARCHETYPES.setdefault(team, {}).setdefault(role, []).append(player)
        log(f"Loaded archetypes for {len(TEAM_ARCHETYPES)} teams")
    except Exception as e:
        log(f"Archetypes CSV error: {e}")


def role_for_player(player: str, team: str, fallback_role: str = None):
    d = TEAM_ARCHETYPES.get(team, {})
    for role, players in d.items():
        if player in players:
            return role
    return fallback_role


# --------------- SUGGESTION ENGINE (PACE-AWARE) ---------------
def z(metric, team_name, df_opp, league_means_opp, league_stds_opp):
    val = float(df_opp.loc[team_name, metric])
    mu = float(league_means_opp[metric])
    sd = float(league_stds_opp[metric]) if float(league_stds_opp[metric]) > 0 else np.nan
    return (val - mu) / sd if sd and not np.isnan(sd) else 0.0, val, mu

def pace_context(team: str, opp: str):
    """Return (team_pace, opp_pace, game_pace, league_pace, pace_factor)."""
    tp = float(df_pace.loc[team, "PACE"]) if team in df_pace.index else np.nan
    op = float(df_pace.loc[opp, "PACE"]) if opp in df_pace.index else np.nan
    if np.isnan(tp) and not np.isnan(op): gp = op
    elif np.isnan(op) and not np.isnan(tp): gp = tp
    else: gp = (tp + op) / 2.0 if not (np.isnan(tp) or np.isnan(op)) else np.nan
    pf = (gp / league_pace) if (league_pace and not np.isnan(gp) and league_pace > 0) else 1.0
    return tp, op, gp, league_pace, pf

CONF_ORDER = ["-", "+", "++", "+++", "UNDER +", "UNDER ++", "UNDER +++"]
def conf_rank(c: str) -> int:
    return CONF_ORDER.index(c) if c in CONF_ORDER else 0

def decide_over_under(pos, neg):
    if pos - neg >= 1: return "OVER"
    elif neg - pos >= 1: return "UNDER"
    else: return "PASS"

def conf_from_score(p, n):
    net = p - n
    if net >= 3: return "+++"
    if net == 2: return "++"
    if net == 1: return "+"
    if net == 0: return "-"
    if net <= -3: return "UNDER +++"
    if net == -2: return "UNDER ++"
    if net == -1: return "UNDER +"
    return "-"

def suggest_for_row(r, df_ff, df_opp, league_means_ff, league_means_opp, league_stds_opp):
    player = str(r.get("player") or "").strip()
    team = str(r.get("team") or "").strip()
    opp  = str(r.get("opponent") or "").strip()
    prop = str(r.get("prop") or "").strip()
    try: line = float(r.get("line"))
    except: line = np.nan
    over_odds = r.get("over_odds"); under_odds = r.get("under_odds"); book = r.get("book")

    if not opp:
        others = [t for t in df_ff.index if t != team]
        opp = others[0] if others else None

    role = r.get("role") or role_for_player(player, team)

    if opp not in df_opp.index or team not in df_pace.index or opp not in df_pace.index:
        return {"player":player,"team":team,"opponent":opp,"prop":prop,"line":line,
                "suggestion":"PASS","confidence":"-","signals":"Missing opp/pace data",
                "book":book,"over_odds":over_odds,"under_odds":under_odds,"role":role}

    # Opponent allowance z-scores (per 100 poss)
    z_3pa, v_3pa, mu_3pa = z("OPP_FG3A", opp, df_opp, league_means_opp, league_stds_opp)
    z_3pct, v_3pct, mu_3pct = z("OPP_FG3_PCT", opp, df_opp, league_means_opp, league_stds_opp)
    z_fta, v_fta, mu_fta = z("OPP_FTA", opp, df_opp, league_means_opp, league_stds_opp)
    z_oreb, v_oreb, mu_oreb = z("OPP_OREB", opp, df_opp, league_means_opp, league_stds_opp)
    z_reb, v_reb, mu_reb = z("OPP_REB", opp, df_opp, league_means_opp, league_stds_opp)
    z_ast, v_ast, mu_ast = z("OPP_AST", opp, df_opp, league_means_opp, league_stds_opp)
    z_tov_forced, v_tov, mu_tov = z("OPP_TOV", opp, df_opp, league_means_opp, league_stds_opp)
    z_pts, v_pts, mu_pts = z("OPP_PTS", opp, df_opp, league_means_opp, league_stds_opp)

    opp_eFG_allowed = float(df_ff.loc[opp, "opp eFG%"])
    efg_hi = opp_eFG_allowed >= league_means_ff["eFG%"] * 1.02
    efg_lo = opp_eFG_allowed <= league_means_ff["eFG%"] * 0.98

    # Pace context
    tp, op, gp, lp, pf = pace_context(team, opp)
    pace_sig = []
    if pf >= PACE_FAST_THRESH:
        pace_sig.append(f"Fast pace (+{(pf-1)*100:.1f}% vs Lg)")
    elif pf <= PACE_SLOW_THRESH:
        pace_sig.append(f"Slow pace ({(pf-1)*100:.1f}% vs Lg)")

    signals = pace_sig[:]
    if z_3pa >= 0.8: signals.append("Opp 3PA high (z≥0.8)")
    if z_3pct >= 0.5: signals.append("Opp 3P% high (z≥0.5)")
    if efg_hi: signals.append("Opp eFG% allowed high")
    if z_fta >= 0.8: signals.append("Opp FTA high (z≥0.8)")
    if z_oreb >= 1.0: signals.append("Opp OREB high (z≥1.0)")
    if z_reb >= 0.8: signals.append("Opp REB high (z≥0.8)")
    if z_ast >= 0.8: signals.append("Opp AST high (z≥0.8)")
    if z_tov_forced <= -0.5: signals.append("Opp doesn't force TOs (z≤−0.5)")
    if z_tov_forced >= 0.8: signals.append("Opp forces TOs (z≥0.8)")

    # Base signals by prop
    prop_lower = prop.lower()
    shooter_sig = (z_3pa >= 0.8) + (z_3pct >= 0.5) + (1 if efg_hi else 0)
    driver_sig  = (z_fta >= 0.8) + (1 if z_pts >= 0.6 else 0)
    rebound_sig = (z_oreb >= 1.0) + (z_reb >= 0.8)
    assist_pos  = (z_ast >= 0.8) + (1 if z_tov_forced <= -0.5 else 0)
    assist_neg  = (1 if z_tov_forced >= 0.8 else 0)
    steals_pos  = (1 if z_tov_forced >= 0.8 else 0)

    # Pace modifiers (applied to volume stats)
    pace_pos = 1 if pf >= PACE_FAST_THRESH else 0
    pace_neg = 1 if pf <= PACE_SLOW_THRESH else 0

    if prop_lower in ["3pm","3ptm","3-pointers made","3pt made","threes","3s"]:
        pos = shooter_sig + pace_pos; neg = (1 if efg_lo else 0) + pace_neg
    elif prop_lower in ["points","pts","p"]:
        pos = shooter_sig + driver_sig + (1 if z_pts >= 0.6 else 0) + pace_pos; neg = (1 if efg_lo else 0) + pace_neg
    elif prop_lower in ["rebounds","reb","r"]:
        pos = rebound_sig + pace_pos; neg = pace_neg
    elif prop_lower in ["assists","ast","a"]:
        pos = assist_pos + pace_pos; neg = assist_neg + pace_neg
    elif prop_lower in ["ftm","free throws made","free throws"]:
        pos = driver_sig + pace_pos; neg = pace_neg
    elif prop_lower in ["steals","stl"]:
        pos = steals_pos + pace_pos; neg = pace_neg
    elif prop_lower in ["turnovers","tov"]:
        pos = (1 if z_tov_forced >= 0.8 else 0) + pace_pos; neg = (1 if z_tov_forced <= -0.5 else 0) + pace_neg
    else:
        pos = 0; neg = 0

    suggestion = decide_over_under(pos, neg) if (pos or neg) else "PASS"
    conf = conf_from_score(pos, neg) if (pos or neg) else "-"

    return {
        "player": player, "team": team, "opponent": opp, "role": role or "(add role)",
        "prop": prop, "line": line, "over_odds": over_odds, "under_odds": under_odds, "book": book,
        "suggestion": suggestion, "confidence": conf,
        "pace_factor": round(pf, 3) if isinstance(pf, (int,float)) else np.nan,
        "game_pace": round(gp, 2) if isinstance(gp, (int,float)) else np.nan,
        "league_pace": round(league_pace, 2) if isinstance(league_pace, (int,float)) else np.nan,
        "signals": "; ".join(signals)
    }


# --------------- GENERATE SUGGESTIONS ---------------
sugg_df = pd.DataFrame()
if not lines_df.empty:
    log("Scoring lines with pace-aware engine...")
    sugg_df = pd.DataFrame([
        suggest_for_row(r, df_ff_all, df_opp_all, league_means_ff, league_means_opp, league_stds_opp)
        for r in lines_df.to_dict("records")
    ])
    sugg_out = os.path.join(OUTPUT_DIR, "prop_suggestions_allteams.csv")
    sugg_df.to_csv(sugg_out, index=False)
    log(f"Saved suggestions → {sugg_out}")
else:
    log("No lines provided; skipping suggestions and plays card.")


# --------------- PLAYS CARD EXPORTS ---------------
CONF_GATE = {"-":0, "+":1, "++":2, "+++":3}
MIN_GATE = CONF_GATE.get(MIN_CONF_FOR_CARD, 0)

def visible_conf(c):
    # normalize to a rank for filtering
    if c in ["-", "+", "++", "+++"]:
        return CONF_GATE[c]
    if c.startswith("UNDER"):
        # Map to +/++/+++ after UNDER
        if "+++" in c: return 3
        if "++"  in c: return 2
        if "+"   in c: return 1
    return 0

def build_plays_card(df, title, out_png, over_only=False, under_only=False):
    if df is None or df.empty: return None
    subset = df[df["suggestion"].isin(["OVER","UNDER"])].copy()
    if over_only: subset = subset[subset["suggestion"]=="OVER"]
    if under_only: subset = subset[subset["suggestion"]=="UNDER"]
    subset["gate"] = subset["confidence"].apply(visible_conf)
    subset = subset[subset["gate"] >= MIN_GATE].copy()
    if subset.empty: return None

    # rank for ordering within team
    def conf_map(c):
        return {"-":0,"+":1,"++":2,"+++":3,"UNDER +":1,"UNDER ++":2,"UNDER +++":3}.get(c,0)
    subset["conf_rank"] = subset["confidence"].apply(conf_map)
    subset = subset.sort_values(["team","conf_rank"], ascending=[True, False])

    rows = []
    for team, grp in subset.groupby("team"):
        rows.extend(grp.head(MAX_PLAYS_PER_TEAM).to_dict("records"))

    fig, ax = plt.subplots(figsize=(12, 16))
    ax.axis("off")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M")
    ax.text(0.5, 0.975, title, ha="center", va="top", fontsize=20, weight="bold")
    ax.text(0.5, 0.95, f"Generated {ts}", ha="center", va="top", fontsize=10)

    y = 0.92; step = 0.032
    cur_team = None
    for row in rows:
        team = row.get("team","")
        if team != cur_team:
            y -= step * 0.5
            ax.text(0.02, y, team, ha="left", va="top", fontsize=14, weight="bold")
            y -= step * 0.6
            cur_team = team

        line = row.get("line")
        line_str = f"{line:.1f}" if isinstance(line, (int,float)) and not pd.isna(line) else "—"
        pace_factor = row.get("pace_factor")
        pace_tag = ""
        if isinstance(pace_factor, (int,float)):
            if pace_factor >= PACE_FAST_THRESH: pace_tag = " | Pace↑"
            elif pace_factor <= PACE_SLOW_THRESH: pace_tag = " | Pace↓"

        text = f"{row.get('player')} — {row.get('prop')} {line_str}  |  {row.get('suggestion')} {row.get('confidence')}  |  {row.get('book') or ''}{pace_tag}"
        ax.text(0.035, y, text, ha="left", va="top", fontsize=11)

        sig = row.get("signals","")
        if isinstance(sig, str) and sig.strip():
            ax.text(0.05, y - step*0.50, sig, ha="left", va="top", fontsize=8)

        y -= step * 1.15
        if y < 0.05: break

    plt.tight_layout()
    plt.savefig(out_png, dpi=200)
    plt.close(fig)
    return out_png

card_main = None
if not sugg_df.empty:
    card_main = build_plays_card(
        sugg_df,
        title="WNBA Props Card (All Teams, Pace-Adjusted)",
        out_png=os.path.join(OUTPUT_DIR, "wnba_props_plays_card_allteams.png"),
        over_only=False, under_only=False
    )
    if card_main: log(f"Saved plays card → {card_main}")

    if MAKE_OVER_UNDER_CARDS:
        over_p = build_plays_card(
            sugg_df, "WNBA Props — OVERs Only (Pace-Adjusted)",
            os.path.join(OUTPUT_DIR, "wnba_props_over_only.png"),
            over_only=True, under_only=False
        )
        if over_p: log(f"Saved over-only card → {over_p}")
        under_p = build_plays_card(
            sugg_df, "WNBA Props — UNDERs Only (Pace-Adjusted)",
            os.path.join(OUTPUT_DIR, "wnba_props_under_only.png"),
            over_only=False, under_only=True
        )
        if under_p: log(f"Saved under-only card → {under_p}")


# --------------- SUMMARY ---------------
with open(os.path.join(OUTPUT_DIR, "run_summary.txt"), "w") as f:
    f.write(f"Run at {datetime.now().isoformat()}\n")
    f.write(f"Teams: {len(team_map)}\n")
    f.write(f"Files: four_factors_all_last6.csv, opponent_general_per100_last6.csv, pace_last6.csv\n")
    if not lines_df.empty:
        f.write("Suggestions: prop_suggestions_allteams.csv\n")
        if card_main: f.write("Card: wnba_props_plays_card_allteams.png\n")
    else:
        f.write("No lines provided; suggestions skipped.\n")

log("Done.")
