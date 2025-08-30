#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
props_model.py
- Reads all inputs from Google Sheets (no external APIs)
- Builds pace- and matchup-adjusted projections for basic props
- Writes out/player_prop_projections.csv
- (The workflow then pushes this CSV back to the Sheet)
"""

import os, json, sys, pandas as pd, numpy as np
from datetime import datetime

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

def optional(tab):
    try:
        return _read_tab(tab)
    except Exception:
        return None

if not SHEET_ID:
    print("ERROR: SHEET_ID missing", file=sys.stderr); sys.exit(1)

# Load required inputs
lines = _read_tab("lines")
opp   = _read_tab("opponent_per100_last6")
pace  = _read_tab("pace_last6")

# normalize
lines.columns = [str(c).strip().lower() for c in lines.columns]
lines["team"] = lines["team"].astype(str).str.strip()
lines["opponent"] = lines["opponent"].astype(str).str.strip()

opp.columns = [str(c).strip() for c in opp.columns]
if "TEAM_NAME" not in opp.columns:
    if "Team" in opp.columns: opp.rename(columns={"Team":"TEAM_NAME"}, inplace=True)
    else: raise SystemExit("opponent_per100_last6 must include TEAM_NAME")
opp = opp.set_index("TEAM_NAME")
num = opp.select_dtypes(include=[float,int])
mu, sd = num.mean(numeric_only=True), num.std(numeric_only=True).replace(0, np.nan)

pace.columns = [str(c).strip().upper() for c in pace.columns]
pace = pace[["TEAM_NAME","PACE"]].copy()
pace["PACE"] = pd.to_numeric(pace["PACE"], errors="coerce")
df_pace = pace.set_index("TEAM_NAME")
league_pace = float(df_pace["PACE"].mean()) if not df_pace.empty else np.nan

# Optional inputs
ff   = optional("four_factors_last6")
arch = optional("archetypes")
base = optional("players_baseline")
onoff= optional("on_off")
rest = optional("status_rest")
posd = optional("positional_defense")

TEAM_ARCH = {}
if arch is not None and not arch.empty:
    arch.columns = [str(c).strip().lower() for c in arch.columns]
    for _, r in arch.iterrows():
        TEAM_ARCH.setdefault(str(r.get("team","")).strip(), {}).setdefault(str(r.get("role","")).strip(), []).append(str(r.get("player","")).strip())

def role_for_player(player, team):
    d = TEAM_ARCH.get(team, {})
    for role, players in d.items():
        if player in players: return role
    return None

def z(team, col):
    v = float(opp.loc[team, col])
    return (v - float(mu[col])) / (float(sd[col]) if not np.isnan(sd[col]) else 1.0)

def pace_factor(team, opp_name):
    tp = float(df_pace.loc[team, "PACE"]) if team in df_pace.index else np.nan
    op = float(df_pace.loc[opp_name, "PACE"]) if opp_name in df_pace.index else np.nan
    if np.isnan(tp) and not np.isnan(op): gp = op
    elif np.isnan(op) and not np.isnan(tp): gp = tp
    else: gp = (tp + op) / 2.0 if not (np.isnan(tp) or np.isnan(op)) else np.nan
    pf = (gp / league_pace) if (league_pace and not np.isnan(gp) and league_pace > 0) else 1.0
    return pf, gp

# Baselines lookup
BASE = {}
if base is not None and not base.empty:
    base.columns = [str(c).strip().lower() for c in base.columns]
    for _, r in base.iterrows():
        p = str(r.get("player","")).strip()
        if not p: continue
        BASE[p.lower()] = {k: r.get(k) for k in base.columns}

# On-off lookup
ONOFF = {}
if onoff is not None and not onoff.empty:
    onoff.columns = [str(c).strip().lower() for c in onoff.columns]
    for _, r in onoff.iterrows():
        p = str(r.get("player","")).strip().lower()
        t = str(r.get("team","")).strip()
        dp40 = r.get("delta_per40")
        nrd = r.get("netrating_diff")
        ONOFF[(p, t)] = {"delta_per40": dp40 if pd.notna(dp40) else (0.1 * nrd if pd.notna(nrd) else 0.0)}

# Rest lookup
REST = {}
if rest is not None and not rest.empty:
    rest.columns = [str(c).strip().lower() for c in rest.columns]
    for _, r in rest.iterrows():
        t = str(r.get("team","")).strip()
        REST[t] = {
            "restdays": r.get("restdays", np.nan),
            "points_adj": r.get("points_adjustment", 0.0),
            "minutes_scale": r.get("minutes_scale", 1.0),
        }

# Position defense lookup
POSD = {}
if posd is not None and not posd.empty:
    posd.columns = [str(c).strip().lower() for c in posd.columns]
    for _, r in posd.iterrows():
        t = str(r.get("team","")).strip()
        POSD[t] = {k: r.get(k) for k in posd.columns if k != "team"}

def implied_prob(odds):
    try:
        o = float(odds)
        return 100.0/(o+100.0) if o>0 else (-o)/((-o)+100.0)
    except:
        return np.nan

def project_row(r):
    player = str(r.get("player") or "").strip()
    team   = str(r.get("team") or "").strip()
    oppn   = str(r.get("opponent") or "").strip()
    prop   = str(r.get("prop") or "").strip().lower()
    try: line = float(r.get("line"))
    except: line = np.nan
    over_odds = r.get("over_odds"); under_odds = r.get("under_odds"); book = r.get("book")

    pf, gp = pace_factor(team, oppn)

    def _z(c):
        try: return z(oppn, c)
        except: return 0.0
    z3a = _z("OPP_FG3A"); z3p = _z("OPP_FG3_PCT"); zfta = _z("OPP_FTA"); zreb = _z("OPP_REB"); zast = _z("OPP_AST"); ztov = _z("OPP_TOV"); zpts = _z("OPP_PTS")

    b = BASE.get(player.lower(), {})
    pos = (b.get("position") or "").upper()
    min_mean = float(b.get("min_mean")) if pd.notna(b.get("min_mean")) else 30.0
    usage    = float(b.get("usage_pct")) if pd.notna(b.get("usage_pct")) else 0.22
    ar3      = float(b.get("3p_ar")) if pd.notna(b.get("3p_ar")) else 0.32
    ftrate   = float(b.get("ft_rate")) if pd.notna(b.get("ft_rate")) else 0.25
    rrate    = float(b.get("reb_rate")) if pd.notna(b.get("reb_rate")) else 0.14
    arate    = float(b.get("ast_rate")) if pd.notna(b.get("ast_rate")) else 0.18
    srate    = float(b.get("stl_rate")) if pd.notna(b.get("stl_rate")) else 0.025
    trate    = float(b.get("tov_rate")) if pd.notna(b.get("tov_rate")) else 0.12

    rs = REST.get(team, {})
    minutes_scale = float(rs.get("minutes_scale", 1.0))
    min_mean *= minutes_scale

    dp40 = ONOFF.get((player.lower(), team), {}).get("delta_per40", 0.0)

    if prop in ["points","pts"]:
        mean = max(5.0, (14.0 + 22.0*usage + 3.0*pf + 1.4*zpts + 0.3*zfta + 0.4*z3a + 0.3*z3p + dp40) * (min_mean/32.0))
        var = mean * (1.0 + 0.35)
    elif prop in ["rebounds","reb"]:
        mean = max(2.0, (6.5 + 40.0*rrate + 0.8*pf + 0.35*zreb + dp40*0.2) * (min_mean/32.0))
        var = mean * (1.0 + 0.30)
    elif prop in ["assists","ast"]:
        mean = max(1.0, (3.8 + 35.0*arate + 0.7*pf + 0.35*zast - 0.2*ztov + dp40*0.25) * (min_mean/32.0))
        var = mean * (1.0 + 0.30)
    elif prop in ["3pm","3ptm","3-pointers made","3pt made","threes","3s"]:
        lam = max(0.1, (1.8 + 5.0*ar3 + 0.35*pf + 0.25*z3a + 0.15*z3p + dp40*0.1) * (min_mean/32.0))
        mean = lam; var = lam
    elif prop in ["steals","stl"]:
        lam = max(0.05, (0.9 + 22.0*srate + 0.1*pf + 0.15*ztov) * (min_mean/32.0))
        mean = lam; var = lam
    elif prop in ["turnovers","tov"]:
        lam = max(0.3, (1.8 + 20.0*trate + 0.2*pf + 0.15*ztov) * (min_mean/32.0))
        mean = lam; var = lam
    else:
        return None

    try:
        from scipy.stats import nbinom, poisson, norm
        if var > mean * 1.05:
            p = mean/var
            rnb = mean*p/(1-p) if 0<p<1 else mean
            p_over = 1 - nbinom.cdf(line, rnb, p)
        else:
            if mean <= 6 and prop in ["3pm","3ptm","steals","stl","turnovers","tov"]:
                p_over = 1 - poisson.cdf(line, mean)
            else:
                sd = max(1e-6, np.sqrt(var))
                p_over = 1 - norm.cdf(line + 1e-9, loc=mean, scale=sd)
    except Exception:
        sd = max(1e-6, np.sqrt(var))
        from math import erf
        zscore = (line - mean)/sd
        p_over = 0.5*(1 - erf(zscore/np.sqrt(2)))

    p_under = 1 - p_over
    p_o_imp = (100.0/(float(over_odds)+100.0) if float(over_odds)>0 else (-float(over_odds))/((-float(over_odds))+100.0)) if over_odds not in [None,""] else 0.5
    p_u_imp = (100.0/(float(under_odds)+100.0) if float(under_odds)>0 else (-float(under_odds))/((-float(under_odds))+100.0)) if under_odds not in [None,""] else 0.5
    edge_over = p_over - p_o_imp
    edge_under = p_under - p_u_imp
    best_side = "OVER" if edge_over >= edge_under else "UNDER"
    edge_bp = edge_over if best_side=="OVER" else edge_under

    return {
        "player": player, "team": team, "opponent": oppn, "prop": r.get("prop"),
        "line": line, "proj_mean": round(mean,3), "proj_var": round(var,3),
        "p_over": round(p_over,4), "p_under": round(p_under,4),
        "best_side": best_side, "edge_bp": round(edge_bp,4),
        "pace_factor": round(pf,3) if isinstance(pf,(int,float)) else np.nan,
        "book": book, "over_odds": over_odds, "under_odds": under_odds
    }

rows = []
for rec in lines.to_dict("records"):
    try:
        res = project_row(rec)
        if res: rows.append(res)
    except Exception as e:
        # keep the run going even if one row fails
        continue

df = pd.DataFrame(rows)
out_csv = os.path.join(OUT, "player_prop_projections.csv")
df.to_csv(out_csv, index=False)
print("Saved", out_csv)
