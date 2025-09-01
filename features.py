# src/roles/features.py
import os, json, pandas as pd, numpy as np
def _get_creds():
    from google.oauth2.service_account import Credentials
    cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    js = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON","").strip()
    scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
    if cred_path and os.path.exists(cred_path): return Credentials.from_service_account_file(cred_path, scopes=scopes)
    if js: return Credentials.from_service_account_info(json.loads(js), scopes=scopes)
    raise RuntimeError("Missing creds")
def _read_sheet(sheet_id, tab):
    import gspread; from gspread_dataframe import get_as_dataframe
    gc = gspread.authorize(_get_creds()); sh = gc.open_by_key(sheet_id)
    df = get_as_dataframe(sh.worksheet(tab), evaluate_formulas=True, header=0)
    return df.dropna(how="all")
def _num(df, cols):
    for c in cols:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df
def build_rolling_features(sheet_id: str, tab_logs="player_game_log", win_short=10, win_long=20):
    logs = _read_sheet(sheet_id, tab_logs); logs.columns = [str(c).strip().lower() for c in logs.columns]
    req = ["date","player","team","min","pts","reb","ast","stl","blk","tov","fga","fg3a","fta"]
    miss = [c for c in req if c not in logs.columns]
    if miss: raise ValueError(f"'player_game_log' missing columns: {miss}")
    logs["date"] = pd.to_datetime(logs["date"], errors="coerce"); logs = logs.dropna(subset=["date","player","team","min"])
    logs = _num(logs, ["min","pts","reb","ast","stl","blk","tov","fga","fg3a","fta"]).sort_values(["player","date"])
    def per40(df):
        m = df["min"].replace(0, np.nan)
        return pd.DataFrame({
            "pts40": (df["pts"]/m)*40, "reb40": (df["reb"]/m)*40, "ast40": (df["ast"]/m)*40,
            "stl40": (df["stl"]/m)*40, "blk40": (df["blk"]/m)*40, "tov40": (df["tov"]/m)*40,
            "fga40": (df["fga"]/m)*40, "fg3a40": (df["fg3a"]/m)*40, "fta40": (df["fta"]/m)*40,
            "three_rate": df["fg3a"]/(df["fga"].replace(0,np.nan)), "ft_rate": df["fta"]/(df["fga"].replace(0,np.nan)), "min": df["min"],
        })
    def roll(df, w):
        x = per40(df); r = x.rolling(w, min_periods=max(5,w//2)).mean(); r["player"]=df["player"].values; r["team"]=df["team"].values; r["date"]=df["date"].values; return r
    def last(df, w):
        parts=[]; 
        for p,g in df.groupby("player", sort=False):
            r=roll(g,w).dropna(subset=["pts40","reb40","ast40"])
            if not r.empty: parts.append(r.iloc[-1])
        if not parts: return pd.DataFrame()
        out=pd.DataFrame(parts).replace([np.inf,-np.inf],np.nan).fillna(0)
        for c in ["three_rate","ft_rate"]: 
            if c in out.columns: out[c]=out[c].clip(0,1.5)
        return out.reset_index(drop=True)
    return last(logs, win_short), last(logs, win_long)
