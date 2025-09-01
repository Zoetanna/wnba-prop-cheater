# scripts/fit_roles.py
import os, pandas as pd, numpy as np, joblib
from src.roles.features import build_rolling_features
from src.roles.cluster_off import fit_offensive_roles, OFF_FEATURES
from src.roles.cluster_def import fit_defensive_roles, DEF_FEATURES
SHEET_ID=os.getenv("SHEET_ID","").strip(); OUT=os.getenv("OUTPUT_DIR","./out").strip()
if not SHEET_ID: raise SystemExit("SHEET_ID missing")
os.makedirs(OUT, exist_ok=True); MODEL_DIR=os.path.join(OUT,"models","roles"); os.makedirs(MODEL_DIR, exist_ok=True)
f10,f20 = build_rolling_features(SHEET_ID, tab_logs=os.getenv("TAB_LOGS","player_game_log"), win_short=10, win_long=20)
if f20.empty: raise SystemExit("Need more player_game_log data")
off = fit_offensive_roles(f20, n_components=int(os.getenv("OFF_K","5"))); joblib.dump(off, os.path.join(MODEL_DIR,"offense.pkl"))
deff= fit_defensive_roles(f20, n_components=int(os.getenv("DEF_K","4"))); joblib.dump(deff, os.path.join(MODEL_DIR,"defense.pkl"))
f20.to_csv(os.path.join(MODEL_DIR,"training_features_long.csv"), index=False); print("Saved role models")
