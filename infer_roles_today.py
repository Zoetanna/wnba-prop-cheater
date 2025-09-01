# scripts/infer_roles_today.py
import os, pandas as pd, numpy as np, joblib
from src.roles.features import build_rolling_features
from src.roles.cluster_off import OFF_FEATURES, predict_offensive_roles
from src.roles.cluster_def import DEF_FEATURES, predict_defensive_roles
from src.roles.label import label_offense, label_defense
from src.roles.report import make_report
SHEET_ID=os.getenv("SHEET_ID","").strip(); OUT=os.getenv("OUTPUT_DIR","./out").strip()
if not SHEET_ID: raise SystemExit("SHEET_ID missing")
os.makedirs(OUT, exist_ok=True); MODEL_DIR=os.path.join(OUT,"models","roles")
f10,f20 = build_rolling_features(SHEET_ID, tab_logs=os.getenv("TAB_LOGS","player_game_log"), win_short=10, win_long=20)
if f10.empty: raise SystemExit("Need more player_game_log data")
off=joblib.load(os.path.join(MODEL_DIR,"offense.pkl")); deff=joblib.load(os.path.join(MODEL_DIR,"defense.pkl"))
probo,labo = predict_offensive_roles(off, f10); probd,labd = predict_defensive_roles(deff, f10)
off_names_idx, off_names, _ = label_offense(f10, off["features"], probo, labo)
def_names_idx, def_names, _ = label_defense(f10, deff["features"], probd, labd)
rep = make_report(f10, off["features"], probo, labo, off_names, f10, deff["features"], probd, labd, def_names)
rep.to_csv(os.path.join(OUT,"player_roles_today.csv"), index=False); print("Saved roles CSV")
