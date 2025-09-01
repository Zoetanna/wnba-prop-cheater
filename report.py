# src/roles/report.py
import numpy as np, pandas as pd
from .smooth import stability
def make_report(df_off, feats_off, probs_off, labels_off, names_off,
                df_def, feats_def, probs_def, labels_def, names_def):
    rep = pd.DataFrame({"player": df_off["player"].values, "team": df_off["team"].values})
    ko = probs_off.shape[1]; kd = probs_def.shape[1]
    for i in range(ko): rep[f"off_p{i}"] = probs_off[:,i]
    for i in range(kd): rep[f"def_p{i}"] = probs_def[:,i]
    rep["off_primary_idx"] = probs_off.argmax(axis=1); rep["off_primary_role"] = rep["off_primary_idx"].apply(lambda i: names_off[i])
    rep["def_primary_idx"] = probs_def.argmax(axis=1); rep["def_primary_role"] = rep["def_primary_idx"].apply(lambda i: names_def[i])
    rep["primary_role"] = rep["off_primary_role"]; rep["secondary_role"] = rep["def_primary_role"]
    rep["stability"] = [stability(probs_off[i]) for i in range(len(rep))]
    return rep.sort_values(["team","player"]).reset_index(drop=True)
