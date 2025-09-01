# src/roles/label.py
def name_offense(c):
    pts,ast,three,ftr,tov = c["pts40"], c["ast40"], c["three_rate"], c["ft_rate"], c["tov40"]
    if three>0.45 and ast<4 and ftr<0.35: return "Shooter"
    if ftr>0.45 and pts>18: return "Driver/Slasher"
    if ast>6 and pts<20: return "Facilitator"
    if pts>22 and (three<0.25 or ftr>0.45): return "Primary Scorer"
    if three>0.35 and ast>5: return "Combo Guard"
    return "Balanced Wing"
def name_defense(c):
    reb,stl,blk = c["reb40"], c["stl40"], c["blk40"]
    if blk>1.6 and reb>8.0: return "Rim Protector"
    if reb>10.0 and blk<1.0: return "Boarding Big"
    if stl>2.0 and reb<7.0: return "Point of Attack"
    return "Team Defender"
def _centers(df, feats, probs, labels):
    import numpy as np, pandas as pd
    k=probs.shape[1]; cents=[]
    for i in range(k):
        m=labels==i
        if m.sum()==0: cents.append({f:0.0 for f in feats}); continue
        sub=df.loc[m, feats]; cents.append({f: float(sub[f].mean()) for f in feats})
    return cents
def label_offense(df, feats, probs, labels):
    cents=_centers(df, feats, probs, labels); names=[name_offense(c) for c in cents]; return [names[i] for i in labels], names, cents
def label_defense(df, feats, probs, labels):
    cents=_centers(df, feats, probs, labels); names=[name_defense(c) for c in cents]; return [names[i] for i in labels], names, cents
