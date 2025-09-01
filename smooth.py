# src/roles/smooth.py
import numpy as np
def ema_probs(prev, curr, alpha=0.6):
    if prev is None or prev.shape!=curr.shape: return curr
    return alpha*prev + (1-alpha)*curr
def stability(p):
    p=p.clip(1e-9,1.0); p=p/p.sum(); import numpy as np
    H=-np.sum(p*np.log(p)); Hm=np.log(len(p)); return float(1-H/Hm) if Hm>0 else 1.0
