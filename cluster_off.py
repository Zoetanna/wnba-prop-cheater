# src/roles/cluster_off.py
import numpy as np, pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.mixture import GaussianMixture
OFF_FEATURES = ["pts40","ast40","three_rate","ft_rate","tov40"]
def fit_offensive_roles(df: pd.DataFrame, n_components=5, random_state=42):
    df=df.copy()
    for c in OFF_FEATURES:
        if c not in df.columns: df[c]=0.0
        df[c]=df[c].replace([np.inf,-np.inf],np.nan).fillna(0.0)
    X=df[OFF_FEATURES].values; scaler=StandardScaler(); Xs=scaler.fit_transform(X)
    k=min(n_components, max(2, len(df)//8)); gmm=GaussianMixture(n_components=k, covariance_type="diag", random_state=random_state).fit(Xs)
    return {"scaler":scaler,"model":gmm,"features":OFF_FEATURES}
def predict_offensive_roles(bundle, df):
    X=df[bundle["features"]].replace([np.inf,-np.inf],np.nan).fillna(0.0).values
    Xs=bundle["scaler"].transform(X); probs=bundle["model"].predict_proba(Xs); labs=bundle["model"].predict(Xs); return probs,labs
