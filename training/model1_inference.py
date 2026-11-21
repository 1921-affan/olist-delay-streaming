import joblib
import numpy as np
import pandas as pd

def load_bundle(path="model1_production_XGB_12.joblib"):
    return joblib.load(path)

def prepare_features(df, bundle):
    X = df[bundle["features"]].copy()
    X["category"] = X["category"].where(X["category"].isin(bundle["top_categories"]), "other")
    for c in bundle["categorical"]:
        X[c] = X[c].astype(str).map(bundle["codes"][c]).fillna(-1).astype(int)
    return X

def score_orders(df, bundle):
    """Returns (raw_score, late_probability, alert).
    raw_score        : model output, use it for ranking and the alert threshold
    late_probability : calibrated chance the order is late, use it on the dashboard
    alert            : True when raw_score >= the April-calibrated threshold"""
    raw = bundle["model"].predict_proba(prepare_features(df, bundle))[:, 1]
    prob = bundle["calibrator"].predict(raw)
    return raw, prob, raw >= bundle["threshold"]
