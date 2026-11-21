import joblib
import numpy as np
import pandas as pd

def load_bundle(path="model3_production.joblib"):
    return joblib.load(path)

def prepare_features(df, bundle):
    # df: rows of handed_to_carrier events (shipment_events_enriched.csv columns)
    X = df[bundle["features"]].copy()
    for c in bundle["categorical"]:
        X[c] = X[c].astype(str).map(bundle["codes"][c]).fillna(-1).astype(int)
    for c in bundle["features"]:
        if c not in bundle["categorical"]:
            X[c] = X[c].astype(float)
    return X

def score_orders(df, bundle):
    # Returns (raw_score, late_probability, alert), same contract as model1_inference.py
    raw = bundle["model"].predict_proba(prepare_features(df, bundle))[:, 1]
    return raw, bundle["calibrator"].predict(raw), raw >= bundle["threshold"]
