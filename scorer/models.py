"""Load the two production model bundles and score single events.

Bundle keys (saved by the Colab notebooks): model, features, categorical, codes,
threshold, calibrator, and for Model 1 also top_categories.
"""
import warnings

import joblib
import numpy as np
import pandas as pd


class StageModel:
    def __init__(self, path, name):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.bundle = joblib.load(path)
        b = self.bundle
        self.name = name
        self.features = list(b["features"])
        self.categorical = list(b.get("categorical", []))
        self.codes = b["codes"]
        self.top_categories = set(b.get("top_categories") or [])
        self.threshold = float(b["threshold"])
        self.calibrator = b.get("calibrator")
        self.model = b["model"]
        self.versions = b.get("versions", {})

    def check_versions(self):
        """Warn if the installed libraries differ from the ones the model was trained with."""
        import sklearn
        import xgboost
        installed = {"scikit-learn": sklearn.__version__, "xgboost": xgboost.__version__}
        problems = [f"{k}: trained {v}, installed {installed[k]}"
                    for k, v in self.versions.items() if k in installed and installed[k] != v]
        return problems

    def prepare(self, rows):
        """rows: list of dicts (messages) -> feature DataFrame in training order."""
        X = pd.DataFrame([{f: r.get(f) for f in self.features} for r in rows], columns=self.features)
        if "category" in X.columns and self.top_categories:
            X["category"] = X["category"].where(X["category"].isin(self.top_categories), "other")
        for c in self.categorical:
            X[c] = X[c].astype(str).map(self.codes[c]).fillna(-1).astype(int)
        for c in self.features:
            if c not in self.categorical:
                X[c] = pd.to_numeric(X[c], errors="coerce").astype(float)
        return X

    def score(self, rows):
        """-> (raw_score array, calibrated probability array, alert bool array)"""
        raw = self.model.predict_proba(self.prepare(rows))[:, 1]
        prob = self.calibrator.predict(raw) if self.calibrator is not None else raw
        return raw, np.asarray(prob, dtype=float), raw >= self.threshold
