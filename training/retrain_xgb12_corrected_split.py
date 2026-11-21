# ================================================================
# EXPERIMENT 15: RETRAIN XGB_12 ON A CORRECTED CHRONOLOGICAL SPLIT
# ================================================================
# Paste this whole cell into Colab and run it. It needs orders_clean.csv
# in the Colab file panel. Tested versions: scikit-learn 1.6.1, xgboost 3.4.1.
#
# Why this split:
#   The earlier final fit put the LATE Feb-Apr 2018 orders into training and
#   kept only the ON-TIME Feb-Apr orders for validation. The model learned
#   "looks like Feb-Apr 2018 -> late", so it scored those on-time validation
#   orders high and the threshold landed at 0.915. On the test set that
#   threshold flagged only ~3.8% of on-time orders, not the 10% it was set for.
#
#   New split (every order of a month stays together, late and on-time):
#     train       : 2017-01-01 .. 2018-03-31  (all orders, incl. Jan 2018)
#     calibration : 2018-04-01 .. 2018-04-30  (all orders, late + on-time)
#     test        : 2018-05-01 .. 2018-08-31  (unchanged, never touched)
#   April is used for three things only: the alert threshold, a check of
#   ranking quality, and turning raw scores into real probabilities.
# ================================================================

import json, zipfile, warnings
import numpy as np
import pandas as pd
import joblib
import sklearn, xgboost
from xgboost import XGBClassifier
from sklearn.isotonic import IsotonicRegression
from sklearn.metrics import roc_auc_score, average_precision_score, confusion_matrix

warnings.filterwarnings("ignore")
SEED = 42
FPR_TARGET = 0.10        # alert threshold: flag 10% of on-time April orders
WEIGHT_MULTIPLIER = 0.75 # same class-weight setting as Experiment 13

print("scikit-learn", sklearn.__version__, "| xgboost", xgboost.__version__)

# ------------------------------------------------ 1. data and split
if "clean" not in globals():
    clean = pd.read_csv("orders_clean.csv", parse_dates=["purchase_ts"])
clean = clean.sort_values("purchase_ts").reset_index(drop=True)
clean["is_late"] = clean["is_late"].astype(int)
P = clean["purchase_ts"]

TRAIN_END, CAL_END, TEST_END = "2018-04-01", "2018-05-01", "2018-09-01"
TR = clean[(P >= "2017-01-01") & (P < TRAIN_END)].copy()
CA = clean[(P >= TRAIN_END) & (P < CAL_END)].copy()
TE = clean[(P >= CAL_END) & (P < TEST_END)].copy()

assert set(TR.order_id).isdisjoint(CA.order_id) and set(CA.order_id).isdisjoint(TE.order_id)
assert len(TE) == 24928 and TE.is_late.sum() == 1114, "test set should be unchanged"
assert CA.is_late.sum() > 0, "calibration month must contain late orders"

y_tr, y_ca, y_te = TR.is_late.to_numpy(), CA.is_late.to_numpy(), TE.is_late.to_numpy()
for name, d, y in [("train", TR, y_tr), ("calibration", CA, y_ca), ("test", TE, y_te)]:
    print(f"{name:12s}: {len(d):6,} rows | {y.sum():5,} late ({y.mean():.1%}) | "
          f"{d.purchase_ts.min().date()} .. {d.purchase_ts.max().date()}")

# ------------------------------------------------ 2. features (same 12 as XGB_12)
FEATURES = ["promise_days", "purchase_hour", "price", "freight", "category", "customer_state",
            "dist_km", "seller_overrun_rate_prior", "seller_handling_days_prior",
            "route_recent_days", "promise_buffer_route", "promise_buffer_state"]
CATEGORICAL = ["category", "customer_state"]

def build_bundle(train_df):
    top = train_df["category"].value_counts().head(20).index.tolist()
    tmp = train_df[FEATURES].copy()
    tmp["category"] = tmp["category"].where(tmp["category"].isin(top), "other")
    codes = {c: {v: i for i, v in enumerate(sorted(tmp[c].astype(str).unique()))} for c in CATEGORICAL}
    if "other" not in codes["category"]:
        codes["category"]["other"] = len(codes["category"])
    return {"features": FEATURES, "categorical": CATEGORICAL, "codes": codes, "top_categories": top}

def transform(df, bundle):
    X = df[bundle["features"]].copy()
    X["category"] = X["category"].where(X["category"].isin(bundle["top_categories"]), "other")
    for c in bundle["categorical"]:
        X[c] = X[c].astype(str).map(bundle["codes"][c]).fillna(-1).astype(int)
    return X

bundle = build_bundle(TR)
X_tr, X_ca, X_te = transform(TR, bundle), transform(CA, bundle), transform(TE, bundle)

# ------------------------------------------------ 3. train (Experiment 13 settings)
positive_weight = (y_tr == 0).sum() / (y_tr == 1).sum() * WEIGHT_MULTIPLIER
params = dict(n_estimators=600, max_depth=2, learning_rate=0.03, min_child_weight=10,
              subsample=0.9, colsample_bytree=0.9, reg_lambda=10.0)
model = XGBClassifier(**params, objective="binary:logistic", eval_metric="logloss",
                      scale_pos_weight=positive_weight, tree_method="hist",
                      random_state=SEED, n_jobs=-1, verbosity=0)
model.fit(X_tr, y_tr)
print(f"\npositive weight: {positive_weight:.4f}")

# ------------------------------------------------ 4. calibrate on April (late + on-time)
raw_ca = model.predict_proba(X_ca)[:, 1]
raw_te = model.predict_proba(X_te)[:, 1]

# Alert threshold: flag FPR_TARGET of on-time April orders.
threshold = float(np.quantile(raw_ca[y_ca == 0], 1 - FPR_TARGET))

# Turn class-weighted raw scores into real late probabilities for the dashboard.
calibrator = IsotonicRegression(out_of_bounds="clip", y_min=0, y_max=1).fit(raw_ca, y_ca)
prob_te = calibrator.predict(raw_te)

def at_threshold(y, s, t):
    a = (s >= t).astype(int)
    tn, fp, fn, tp = confusion_matrix(y, a, labels=[0, 1]).ravel()
    prec, rec = tp / max(tp + fp, 1), tp / max(tp + fn, 1)
    return dict(threshold=t, alert_rate=a.mean(), fpr=fp / max(fp + tn, 1), precision=prec, recall=rec,
                f1=2 * prec * rec / max(prec + rec, 1e-9), TP=int(tp), FP=int(fp), TN=int(tn), FN=int(fn))

def top_k(y, s, k):
    n = max(1, int(len(y) * k)); tp = int(y[np.argsort(-s)[:n]].sum())
    return dict(alerts=n, TP=tp, precision=tp / n, recall=tp / y.sum())

cal_metrics = dict(pr_auc=average_precision_score(y_ca, raw_ca), roc_auc=roc_auc_score(y_ca, raw_ca))
test_metrics = dict(
    pr_auc=average_precision_score(y_te, raw_te), roc_auc=roc_auc_score(y_te, raw_te), base_rate=y_te.mean(),
    at_threshold=at_threshold(y_te, raw_te, threshold),
    top5=top_k(y_te, raw_te, 0.05), top10=top_k(y_te, raw_te, 0.10), top20=top_k(y_te, raw_te, 0.20),
    mean_raw_score=float(raw_te.mean()), mean_calibrated_prob=float(prob_te.mean()),
)

print("\n" + "=" * 65)
print("XGB_12 — CORRECTED SPLIT")
print("=" * 65)
print(f"April calibration : PR-AUC {cal_metrics['pr_auc']:.4f} | ROC-AUC {cal_metrics['roc_auc']:.4f}")
print(f"Test              : PR-AUC {test_metrics['pr_auc']:.4f} | ROC-AUC {test_metrics['roc_auc']:.4f} "
      f"| base rate {test_metrics['base_rate']:.2%}")
t = test_metrics["at_threshold"]
print(f"Threshold {threshold:.4f} (10% FPR on April on-time orders) -> test: "
      f"FPR {t['fpr']:.1%}, alerts {t['alert_rate']:.1%}, recall {t['recall']:.1%}, "
      f"precision {t['precision']:.1%}, TP {t['TP']}, FP {t['FP']}")
for k in ["top5", "top10", "top20"]:
    r = test_metrics[k]
    print(f"Riskiest {k[3:]:>2}% of test orders: {r['TP']} late caught | recall {r['recall']:.1%} | precision {r['precision']:.1%}")
print(f"Mean raw score {test_metrics['mean_raw_score']:.3f} -> mean calibrated probability "
      f"{test_metrics['mean_calibrated_prob']:.3f} (true late rate {y_te.mean():.3f})")

# Side-by-side with the earlier final fit (numbers from final_model_metrics.json)
print("\nEarlier XGB_12 (old split): PR-AUC 0.0975 | ROC-AUC 0.6956 | threshold 0.9150 | "
      "test FPR 3.8% | recall 14.7% | precision 15.3%")

# ------------------------------------------------ 5. save bundle + inference code
bundle.update({
    "model": model, "model_name": "XGB_12", "algorithm": "XGBoost",
    "threshold": threshold, "fpr_target": FPR_TARGET, "calibrator": calibrator,
    "weight_multiplier": WEIGHT_MULTIPLIER, "positive_weight": float(positive_weight), "config": params,
    "split": {"train": ["2017-01-01", TRAIN_END], "calibration": [TRAIN_END, CAL_END], "test": [CAL_END, TEST_END]},
    "trained_on": {"rows": int(len(TR)), "late": int(y_tr.sum())},
    "versions": {"scikit-learn": sklearn.__version__, "xgboost": xgboost.__version__},
    "calibration_metrics": cal_metrics, "test_metrics": test_metrics,
})
joblib.dump(bundle, "model1_production_XGB_12.joblib")

pd.DataFrame({"order_id": TE.order_id.values, "actual_is_late": y_te, "raw_score": raw_te,
              "late_probability": prob_te, "alert": (raw_te >= threshold).astype(int)}
             ).to_csv("model1_production_test_predictions.csv", index=False)

json.dump({"calibration": cal_metrics, "test": test_metrics, "threshold": threshold,
           "split": bundle["split"], "versions": bundle["versions"]},
          open("model1_production_metrics.json", "w"), indent=2, default=float)

INFERENCE_CODE = '''import joblib
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
'''
open("model1_inference.py", "w").write(INFERENCE_CODE)

files_out = ["model1_production_XGB_12.joblib", "model1_inference.py",
             "model1_production_metrics.json", "model1_production_test_predictions.csv"]
with zipfile.ZipFile("Model1_Production_XGB12.zip", "w", zipfile.ZIP_DEFLATED) as z:
    for f in files_out:
        z.write(f)
print("\nSaved: Model1_Production_XGB12.zip ->", files_out)

try:
    from google.colab import files
    files.download("Model1_Production_XGB12.zip")
except Exception:
    pass
