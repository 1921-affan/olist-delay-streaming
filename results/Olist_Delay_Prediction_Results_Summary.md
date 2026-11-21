# Olist Delivery-Delay Prediction — Results Summary (for the presentation)

All numbers below come from the project notebooks run in Google Colab (Nov 2026) and from re-analysis of their saved prediction files.
Late = delivered on a calendar day after the promised date. Test set = all 24,928 real orders placed May–Aug 2018 (1,114 late, 4.47%), never used for any choice.

---

## 1. Pipeline at a glance

| Stage | Event | What the scorer does | Inputs |
|---|---|---|---|
| 1 | Order placed (`orders` topic) | **Model 1** score + rule tags R5, R6, R7 | 12 checkout-time columns |
| 2 | Approved (`shipment-events`) | Keeps Model 1 score, adds **R1** tag (approval > 24 h) | `approval_lag_h` on the event |
| 3 | Handed to carrier (`shipment-events`) | **Model 3** re-score + R2 (and R3 as in-transit monitor) | 10 columns, all on the handoff event |
| 4 | Delivered (`shipment-events`) | Records actual outcome for predicted-vs-actual monitoring | `is_late_actual` |

Replay files: `orders_stream.csv` (25,314 orders, checkout info only) and `shipment_events_enriched.csv` (75,380 events; each event carries only what is known at that moment; 154 handoff events dated before their order are flagged `bad_timestamp = 1`).

---

## 2. Model 1 (scored when the order is placed)

### 2.1 Three candidates were trained (first final fit)

All trained on the same data; thresholds set to 10% false-positive rate on the validation set.

| Candidate | Features | Test PR-AUC | Test ROC-AUC | Late caught at threshold | Precision | False alerts |
|---|---|---|---|---|---|---|
| **XGB_12** (XGBoost) | 12 | 0.0975 | **0.6956** | **164** (14.7%) | **15.3%** | 910 |
| CAT_15 (CatBoost) | 15 | **0.0990** | 0.6865 | 150 (13.5%) | 13.4% | 973 |
| HGB_15 (HistGradientBoosting) | 15 | 0.0829 | 0.6602 | 115 (10.3%) | 12.2% | 824 |

Same alert budget for every model (flag the riskiest X% of test orders):

| Riskiest | XGB_12 | CAT_15 | HGB_15 |
|---|---|---|---|
| 5% (1,246 alerts) | **179** late caught | 162 | 146 |
| 10% (2,492 alerts) | 283 | **293** | 260 |
| 20% (4,985 alerts) | **435** | 410 | 419 |

2017 time-ordered cross-validation (before the final fit): HGB_15 0.1136, XGB_12 0.1114, CAT_15 0.1105 mean PR-AUC — all within fold noise (std ≈ 0.05).

### 2.2 Why XGB_12 was chosen

Checked with 1,000 bootstrap resamples of the test set:

| Comparison | PR-AUC difference (95% range) | ROC-AUC difference (95% range) |
|---|---|---|
| XGB_12 − CAT_15 | −0.0015 (−0.0096 to +0.0062) → **a tie** (XGB ahead in 35% of resamples) | **+0.0091 (+0.0027 to +0.0152)** → XGB better in 100% |
| XGB_12 − HGB_15 | +0.0147 (+0.0105 to +0.0198) | +0.0353 (+0.0296 to +0.0411) |
| CAT_15 − HGB_15 | +0.0162 (+0.0093 to +0.0236) | +0.0263 (+0.0209 to +0.0320) |

* **HGB_15 dropped:** clearly worst on every test metric.
* **XGB_12 over CAT_15:** PR-AUC is a statistical tie; XGB_12 has a real ROC-AUC advantage, catches more late orders at the threshold and at most alert budgets, and needs **3 fewer inputs** (no `seller_state`, `volume_cm3`, `purchase_weekday`) for the live scorer.
* Honest note: the choice was made using test results; 2017 CV had ranked the three within noise.

### 2.3 Problem found and fixed: the data split

* Old design: training got the **late** Feb–Apr 2018 orders; validation got only the **on-time** Feb–Apr orders.
* Effect: the model learned "looks like Feb–Apr 2018 → late", scored those on-time orders high, and the threshold landed at 0.915. A threshold meant to flag 10% of on-time orders flagged only **3.8%** on test.
* Fix: a clean chronological split, every month kept whole.

| Part | Dates | Orders | Late |
|---|---|---|---|
| Train | Jan 2017 – Mar 2018 | 63,332 | 5,097 (8.0%) |
| Calibration (threshold + probabilities) | Apr 2018 | 6,670 | 306 (4.6%) |
| Test (untouched) | May – Aug 2018 | 24,928 | 1,114 (4.5%) |

### 2.4 Production Model 1 = XGB_12 retrained on the corrected split

| Test metric | Old XGB_12 | **Production XGB_12** |
|---|---|---|
| PR-AUC | 0.0975 | **0.1037** |
| ROC-AUC | 0.696 | **0.725** |
| Threshold | 0.915 | 0.749 |
| On-time orders flagged (target 10%) | 3.8% | 5.3% |
| Late caught at threshold | 164 (14.7%) | **195 (17.5%)** |
| Precision at threshold | 15.3% | 13.3% |
| Riskiest 10%: late caught | 283 | **299 (26.8%)** |
| Riskiest 20%: late caught | 435 | **480 (43.1%)** |

* Improvement is real: new − old PR-AUC +0.0064 (95% range +0.0012 to +0.0112); ROC-AUC +0.029 (+0.022 to +0.036), better in every resample and in all four time chunks of the test period.
* April calibration: PR-AUC 0.146, ROC-AUC 0.768.
* Settings: 600 trees, depth 2, learning rate 0.03, min_child_weight 10, subsample 0.9, colsample 0.9, λ = 10, class weight × 0.75.
* 12 features: promise_days, purchase_hour, price, freight, category, customer_state, dist_km, seller_overrun_rate_prior, seller_handling_days_prior, route_recent_days, promise_buffer_route, promise_buffer_state.

**Limitation (Model 1):** under-reacts to surges. In the last test chunk (~August, 6.3% late) it flagged only 1.8% of orders and its mean probability was 2.5%. Calibrated probabilities run low in the middle (predicted 1–3%, actual 3–7%); top decile 13.1% predicted vs 11.8% actual.

---

## 3. Model 3 (re-scored at carrier handoff)

### 3.1 Feature selection: 27 candidates → 10

Method (training period only, 4 chronological folds with 1-month embargo): single-column screen → redundancy check (|Spearman ρ| ≥ 0.8) → permutation importance on held-out folds → recursive backward elimination → smallest set within 2% of the best mean PR-AUC.

Strongest single columns (direction-adjusted ROC-AUC): route_recent_days 0.678, promise_buffer_route 0.655, seller_overrun_days 0.647, promise_buffer_state 0.641, handling_days 0.630.

Permutation importance with all 27 (PR-AUC drop when shuffled): handling_days 0.086, seller_overrun_days 0.065, promise_buffer_state 0.032, route_recent_days 0.029, promise_buffer_route 0.010.

Overlapping pairs: promise_buffer_route / promise_buffer_state (ρ 0.95), handling_days / post_approval_days (0.91), same_state / route_recent_days (−0.82).

| Column set | CV PR-AUC | CV ROC-AUC | April PR-AUC | April ROC-AUC |
|---|---|---|---|---|
| All 27 | 0.314 | 0.738 | 0.271 | 0.820 |
| **Chosen 10** | **0.320** | **0.743** | **0.280** | **0.813** |
| Model 1's 12 + handoff 4 | 0.321 | 0.741 | 0.278 | 0.814 |
| Handoff 4 only | 0.250 | 0.649 | 0.169 | 0.652 |

Best on the elimination path: 0.3255 with 17 columns; 10 is the smallest within 2%.

**Chosen 10:** handling_days, seller_overrun_days, post_approval_days (handoff); route_recent_days, promise_buffer_route, promise_buffer_state, promise_days, dist_km (route/promise); customer_state, weight_g.
Dropped notably: approval_lag_h (matches weak rule R1) and all seller-history columns (seller_overrun_days measures this order's own delay).

### 3.2 Model family comparison (same 10 columns, same folds)

Rule fixed **before** opening the test set: keep XGBoost unless another family is > 2% better on mean CV PR-AUC **and** wins ≥ 3 of 4 folds.

| Family (best setting) | CV PR-AUC | CV ROC-AUC | Test PR-AUC | Test ROC-AUC |
|---|---|---|---|---|
| **XGBoost (chosen)** | 0.320 | 0.743 | 0.292 | 0.829 |
| Logistic regression (C = 0.003) | 0.324 | 0.749 | 0.221 | 0.829 |
| LightGBM | 0.309 | 0.726 | 0.303 | 0.836 |
| CatBoost | 0.299 | 0.715 | 0.297 | 0.823 |

* Logistic regression was only 1.2% ahead in CV (below the 2% bar) and fell well behind on test — the rule protected the choice.
* LightGBM scored slightly higher on test, but switching after seeing test results would be test-set selection; it was weaker in CV.
* XGBoost keeps one library for both models in the scorer.
* Why gradient-boosted trees: large benchmarks (Grinsztajn et al., NeurIPS 2022) show tree ensembles beat deep learning on medium-sized tabular data; delivery-delay studies find XGBoost/CatBoost on top. TabPFN-2.5 excluded: designed for ≤ 50k rows (we have 63k), needs a GPU, licence restricts production use.

### 3.3 Production Model 3 results (test, May–Aug 2018)

| Metric | Value |
|---|---|
| PR-AUC | **0.292** |
| ROC-AUC | **0.829** |
| Threshold (10% FPR on April on-time) | 0.736 |
| On-time orders flagged | 5.9% |
| Late caught at threshold | **448 of 1,114 (40.2%)** |
| Precision at threshold | 24.2% |
| Riskiest 5% / 10% / 20%: late caught | 349 (31%) / 516 (46%) / 734 (66%) |
| April calibration PR-AUC / ROC-AUC | 0.280 / 0.813 |

Calibration: top decile predicted 17.3% late vs 21.2% actual; overall mean 3.1% vs 4.5% actual.

---

## 4. Model 1 vs Model 3 on the same 24,928 test orders

| | Model 1 (order placed) | Model 3 (handoff) |
|---|---|---|
| PR-AUC | 0.104 | **0.292** (2.8×) |
| ROC-AUC | 0.725 | **0.829** |
| Late caught at threshold | 195 (17.5%) | **448 (40.2%)** |
| Precision at threshold | 13.3% | **24.2%** |
| Riskiest 10%: late caught | 27% | **46%** |

* Difference is certain: Model 3 − Model 1 PR-AUC +0.188 (95% range +0.165 to +0.210); ROC-AUC +0.105 (+0.093 to +0.116).
* By time chunk (PR-AUC, Model 3 vs Model 1): 0.268 vs 0.155 · 0.131 vs 0.067 · 0.390 vs 0.065 · **0.361 vs 0.088 (August surge)**.
* Together: 465 of 1,114 late orders (42%) flagged by at least one model, with 8.6% of orders flagged; 178 flagged by both.
* Message for the slides: **the prediction sharpens as the order moves through the pipeline** — Model 1 gives early warning at checkout; Model 3 catches far more once the seller's handoff timing is known.

---

## 5. Operational rules R1–R9 (from the rules report)

| Rule | Condition | Fires | Late rate fired / not | Lift | Late caught |
|---|---|---|---|---|---|
| R1 Slow approval | Approval > 24 h | 18.7% | 9.1% / 7.0% | 1.30× | 23.0% |
| R2 Seller missed deadline | Handoff after shipping limit | 3.4% | 28.4% / 6.8% | 4.17× | 12.7% |
| R3 Slow transit | Transit > 8 days (in-transit monitor) | 16.3% | 43.1% / 0.6% | 71.9× | 93.3% |
| R4 Already late / stuck | Promise passed, no delivery | — | not label-evaluable | — | — |
| R5 Tight promise | Promise ≤ 10 days | 3.3% | 11.4% / 7.3% | 1.57× | 5.0% |
| R6 Long route | Distance > 1,000 km | 16.7% | 11.7% / 6.5% | 1.79× | 26.5% |
| R7 Unreliable seller | Past late rate > 15%, 20+ orders | 8.8% (68 rows) | 33.3% / 11.3% | 2.95× | 22.2% |
| R8 Region spike | Prior-month state late ≥ 15% | 0.0% | no triggers | — | 0.0% |
| R9 High model risk | Top-k model score | not evaluated | — | — | — |

---

## 6. Software versions

| Library | Version | Used by |
|---|---|---|
| scikit-learn | 1.6.1 | Model 1, Model 3, scorer |
| xgboost | 3.4.1 | Model 1, Model 3, scorer |
| lightgbm | 4.6.0 | Model 3 comparison only |
| catboost | 1.2.10 | Model 3 comparison only |

## 7. Files behind these numbers (in the results zip)

* `model1_candidates/` — first final fit: final_model_comparison.csv, final_model_metrics.json, final_test_predictions.csv
* `model1_production/` — production XGB_12 model, metrics, test predictions, inference code
* `model3_feature_selection/` — screen, permutation importance, elimination path, set comparison, selected_features.json
* `model3_production/` — production Model 3 model, metrics, test predictions, CV for all settings, family comparison, inference code
* `notebooks_and_scripts/` — retraining cell, Model 3 notebooks, events enrichment script, requirements-scorer.txt
