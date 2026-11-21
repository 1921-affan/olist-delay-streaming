"""
Add the approval and handoff inputs to shipment_events.csv, so every event the
scorer receives already carries what it needs. No order-state lookup is needed
to score Model 3.

  order_approved     + approval_lag_h                      (rule R1)
  handed_to_carrier  + approval_lag_h, handling_days,
                       seller_overrun_days, post_approval_days,
                       shipping_limit_ts, and the 23 Model 1 columns
                       (= all 27 Model 3 inputs, rules R2)
  delivered          unchanged (is_late_actual)

Every value is computed only from times known at that event, with the same
formulas used in orders_clean.csv:
  approval_lag_h      = (approved_ts - purchase_ts) in hours
  handling_days       = (carrier_ts  - purchase_ts) in days
  seller_overrun_days = (carrier_ts  - shipping_limit_ts) in days
  post_approval_days  = (carrier_ts  - approved_ts) in days

bad_timestamp = 1 when the event is dated before the order was placed
(a known Olist data error). The producer should skip or dead-letter these.

Usage: python enrich_shipment_events.py --stream orders_stream.csv \
           --events shipment_events.csv --out shipment_events_enriched.csv
"""
import argparse
import numpy as np
import pandas as pd

STAGE1 = ["promise_days", "purchase_hour", "purchase_weekday", "price", "freight", "n_items", "weight_g",
          "volume_cm3", "category", "customer_state", "seller_state", "same_state", "dist_km", "pay_type",
          "installments", "seller_prior_orders", "seller_late_rate_prior", "seller_overrun_rate_prior",
          "seller_handling_days_prior", "route_recent_days", "promise_buffer_route", "promise_buffer_state",
          "order_load_ratio"]
HANDOFF = ["approval_lag_h", "handling_days", "seller_overrun_days", "post_approval_days"]
DAY, HOUR = 86400.0, 3600.0


def main(stream_path, events_path, out_path):
    s = pd.read_csv(stream_path, parse_dates=["event_time", "shipping_limit_ts"])
    e = pd.read_csv(events_path, parse_dates=["event_time", "estimated_delivery_date"])
    s = s.rename(columns={"event_time": "purchase_ts"})

    times = (e.pivot_table(index="order_id", columns="event_type", values="event_time", aggfunc="first")
               .rename(columns={"order_approved": "approved_ts", "handed_to_carrier": "carrier_ts"})
               [["approved_ts", "carrier_ts"]].reset_index())
    o = s[["order_id", "purchase_ts", "shipping_limit_ts"] + STAGE1].merge(times, on="order_id", how="left")
    o["approval_lag_h"] = (o.approved_ts - o.purchase_ts).dt.total_seconds() / HOUR
    o["handling_days"] = (o.carrier_ts - o.purchase_ts).dt.total_seconds() / DAY
    o["seller_overrun_days"] = (o.carrier_ts - o.shipping_limit_ts).dt.total_seconds() / DAY
    o["post_approval_days"] = (o.carrier_ts - o.approved_ts).dt.total_seconds() / DAY

    extra = ["order_id", "purchase_ts", "shipping_limit_ts"] + [c for c in STAGE1 if c not in e.columns] + HANDOFF
    out = e.merge(o[extra], on="order_id", how="left")
    approved, handoff = out.event_type == "order_approved", out.event_type == "handed_to_carrier"

    # Each event carries only what is known at that moment.
    out.loc[~(approved | handoff), "approval_lag_h"] = np.nan
    later = [c for c in STAGE1 if c not in e.columns]   # customer_state is already on every event
    out.loc[~handoff, ["handling_days", "seller_overrun_days", "post_approval_days", "shipping_limit_ts"] + later] = np.nan
    out["bad_timestamp"] = (out.event_time < out.purchase_ts).astype(int)
    out = out.drop(columns="purchase_ts")

    cols = list(e.columns) + ["bad_timestamp", "approval_lag_h", "handling_days", "seller_overrun_days",
                              "post_approval_days", "shipping_limit_ts"] + later
    out[cols].to_csv(out_path, index=False, date_format="%Y-%m-%d %H:%M:%S")

    print(f"{len(out):,} events written to {out_path}")
    print(out.groupby("event_type")[["approval_lag_h", "handling_days", "promise_days"]].count()
          .rename(columns=lambda c: c + " filled"))
    print("bad_timestamp events:", out.groupby("event_type").bad_timestamp.sum().to_dict())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--stream", default="orders_stream.csv")
    ap.add_argument("--events", default="shipment_events.csv")
    ap.add_argument("--out", default="shipment_events_enriched.csv")
    a = ap.parse_args()
    main(a.stream, a.events, a.out)
