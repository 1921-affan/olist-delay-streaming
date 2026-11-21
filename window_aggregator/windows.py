"""Event-time tumbling windows (1 simulated day) per customer state and per seller, plus R8.

Pure Python: state can be saved to / restored from JSON, so a restart continues exactly.
"""
from collections import defaultdict
from datetime import datetime, timedelta

DAY = timedelta(days=1)

STATE_FIELDS = ["orders_placed", "m1_prob_sum", "m1_alerts", "approvals", "r1_slow_approvals",
                "handoffs", "m3_prob_sum", "m3_alerts", "deliveries", "late_deliveries"]
SELLER_FIELDS = ["handoffs", "m3_prob_sum", "r2_missed_deadline", "deliveries", "late_deliveries"]


def day_start(t):
    return datetime(t.year, t.month, t.day)


def parse(t):
    return datetime.fromisoformat(str(t).replace("Z", "")).replace(tzinfo=None)


class WindowAggregator:
    def __init__(self, grace_hours=6, r8_late_rate=0.15, r8_min_deliveries=50, r8_days=7, max_lateness_days=7):
        self.grace = timedelta(hours=grace_hours)
        self.r8_late_rate = r8_late_rate
        self.r8_min_deliveries = r8_min_deliveries
        self.r8_days = r8_days
        self.watermark = None                     # max event_time seen - grace
        self.max_event_time = None
        self.state = defaultdict(lambda: dict.fromkeys(STATE_FIELDS, 0))     # (state, day_iso) -> counters
        self.seller = defaultdict(lambda: dict.fromkeys(SELLER_FIELDS, 0))   # (seller, day_iso) -> counters
        self.closed_days = set()                  # day_iso already closed
        self.history = {}                         # (state, day_iso) -> (deliveries, late) for R8, closed days only
        self.alerted = set()                      # (state, day_iso) R8 alerts already raised
        self.dirty_state, self.dirty_seller = set(), set()
        self.late_events = 0
        self.seen = defaultdict(set)              # day_iso -> prediction ids already counted (dedup)
        self.duplicates = 0
        self.max_lateness = timedelta(days=max_lateness_days)   # allowed lateness behind the watermark
        self.dropped_too_late = 0

    # ------------------------------------------------------------ input
    def add(self, p):
        """p: prediction record from `delay-predictions`."""
        t = parse(p["event_time"])
        if self.watermark is not None and t < day_start(self.watermark) - self.max_lateness:
            self.dropped_too_late += 1            # beyond allowed lateness (e.g. a replay mistake) -> ignore
            return
        day = day_start(t).isoformat()
        pid = p.get("prediction_id") or (f"{p['order_id']}:{p['stage']}" if p.get("order_id") else None)
        if pid is not None:
            if pid in self.seen[day]:             # re-delivered prediction (at-least-once) -> count once
                self.duplicates += 1
                return
            self.seen[day].add(pid)
        st = p.get("customer_state") or "??"
        if day in self.closed_days:
            self.late_events += 1                 # applied anyway; window row is re-written
        c = self.state[(st, day)]
        stage, prob, alert = p["stage"], p.get("late_probability"), bool(p.get("alert"))
        tags = p.get("reason_tags") or []
        if stage == "placed":
            c["orders_placed"] += 1
            c["m1_prob_sum"] += prob or 0.0
            c["m1_alerts"] += alert
        elif stage == "approved":
            c["approvals"] += 1
            c["r1_slow_approvals"] += "R1_slow_approval" in tags
        elif stage == "handoff":
            c["handoffs"] += 1
            c["m3_prob_sum"] += prob or 0.0
            c["m3_alerts"] += alert
        elif stage == "delivered":
            c["deliveries"] += 1
            c["late_deliveries"] += int(p.get("is_late_actual") or 0)
        self.dirty_state.add((st, day))

        seller = p.get("seller_id")
        if seller and stage in ("handoff", "delivered"):
            s = self.seller[(seller, day)]
            if stage == "handoff":
                s["handoffs"] += 1
                s["m3_prob_sum"] += prob or 0.0
                s["r2_missed_deadline"] += "R2_missed_ship_deadline" in tags
            else:
                s["deliveries"] += 1
                s["late_deliveries"] += int(p.get("is_late_actual") or 0)
            self.dirty_seller.add((seller, day))

        if self.max_event_time is None or t > self.max_event_time:
            self.max_event_time = t
            self.watermark = t - self.grace
        if day in self.closed_days:               # late event: refresh R8 history for that closed day
            self.history[(st, day)] = (c["deliveries"], c["late_deliveries"])

    # ------------------------------------------------------------ closing windows
    def close_ready(self):
        """Close every day whose end is before the watermark. Returns list of R8 alerts raised."""
        if self.watermark is None:
            return []
        open_days = sorted({d for (_, d) in self.state} - self.closed_days)
        alerts = []
        for day in open_days:
            if datetime.fromisoformat(day) + DAY > self.watermark:
                break
            self.closed_days.add(day)
            for (st, d), c in self.state.items():
                if d == day:
                    self.history[(st, d)] = (c["deliveries"], c["late_deliveries"])
                    self.dirty_state.add((st, d))
            alerts += self._r8(day)
        return alerts

    def rolling(self, st, day):
        """7-day rolling deliveries and late rate ending on `day` (closed days only)."""
        end = datetime.fromisoformat(day)
        dl = late = 0
        for k in range(self.r8_days):
            d = (end - k * DAY).isoformat()
            a, b = self.history.get((st, d), (0, 0))
            dl, late = dl + a, late + b
        return dl, (late / dl if dl else None)

    def _r8(self, day):
        out = []
        for st in sorted({s for (s, d) in self.history if d == day}):
            dl, rate = self.rolling(st, day)
            if rate is None or dl < self.r8_min_deliveries or rate < self.r8_late_rate:
                continue
            prev = (datetime.fromisoformat(day) - DAY).isoformat()
            already = (st, day) in self.alerted
            self.alerted.add((st, day))
            if not already and (st, prev) not in self.alerted:       # alert only when the state first crosses
                out.append({
                    "alert_id": f"region_surge:{st}:{day[:10]}",
                    "alert_type": "region_surge", "order_id": None, "stage": None, "severity": "high",
                    "score": rate, "reasons": [f"R8_region_spike_{self.r8_days}d"],
                    "customer_state": st, "seller_id": None,
                    "event_time": (datetime.fromisoformat(day) + DAY).isoformat(),
                    "detail": {"rolling_deliveries": dl, "rolling_late_rate": rate},
                })
        return out

    # ------------------------------------------------------------ output rows
    def state_row(self, key):
        st, day = key
        c = self.state[key]
        dl, rate7 = self.rolling(st, day) if day in self.closed_days else (None, None)
        return {
            "customer_state": st, "window_start": day,
            "window_end": (datetime.fromisoformat(day) + DAY).isoformat(),
            "orders_placed": c["orders_placed"],
            "avg_m1_probability": c["m1_prob_sum"] / c["orders_placed"] if c["orders_placed"] else None,
            "m1_alerts": c["m1_alerts"], "approvals": c["approvals"], "r1_slow_approvals": c["r1_slow_approvals"],
            "handoffs": c["handoffs"],
            "avg_m3_probability": c["m3_prob_sum"] / c["handoffs"] if c["handoffs"] else None,
            "m3_alerts": c["m3_alerts"], "deliveries": c["deliveries"], "late_deliveries": c["late_deliveries"],
            "late_rate": c["late_deliveries"] / c["deliveries"] if c["deliveries"] else None,
            "rolling7_deliveries": dl, "rolling7_late_rate": rate7,
        }

    def seller_row(self, key):
        seller, day = key
        s = self.seller[key]
        return {"seller_id": seller, "window_start": day, "handoffs": s["handoffs"],
                "avg_m3_probability": s["m3_prob_sum"] / s["handoffs"] if s["handoffs"] else None,
                "r2_missed_deadline": s["r2_missed_deadline"], "deliveries": s["deliveries"],
                "late_deliveries": s["late_deliveries"]}

    def take_dirty(self):
        """Rows changed since the last checkpoint (open windows are written too, so dashboards are live)."""
        st = [self.state_row(k) for k in sorted(self.dirty_state)]
        se = [self.seller_row(k) for k in sorted(self.dirty_seller)]
        self.dirty_state, self.dirty_seller = set(), set()
        return st, se

    def evict(self, keep_days=10):
        """Forget windows older than keep_days behind the watermark (already saved in the DB)."""
        if self.watermark is None:
            return
        cutoff = (day_start(self.watermark) - timedelta(days=keep_days)).isoformat()
        for store in (self.state, self.seller):
            for k in [k for k in store if k[1] < cutoff]:
                del store[k]
        self.history = {k: v for k, v in self.history.items() if k[1] >= cutoff}
        self.closed_days = {d for d in self.closed_days if d >= cutoff}
        self.alerted = {k for k in self.alerted if k[1] >= cutoff}
        for d in [d for d in self.seen if d < cutoff]:
            del self.seen[d]

    # ------------------------------------------------------------ checkpoint
    def to_dict(self):
        return {
            "max_event_time": self.max_event_time.isoformat() if self.max_event_time else None,
            "state": [[k[0], k[1], v] for k, v in self.state.items()],
            "seller": [[k[0], k[1], v] for k, v in self.seller.items()],
            "closed_days": sorted(self.closed_days),
            "history": [[k[0], k[1], v[0], v[1]] for k, v in self.history.items()],
            "alerted": [list(k) for k in self.alerted],
            "late_events": self.late_events,
            "seen": {d: sorted(ids) for d, ids in self.seen.items()},
            "duplicates": self.duplicates,
            "dropped_too_late": self.dropped_too_late,
        }

    def load(self, d):
        if d.get("max_event_time"):
            self.max_event_time = parse(d["max_event_time"])
            self.watermark = self.max_event_time - self.grace
        for st, day, v in d.get("state", []):
            self.state[(st, day)] = v
        for se, day, v in d.get("seller", []):
            self.seller[(se, day)] = v
        self.closed_days = set(d.get("closed_days", []))
        self.history = {(a, b): (c, e) for a, b, c, e in d.get("history", [])}
        self.alerted = {tuple(k) for k in d.get("alerted", [])}
        self.late_events = d.get("late_events", 0)
        for day, ids in d.get("seen", {}).items():
            self.seen[day] = set(ids)
        self.duplicates = d.get("duplicates", 0)
        self.dropped_too_late = d.get("dropped_too_late", 0)
