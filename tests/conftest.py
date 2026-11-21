import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
DATA_ORDERS = os.path.join(ROOT, "data", "orders_stream.csv")
DATA_EVENTS = os.path.join(ROOT, "data", "shipment_events_enriched.csv")
