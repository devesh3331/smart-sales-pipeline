import pandas as pd
import os
from datetime import datetime

# ── Load processed data ────────────────────────────────────
fact = pd.read_csv("data/processed/fact_orders.csv")

print("=" * 50)
print("DATA QUALITY REPORT")
print(f"Run time: {datetime.now()}")
print(f"Total records: {len(fact)}")
print("=" * 50)

errors = []

# ── Check 1: Null order_id ─────────────────────────────────
null_order_ids = fact["order_id"].isna().sum()
status = "✓ PASS" if null_order_ids == 0 else "✗ FAIL"
print(f"\n[{status}] Null order_id count: {null_order_ids}")
if null_order_ids > 0:
    errors.append(f"Null order_ids found: {null_order_ids}")

# ── Check 2: Negative payment values ──────────────────────
negative_payments = (fact["payment_value"] < 0).sum()
status = "✓ PASS" if negative_payments == 0 else "✗ FAIL"
print(f"[{status}] Negative payment_value count: {negative_payments}")
if negative_payments > 0:
    errors.append(f"Negative payments found: {negative_payments}")

# ── Check 3: Future order dates ────────────────────────────
fact["order_date"] = pd.to_datetime(fact["order_date"], errors="coerce")
future_dates = (fact["order_date"] > pd.Timestamp.today()).sum()
status = "✓ PASS" if future_dates == 0 else "✗ FAIL"
print(f"[{status}] Future order_date count: {future_dates}")
if future_dates > 0:
    errors.append(f"Future dates found: {future_dates}")

# ── Check 4: Row count threshold ──────────────────────────
MIN_EXPECTED_ROWS = 50000
total_rows = len(fact)
status = "✓ PASS" if total_rows >= MIN_EXPECTED_ROWS else "✗ FAIL"
print(f"[{status}] Row count check: {total_rows} (min expected: {MIN_EXPECTED_ROWS})")
if total_rows < MIN_EXPECTED_ROWS:
    errors.append(f"Row count too low: {total_rows}")

# ── Check 5: Duplicate order_ids ──────────────────────────
duplicates = fact["order_id"].duplicated().sum()
status = "✓ PASS" if duplicates == 0 else "✗ FAIL"
print(f"[{status}] Duplicate order_ids: {duplicates}")
if duplicates > 0:
    errors.append(f"Duplicate order_ids: {duplicates}")

# ── Check 6: Null customer_state ──────────────────────────
null_states = fact["customer_state"].isna().sum()
status = "✓ PASS" if null_states == 0 else "✗ FAIL"
print(f"[{status}] Null customer_state count: {null_states}")
if null_states > 0:
    errors.append(f"Null customer_state: {null_states}")

# ── Check 7: Invalid delivery days (negative) ─────────────
if "delivery_days" in fact.columns:
    invalid_delivery = (fact["delivery_days"] < 0).sum()
    status = "✓ PASS" if invalid_delivery == 0 else "✗ FAIL"
    print(f"[{status}] Invalid delivery_days (negative): {invalid_delivery}")
    if invalid_delivery > 0:
        errors.append(f"Negative delivery_days: {invalid_delivery}")

# ── Summary ────────────────────────────────────────────────
print("\n" + "=" * 50)
if len(errors) == 0:
    print("✓ ALL CHECKS PASSED — data is clean")
else:
    print(f"✗ {len(errors)} CHECK(S) FAILED:")
    for e in errors:
        print(f"  → {e}")

# ── Save quality report as log ─────────────────────────────
os.makedirs("logs", exist_ok=True)
log_file = f"logs/quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
with open(log_file, "w", encoding="utf-8") as f:
    f.write(f"Quality Report — {datetime.now()}\n")
    f.write(f"Total records: {total_rows}\n\n")
    if errors:
        f.write("FAILURES:\n")
        for e in errors:
            f.write(f"  → {e}\n")
    else:
        f.write("ALL CHECKS PASSED\n")

print(f"\n✓ Report saved to {log_file}")
print("\nDay 3 project task complete ✓")