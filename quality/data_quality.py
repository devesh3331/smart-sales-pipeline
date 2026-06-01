from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan
from datetime import datetime
import os

spark = SparkSession.builder \
    .appName("DataQualityChecks") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# ── Load processed data ────────────────────────────────────
fact = spark.read.parquet("data/processed/fact_orders/")

print("=" * 50)
print("DATA QUALITY REPORT")
print(f"Run time: {datetime.now()}")
print(f"Total records: {fact.count()}")
print("=" * 50)

errors = []

# ── Check 1: Null order_id ─────────────────────────────────
null_order_ids = fact.filter(col("order_id").isNull()).count()
status = "✓ PASS" if null_order_ids == 0 else "✗ FAIL"
print(f"\n[{status}] Null order_id count: {null_order_ids}")
if null_order_ids > 0:
    errors.append(f"Null order_ids found: {null_order_ids}")

# ── Check 2: Negative payment values ──────────────────────
negative_payments = fact.filter(col("payment_value") < 0).count()
status = "✓ PASS" if negative_payments == 0 else "✗ FAIL"
print(f"[{status}] Negative payment_value count: {negative_payments}")
if negative_payments > 0:
    errors.append(f"Negative payments found: {negative_payments}")

# ── Check 3: Future order dates ────────────────────────────
from pyspark.sql.functions import current_date
future_dates = fact.filter(col("order_date") > current_date()).count()
status = "✓ PASS" if future_dates == 0 else "✗ FAIL"
print(f"[{status}] Future order_date count: {future_dates}")
if future_dates > 0:
    errors.append(f"Future dates found: {future_dates}")

# ── Check 4: Row count threshold ──────────────────────────
# Simulating: if total rows < 50000 something is wrong
MIN_EXPECTED_ROWS = 50000
total_rows = fact.count()
status = "✓ PASS" if total_rows >= MIN_EXPECTED_ROWS else "✗ FAIL"
print(f"[{status}] Row count check: {total_rows} (min expected: {MIN_EXPECTED_ROWS})")
if total_rows < MIN_EXPECTED_ROWS:
    errors.append(f"Row count too low: {total_rows}")

# ── Check 5: Duplicate order_ids ──────────────────────────
total = fact.count()
distinct = fact.select("order_id").distinct().count()
duplicates = total - distinct
status = "✓ PASS" if duplicates == 0 else "✗ FAIL"
print(f"[{status}] Duplicate order_ids: {duplicates}")
if duplicates > 0:
    errors.append(f"Duplicate order_ids: {duplicates}")

# ── Check 6: Null customer_state ──────────────────────────
null_states = fact.filter(col("customer_state").isNull()).count()
status = "✓ PASS" if null_states == 0 else "✗ FAIL"
print(f"[{status}] Null customer_state count: {null_states}")
if null_states > 0:
    errors.append(f"Null customer_state: {null_states}")

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
with open(log_file, "w") as f:
    f.write(f"Quality Report — {datetime.now()}\n")
    f.write(f"Total records: {total_rows}\n\n")
    if errors:
        f.write("FAILURES:\n")
        for e in errors:
            f.write(f"  → {e}\n")
    else:
        f.write("ALL CHECKS PASSED\n")

print(f"\n✓ Report saved to {log_file}")
print("Day 3 project task complete ✓")