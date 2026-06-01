import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
import os
os.makedirs("data/processed", exist_ok=True)

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, count, avg, round, to_date,
    datediff, when, year, month
)
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
# ── Spark session ──────────────────────────────────────────
spark = SparkSession.builder \
    .appName("SmartSalesPipeline-Transform") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")  # suppress noisy logs

# ── Load raw data ──────────────────────────────────────────
orders   = spark.read.csv("data/raw/olist_orders_dataset.csv",   header=True, inferSchema=True)
customers= spark.read.csv("data/raw/olist_customers_dataset.csv",header=True, inferSchema=True)
payments = spark.read.csv("data/raw/olist_order_payments_dataset.csv", header=True, inferSchema=True)
items    = spark.read.csv("data/raw/olist_order_items_dataset.csv",    header=True, inferSchema=True)
products = spark.read.csv("data/raw/olist_products_dataset.csv",       header=True, inferSchema=True)

print("✓ Raw data loaded")

# ── Step 1: Join orders + customers ───────────────────────
orders_customers = orders.join(customers, on="customer_id", how="left")

# ── Step 2: Join with payments ────────────────────────────
orders_payments = orders_customers.join(payments, on="order_id", how="left")

# ── Step 3: Join with items + products ────────────────────
items_products = items.join(products, on="product_id", how="left")
full = orders_payments.join(items_products, on="order_id", how="left")

print("✓ All tables joined")
print(f"  Total rows in joined dataset: {full.count()}")

# ── Step 4: Add derived columns ───────────────────────────
full = full \
    .withColumn("order_date", to_date(col("order_purchase_timestamp"))) \
    .withColumn("order_year",  year(col("order_date"))) \
    .withColumn("order_month", month(col("order_date"))) \
    .withColumn("delivery_days",
        datediff(
            col("order_delivered_customer_date"),
            col("order_purchase_timestamp")
        )
    ) \
    .withColumn("is_late",
        when(
            col("order_delivered_customer_date") > col("order_estimated_delivery_date"), 1
        ).otherwise(0)
    )

print("✓ Derived columns added")

# ── Metric 1: Daily revenue ───────────────────────────────
daily_revenue = full.groupBy("order_date") \
    .agg(
        round(sum("payment_value"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders")
    ) \
    .orderBy("order_date")

print("\n=== DAILY REVENUE (last 10 days) ===")
daily_revenue.orderBy("order_date", ascending=False).show(10)

# ── Metric 2: Revenue by product category ─────────────────
category_revenue = full.groupBy("product_category_name") \
    .agg(
        round(sum("payment_value"), 2).alias("total_revenue"),
        count("order_id").alias("total_orders"),
        round(avg("payment_value"), 2).alias("avg_order_value")
    ) \
    .orderBy("total_revenue", ascending=False)

print("\n=== TOP 10 CATEGORIES BY REVENUE ===")
category_revenue.show(10)

# ── Metric 3: Average delivery time by state ──────────────
delivery_by_state = full.filter(col("delivery_days").isNotNull()) \
    .groupBy("customer_state") \
    .agg(
        round(avg("delivery_days"), 1).alias("avg_delivery_days"),
        round(avg("is_late") * 100, 1).alias("late_delivery_pct")
    ) \
    .orderBy("avg_delivery_days")

print("\n=== DELIVERY PERFORMANCE BY STATE ===")
delivery_by_state.show()

# ── Metric 4: Monthly order trend ────────────────────────
monthly_trend = full.groupBy("order_year", "order_month") \
    .agg(
        count("order_id").alias("total_orders"),
        round(sum("payment_value"), 2).alias("total_revenue")
    ) \
    .orderBy("order_year", "order_month")

print("\n=== MONTHLY TREND ===")
monthly_trend.show()

# ── Step 5: Save processed output as Parquet ──────────────
import os
os.makedirs("data/processed", exist_ok=True)

# full.write.mode("overwrite").parquet("data/processed/fact_orders/")
# daily_revenue.write.mode("overwrite").parquet("data/processed/daily_revenue/")
# category_revenue.write.mode("overwrite").parquet("data/processed/category_revenue/")

import os
os.makedirs("data/processed", exist_ok=True)

# Save as CSV — works perfectly on Windows without Hadoop setup
full.toPandas().to_csv("data/processed/fact_orders.csv", index=False)
daily_revenue.toPandas().to_csv("data/processed/daily_revenue.csv", index=False)
category_revenue.toPandas().to_csv("data/processed/category_revenue.csv", index=False)
delivery_by_state.toPandas().to_csv("data/processed/delivery_by_state.csv", index=False)
monthly_trend.toPandas().to_csv("data/processed/monthly_trend.csv", index=False)

print("\n✓ Processed data saved to data/processed/ as CSV")
print("Day 2 complete ✓")

