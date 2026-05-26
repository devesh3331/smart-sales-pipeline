from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, to_date

# Start Spark session
spark = SparkSession.builder \
    .appName("SmartSalesPipeline") \
    .getOrCreate()

# Load raw data
orders = spark.read.csv("data/raw/olist_orders_dataset.csv", header=True, inferSchema=True)
customers = spark.read.csv("data/raw/olist_customers_dataset.csv", header=True, inferSchema=True)
payments = spark.read.csv("data/raw/olist_order_payments_dataset.csv", header=True, inferSchema=True)
items = spark.read.csv("data/raw/olist_order_items_dataset.csv", header=True, inferSchema=True)

# Quick exploration
print("=== ORDERS ===")
print(f"Total orders: {orders.count()}")
orders.printSchema()
orders.show(5)

print("=== PAYMENTS ===")
print(f"Total payment records: {payments.count()}")
payments.show(5)

# Basic metric — total revenue
total_revenue = payments.agg(sum("payment_value").alias("total_revenue"))
print("=== TOTAL REVENUE ===")
total_revenue.show()

# Order status breakdown
print("=== ORDER STATUS BREAKDOWN ===")
orders.groupBy("order_status").count().orderBy("count", ascending=False).show()