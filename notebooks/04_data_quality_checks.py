# Databricks notebook source
# Data quality: table-level counts and orphan order report.

from src.config import OUTPUT_DIR
from src.data_quality import get_quality_summary, find_orphan_orders

silver = str(OUTPUT_DIR / "silver")
quality = str(OUTPUT_DIR / "quality")

customers = spark.read.format("delta").load(f"{silver}/customers")
products = spark.read.format("delta").load(f"{silver}/products")
orders = spark.read.format("delta").load(f"{silver}/orders")

summary = get_quality_summary(customers, products, orders)
orphans = find_orphan_orders(orders, customers, products)

summary.show(truncate=False)
orphans.show(truncate=False)

summary.write.format("delta").mode("overwrite").save(f"{quality}/summary")
orphans.write.format("delta").mode("overwrite").save(f"{quality}/orphan_orders")
