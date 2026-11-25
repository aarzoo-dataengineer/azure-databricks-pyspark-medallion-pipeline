# Databricks notebook source
# Gold: publish business-level sales aggregates.

from src.config import OUTPUT_DIR
from src.transformations import build_sales_enriched, build_gold_tables

silver = str(OUTPUT_DIR / "silver")
gold = str(OUTPUT_DIR / "gold")

orders = spark.read.format("delta").load(f"{silver}/orders")
customers = spark.read.format("delta").load(f"{silver}/customers")
products = spark.read.format("delta").load(f"{silver}/products")

enriched = build_sales_enriched(orders, customers, products)
daily_sales, product_sales, customer_sales = build_gold_tables(enriched)

daily_sales.write.format("delta").mode("overwrite").save(f"{gold}/daily_sales")
product_sales.write.format("delta").mode("overwrite").save(f"{gold}/product_sales")
customer_sales.write.format("delta").mode("overwrite").save(f"{gold}/customer_sales")
