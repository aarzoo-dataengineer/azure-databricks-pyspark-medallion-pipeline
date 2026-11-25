# Databricks notebook source
# Silver: standardize, deduplicate, and apply basic validity rules.

from delta.tables import DeltaTable
from src.config import OUTPUT_DIR
from src.transformations import clean_customers, clean_products, clean_orders

bronze = str(OUTPUT_DIR / "bronze")
silver = str(OUTPUT_DIR / "silver")

customers = clean_customers(spark.read.format("delta").load(f"{bronze}/customers"))
products = clean_products(spark.read.format("delta").load(f"{bronze}/products"))
orders = clean_orders(spark.read.format("delta").load(f"{bronze}/orders"))

customers.write.format("delta").mode("overwrite").save(f"{silver}/customers")
products.write.format("delta").mode("overwrite").save(f"{silver}/products")
orders.write.format("delta").mode("overwrite").save(f"{silver}/orders")
