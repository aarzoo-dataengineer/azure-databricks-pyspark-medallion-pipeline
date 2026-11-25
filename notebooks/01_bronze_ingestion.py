# Databricks notebook source
# COMMAND ----------
# Bronze: preserve source records with ingestion metadata.
# For local execution, use: python scripts/run_pipeline.py

from pyspark.sql import functions as F
from src.config import CUSTOMERS_PATH, PRODUCTS_PATH, ORDERS_PATH, OUTPUT_DIR
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, ORDER_SCHEMA

spark.conf.set("spark.sql.session.timeZone", "UTC")

customers_raw = (
    spark.read.option("header", True).schema(CUSTOMER_SCHEMA).csv(CUSTOMERS_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)
products_raw = (
    spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv(PRODUCTS_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)
orders_raw = (
    spark.read.option("header", True).schema(ORDER_SCHEMA).csv(ORDERS_PATH)
    .withColumn("_source_file", F.input_file_name())
    .withColumn("_ingested_at", F.current_timestamp())
)

base = str(OUTPUT_DIR / "bronze")
customers_raw.write.format("delta").mode("overwrite").save(f"{base}/customers")
products_raw.write.format("delta").mode("overwrite").save(f"{base}/products")
orders_raw.write.format("delta").mode("overwrite").save(f"{base}/orders")
