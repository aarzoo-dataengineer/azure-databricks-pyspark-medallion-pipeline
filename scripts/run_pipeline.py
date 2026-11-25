import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.spark_session import get_spark
from src.config import CUSTOMERS_PATH, PRODUCTS_PATH, ORDERS_PATH, OUTPUT_DIR
from src.schemas import CUSTOMER_SCHEMA, PRODUCT_SCHEMA, ORDER_SCHEMA
from src.transformations import (
    clean_customers, clean_products, clean_orders,
    build_sales_enriched, build_gold_tables,
)
from src.data_quality import get_quality_summary, find_orphan_orders
from pyspark.sql import functions as F


def main():
    spark = get_spark()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        # BRONZE
        customers_raw = spark.read.option("header", True).schema(CUSTOMER_SCHEMA).csv(CUSTOMERS_PATH)
        products_raw = spark.read.option("header", True).schema(PRODUCT_SCHEMA).csv(PRODUCTS_PATH)
        orders_raw = spark.read.option("header", True).schema(ORDER_SCHEMA).csv(ORDERS_PATH)

        for name, df in [
            ("customers", customers_raw), ("products", products_raw), ("orders", orders_raw)
        ]:
            df.withColumn("_source_file", F.input_file_name()) \
              .withColumn("_ingested_at", F.current_timestamp()) \
              .write.format("delta").mode("overwrite") \
              .save(str(OUTPUT_DIR / "bronze" / name))

        # SILVER
        customers = clean_customers(customers_raw)
        products = clean_products(products_raw)
        orders = clean_orders(orders_raw)

        for name, df in [
            ("customers", customers), ("products", products), ("orders", orders)
        ]:
            df.write.format("delta").mode("overwrite").save(str(OUTPUT_DIR / "silver" / name))

        # GOLD
        enriched = build_sales_enriched(orders, customers, products)
        daily, product, customer = build_gold_tables(enriched)
        for name, df in [
            ("daily_sales", daily), ("product_sales", product), ("customer_sales", customer)
        ]:
            df.write.format("delta").mode("overwrite").save(str(OUTPUT_DIR / "gold" / name))

        # QUALITY
        summary = get_quality_summary(customers, products, orders)
        orphans = find_orphan_orders(orders, customers, products)
        summary.write.format("delta").mode("overwrite").save(str(OUTPUT_DIR / "quality" / "summary"))
        orphans.write.format("delta").mode("overwrite").save(str(OUTPUT_DIR / "quality" / "orphan_orders"))

        print("Pipeline completed. Output directory:", OUTPUT_DIR)
        print("Daily sales:")
        daily.show(truncate=False)
        print("Orphan orders:")
        orphans.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
