from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("customer_id", F.trim("customer_id"))
          .withColumn("customer_name", F.trim("customer_name"))
          .withColumn("email", F.lower(F.trim("email")))
          .withColumn("city", F.initcap(F.trim("city")))
          .withColumn("state", F.initcap(F.trim("state")))
          .dropDuplicates(["customer_id"])
          .filter(F.col("customer_id").isNotNull())
    )


def clean_products(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("product_id", F.trim("product_id"))
          .withColumn("product_name", F.trim("product_name"))
          .withColumn("category", F.initcap(F.trim("category")))
          .filter(F.col("product_id").isNotNull())
          .filter(F.col("unit_price") > 0)
          .dropDuplicates(["product_id"])
    )


def clean_orders(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("order_id", F.trim("order_id"))
          .withColumn("customer_id", F.trim("customer_id"))
          .withColumn("product_id", F.trim("product_id"))
          .withColumn("order_status", F.initcap(F.trim("order_status")))
          .filter(F.col("order_id").isNotNull())
          .filter(F.col("quantity") > 0)
          .dropDuplicates(["order_id"])
    )


def build_sales_enriched(
    orders: DataFrame, customers: DataFrame, products: DataFrame
) -> DataFrame:
    """Keep only orders with matching customer and product records."""
    return (
        orders.filter(F.col("order_status") == "Completed")
        .join(customers, on="customer_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn("sales_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    )


def build_gold_tables(enriched: DataFrame):
    daily_sales = (
        enriched.groupBy("order_date")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        )
        .orderBy("order_date")
    )

    product_sales = (
        enriched.groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("units_sold"),
            F.round(F.sum("sales_amount"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    customer_sales = (
        enriched.groupBy("customer_id", "customer_name", "city", "state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("sales_amount"), 2).alias("lifetime_value"),
        )
        .orderBy(F.desc("lifetime_value"))
    )
    return daily_sales, product_sales, customer_sales
