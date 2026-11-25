from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def get_quality_summary(
    customers: DataFrame, products: DataFrame, orders: DataFrame
) -> DataFrame:
    """Return simple table-level counts for a quality report."""
    rows = [
        ("customers", customers.count(),
         customers.filter(F.col("customer_id").isNull()).count()),
        ("products", products.count(),
         products.filter(F.col("product_id").isNull()).count()),
        ("orders", orders.count(),
         orders.filter(F.col("order_id").isNull()).count()),
    ]
    spark = customers.sparkSession
    return spark.createDataFrame(
        rows, ["table_name", "record_count", "null_business_key_count"]
    )


def find_orphan_orders(
    orders: DataFrame, customers: DataFrame, products: DataFrame
) -> DataFrame:
    """Find orders whose customer or product key is not present in dimensions."""
    customer_keys = customers.select("customer_id").distinct()
    product_keys = products.select("product_id").distinct()

    return (
        orders.join(customer_keys, on="customer_id", how="left_anti")
        .select("order_id", "customer_id", "product_id", "order_date")
        .withColumn("issue", F.lit("Missing customer reference"))
        .unionByName(
            orders.join(product_keys, on="product_id", how="left_anti")
            .select("order_id", "customer_id", "product_id", "order_date")
            .withColumn("issue", F.lit("Missing product reference"))
        )
    )
