from pyspark.sql import SparkSession
from src.transformations import clean_customers, clean_products, clean_orders


def test_clean_customers_deduplicates(spark):
    rows = [
        ("C1", " Alice ", "ALICE@EXAMPLE.COM", "pune", "maharashtra", None),
        ("C1", "Alice", "alice@example.com", "Pune", "Maharashtra", None),
    ]
    columns = ["customer_id", "customer_name", "email", "city", "state", "signup_date"]
    result = clean_customers(spark.createDataFrame(rows, columns))
    assert result.count() == 1
    assert result.first()["email"] == "alice@example.com"


def test_clean_products_removes_non_positive_price(spark):
    rows = [("P1", "Mouse", "electronics", 100.0), ("P2", "Bad", "other", -1.0)]
    result = clean_products(spark.createDataFrame(rows, ["product_id", "product_name", "category", "unit_price"]))
    assert result.count() == 1


def test_clean_orders_removes_non_positive_quantity(spark):
    rows = [("O1", "C1", "P1", None, 2, "completed"), ("O2", "C1", "P1", None, -1, "completed")]
    result = clean_orders(spark.createDataFrame(rows, ["order_id", "customer_id", "product_id", "order_date", "quantity", "order_status"]))
    assert result.count() == 1


# pytest fixture
import pytest


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("transformation-tests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
