from pathlib import Path

# Project root is the parent of src/.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

CUSTOMERS_PATH = str(DATA_DIR / "customers.csv")
PRODUCTS_PATH = str(DATA_DIR / "products.csv")
ORDERS_PATH = str(DATA_DIR / "orders.csv")
