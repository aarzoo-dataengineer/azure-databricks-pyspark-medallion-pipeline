<<<<<<< HEAD
# Retail Sales Data Engineering Pipeline — PySpark & Delta Lake

An end-to-end portfolio project demonstrating a lakehouse-style retail analytics pipeline using **PySpark**, **Delta Lake**, and the **Bronze / Silver / Gold medallion architecture**.

> **Important:** This repository is designed to run locally with Apache Spark (and Delta Lake) or in an Azure Databricks workspace. It does not require an Azure subscription to review the code or run the local version. No Azure deployment is included or claimed.

## Business scenario

A retail business receives customer, product, and order records. The pipeline:
1. Ingests source CSV files into a Bronze Delta layer.
2. Cleans, standardizes, validates, and deduplicates data into Silver.
3. Builds business-ready sales metrics in Gold.
4. Runs data-quality checks and produces an exception report.

## Architecture

```text
CSV source files
  data/customers.csv
  data/products.csv
  data/orders.csv
          |
          v
Bronze (raw Delta tables)
  customers / products / orders
          |
          v
Silver (cleaned Delta tables)
  customers / products / orders
          |
          v
Gold (analytics Delta tables)
  daily_sales / product_sales / customer_sales
          |
          v
SQL analysis / BI tools
```

## Repository layout

```text
.
├── data/                         # Small sample input datasets
├── notebooks/                    # Numbered pipeline entry points
├── src/                          # Reusable PySpark transformations
├── sql/                          # Example analytical SQL
├── tests/                        # Unit tests for transformation logic
├── docs/                         # Architecture and run guide
├── scripts/                      # Local runner
├── requirements.txt
└── README.md
```

## Requirements

- Python 3.10 or 3.11
- Java 8, 11, or 17 compatible with the selected Spark version
- Git (optional, for version control)
- For local execution: `pyspark` and `delta-spark` from `requirements.txt`

Azure Databricks is optional. The scripts use local paths by default. In Databricks, change the configured base path to a Unity Catalog Volume or an approved cloud storage location.

## Run locally

### 1. Create and activate a virtual environment

**Windows PowerShell**
```powershell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
```

**macOS / Linux**
```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
python scripts/run_pipeline.py
```

By default, outputs are written under `output/`:
- `output/bronze/`
- `output/silver/`
- `output/gold/`
- `output/quality/`

The first run may download Spark/Delta dependencies and requires a compatible Java installation. If Java/Spark setup is not available on your computer, you can still upload and review the project on GitHub without executing it.

## Run in Azure Databricks (optional)

1. Create or use an Azure Databricks workspace and compute.
2. Upload the `data/` folder to a Unity Catalog Volume or cloud storage location accessible to the workspace.
3. Import the Python files from `src/` and `notebooks/`, or clone this repository using Databricks Git folders.
4. Update the input and output paths in `notebooks/` for your workspace.
5. Run the notebooks in order: Bronze → Silver → Gold → Data Quality.

This project does not provision Azure resources, configure credentials, or connect to a real Azure storage account.

## Data model

- `customers`: customer_id, customer_name, email, city, state, signup_date
- `products`: product_id, product_name, category, unit_price
- `orders`: order_id, customer_id, product_id, order_date, quantity, order_status

The sample data is synthetic and created for demonstration.

## Engineering practices demonstrated

- Explicit schemas instead of relying on schema inference
- Type casting and standardization
- Null and invalid-record handling
- Duplicate removal using business keys
- Referential-integrity checks
- Joins and aggregations
- Delta Lake writes
- Idempotent overwrite for reproducible demo runs
- Basic unit tests

## Limitations and next improvements

This is a portfolio/demo pipeline, not a production deployment. For production, add incremental ingestion with watermarks, Delta `MERGE` upserts, schema-evolution policy, secrets management, orchestration, monitoring, access controls, CI/CD, and environment-specific configuration.

## GitHub publishing

1. Create a free account at https://github.com/.
2. Create a **Public** repository named `azure-databricks-pyspark-medallion-pipeline`.
3. On the repository page, choose **Add file → Upload files**.
4. Extract this ZIP on your computer and drag the project files/folders into the upload area.
5. Commit the upload with message: `Initial commit: retail PySpark medallion pipeline`.
6. Your repository URL will be:
   `https://github.com/YOUR-GITHUB-USERNAME/azure-databricks-pyspark-medallion-pipeline`

Replace `YOUR-GITHUB-USERNAME` with your actual GitHub username. Add that URL to your resume only after the repository is public and accessible.

## Suggested resume description

**Retail Sales Data Engineering Pipeline | PySpark, Delta Lake, SQL**
- Built a sample end-to-end data pipeline using PySpark and the Bronze, Silver, and Gold medallion architecture.
- Implemented data cleansing, deduplication, schema enforcement, validation, joins, and sales aggregations.
- Stored processed datasets in Delta format and prepared analytical outputs for downstream reporting.

Only describe work you have personally reviewed and can explain in an interview.
=======
# azure-databricks-pyspark-medallion-pipeline
End-to-End Data Engineering using PySpark and Delta Lake
>>>>>>> 03a5ac326aa1fe5fb26f71eaea94fb936f747b27
