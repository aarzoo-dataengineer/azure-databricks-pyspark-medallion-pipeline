# Architecture and design notes

## Medallion layers

### Bronze
Stores source-shaped records in Delta format and adds ingestion metadata (`_source_file`, `_ingested_at`). In this demo, each run overwrites the layer to keep the sample reproducible.

### Silver
Applies standardization, data type rules from explicit schemas, duplicate removal, and basic business validity checks. Orders with non-positive quantities and products with non-positive prices are excluded.

### Gold
Joins completed orders to valid customers and products, calculates `sales_amount`, and creates daily, product, and customer aggregates.

## Data quality
The pipeline produces a small table-level summary and an orphan-order report. Orders referencing unknown customers or products are not included in the enriched Gold dataset because the joins are inner joins.

## Production extensions
- Use Auto Loader or a controlled batch ingestion pattern.
- Track watermarks and process only new/changed records.
- Use Delta `MERGE INTO` for idempotent upserts.
- Quarantine invalid rows instead of silently dropping them.
- Add expectations, metrics, alerts, and job retries.
- Store secrets in a managed secret scope; never commit credentials.
- Use Unity Catalog for governance and access control.
