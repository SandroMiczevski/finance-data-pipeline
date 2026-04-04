# BigQuery Finance Data Pipeline

This directory contains scripts to create and manage BigQuery tables for financial data analysis. The pipeline transforms CSV files from a GCS bucket into structured BigQuery tables with advanced analytics.

## Overview

The pipeline consists of three main stages:

1. **External Tables** - Point to CSV files in GCS (read-only views)
2. **Managed Tables** - Permanent BigQuery tables with cleaned data
3. **Transformation Tables** - Analytics tables with derived metrics and trends

## Files

- `01_create_external_tables.sql` - Creates external tables from GCS CSV files
- `02_create_managed_tables.sql` - Creates managed tables from external tables
- `03_data_transformations.sql` - Creates analytics and transformation tables
- `bigquery_orchestrator.py` - Python script to orchestrate the entire pipeline
- `README.md` - This file

## Prerequisites

### GCP Setup
1. GCP Project with BigQuery and GCS enabled
2. Service account with permissions:
   - `bigquery.datasets.create`
   - `bigquery.tables.create`
   - `bigquery.tables.delete`
   - `bigquery.jobs.create`
   - Storage Object Viewer for GCS bucket

3. CSV files uploaded to GCS bucket:
   - `balance_sheet.csv`
   - `income_statement.csv`
   - `company_info.csv`
   - `cash_flow_statement.csv`

### Environment Setup
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install google-cloud-bigquery python-dotenv
```

### Environment Variables

Create a `.env` file in the `bigquery_scripts` directory:

```env
GOOGLE_CLOUD_PROJECT=your-project-id
GCP_GCS_BUCKET=your-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json
```

Or set them as environment variables:
```bash
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GCP_GCS_BUCKET="your-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"
```

## Usage

### 1. Run the Complete Pipeline

```bash
python bigquery_orchestrator.py \
    --project-id your-project-id \
    --dataset-id finance_data \
    --bucket-name your-bucket-name
```

Or using environment variables:
```bash
python bigquery_orchestrator.py
```

### 2. List Created Tables

```bash
python bigquery_orchestrator.py --list-tables
```

### 3. Get Information About a Specific Table

```bash
python bigquery_orchestrator.py --table-info balance_sheet
```

## Tables Created

### Source Tables (External)
- `balance_sheet_external` - External table for balance sheet data
- `income_statement_external` - External table for income statement data
- `company_info_external` - External table for company information
- `cash_flow_statement_external` - External table for cash flow data

### Managed Tables
- `balance_sheet` - Cleaned balance sheet data
- `income_statement` - Cleaned income statement data
- `company_info` - Cleaned company information
- `cash_flow_statement` - Cleaned cash flow data

### Analytics Tables
- `financial_ratios` - Calculated financial metrics (profitability, liquidity, leverage, efficiency)
- `balance_sheet_trends` - Balance sheet changes and growth rates
- `income_statement_trends` - Income statement changes and growth rates
- `company_snapshot_latest` - Latest quarter snapshot for all companies
- `yoy_comparison` - Year-over-year comparisons

## SQL Queries

Each SQL file contains multiple statements that can also be executed individually:

### External Tables
```bash
bq query --use_legacy_sql=false < 01_create_external_tables.sql
```

### Managed Tables
```bash
bq query --use_legacy_sql=false < 02_create_managed_tables.sql
```

### Transformations
```bash
bq query --use_legacy_sql=false < 03_data_transformations.sql
```

## Customization

### Change Dataset Name
```bash
python bigquery_orchestrator.py --dataset-id my_dataset
```

### Modify SQL Scripts
Replace placeholders in SQL files:
- `{project_id}` - Your GCP project ID
- `{dataset_id}` - Your BigQuery dataset ID
- `{bucket_name}` - Your GCS bucket name

## Sample Queries

### Get Latest Financial Metrics
```sql
SELECT
  ticker,
  latest_report_date,
  Total_Revenue,
  Net_Income,
  net_profit_margin_pct,
  current_ratio,
  debt_to_equity
FROM `project.finance_data.company_snapshot_latest`
ORDER BY ticker;
```

### Revenue Growth Trends
```sql
SELECT
  ticker,
  report_date,
  Total_Revenue,
  revenue_growth_pct,
  gross_profit_margin_pct,
  operating_margin_pct
FROM `project.finance_data.income_statement_trends`
ORDER BY ticker, report_date DESC;
```

### Financial Ratios Analysis
```sql
SELECT
  ticker,
  report_date,
  profit_margin,
  roa,
  roe,
  current_ratio,
  debt_to_equity,
  asset_turnover
FROM `project.finance_data.financial_ratios`
ORDER BY ticker, report_date DESC;
```

### Year-over-Year Comparison
```sql
SELECT
  ticker,
  current_date,
  revenue_yoy_growth_pct,
  net_income_yoy_growth_pct,
  eps_yoy_growth_pct
FROM `project.finance_data.yoy_comparison`
WHERE current_date >= '2023-01-01'
ORDER BY ticker, current_date DESC;
```

## Troubleshooting

### Permission Denied Error
Ensure your service account has the required roles:
- `roles/bigquery.admin` or specific BigQuery permissions
- `roles/storage.objectViewer` for GCS bucket access

### Table Not Found
Verify that:
1. The GCS bucket exists and contains the CSV files
2. File paths in SQL scripts match actual file names
3. Dataset exists in BigQuery

### Query Timeout
For large datasets, increase the timeout or partition queries:
```python
job_config = bigquery.QueryJobConfig()
job_config.priority = bigquery.QueryPriority.INTERACTIVE
job_config.use_query_cache = True
```

## Data Refresh

To refresh data from updated CSV files:

```bash
# Clear existing managed tables
bq rm -d -r -f finance_data

# Re-run the pipeline
python bigquery_orchestrator.py
```

## Performance Optimization

### For Large Datasets
1. **Partition tables** by date:
   ```sql
   CREATE OR REPLACE TABLE `project.dataset.balance_sheet`
   PARTITION BY report_date
   AS SELECT ...
   ```

2. **Cluster tables** by ticker:
   ```sql
   CREATE OR REPLACE TABLE `project.dataset.balance_sheet`
   PARTITION BY report_date
   CLUSTER BY ticker
   AS SELECT ...
   ```

3. **Create materialized views** for frequently accessed queries:
   ```sql
   CREATE MATERIALIZED VIEW financial_ratios_mv AS
   SELECT * FROM financial_ratios
   WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);
   ```

## Cost Optimization

- External tables don't store data (reduce storage costs)
- Use clustering and partitioning to reduce query scans
- Set up BigQuery slots for predictable pricing
- Use scheduled queries for periodic updates

## Next Steps

1. **Automate Updates**: Schedule regular pipeline runs using Cloud Scheduler
2. **Add Visualizations**: Connect Power BI or Looker Studio
3. **Create Reports**: Build custom dashboards for stakeholders
4. **Add Validation**: Implement data quality checks
5. **Expand Analysis**: Add more transformation tables for specific use cases

## Support

For issues or questions:
1. Check GCP logs in Cloud Logging
2. Review BigQuery job history
3. Validate CSV file formats
4. Check service account permissions
