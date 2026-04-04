# BigQuery Scripts - Quick Start Guide

## 📋 Overview

This directory contains everything needed to transform your CSV files in GCS into BigQuery tables with financial analytics. The pipeline includes:

- **01_create_external_tables.sql** - Read-only views of CSV files in GCS
- **02_create_managed_tables.sql** - Permanent tables with clean data
- **03_data_transformations.sql** - Analytics tables with financial metrics
- **04_sample_queries.sql** - 15+ pre-built analysis queries
- **bigquery_orchestrator.py** - Automated pipeline orchestration
- **run_pipeline.sh** - Simplified shell wrapper
- **deploy.sh** - Setup and deployment utilities

## ⚡ Quick Start (5 minutes)

### 1. Prerequisites Setup

```bash
# Set your GCP credentials
export GOOGLE_CLOUD_PROJECT="your-project-id"
export GCP_GCS_BUCKET="your-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

# Or use ADC (Application Default Credentials)
gcloud auth application-default login
```

### 2. Setup Environment

```bash
cd bigquery_scripts

# Option A: Use the deployment script
bash deploy.sh setup

# Option B: Manual setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Verify Configuration

```bash
# Check all prerequisites
bash deploy.sh verify
```

### 4. Run the Pipeline

```bash
# Option A: Simple shell script
bash run_pipeline.sh

# Option B: Direct Python
python3 bigquery_orchestrator.py

# Option C: With custom settings
python3 bigquery_orchestrator.py \
  --project-id my-project \
  --dataset-id my_finance_data \
  --bucket-name my-bucket
```

## 📊 What Gets Created

### Tables by Type

**Source Tables** (External - 4 tables)
- Read data directly from GCS CSVs
- No storage cost
- Great for initial exploration

**Managed Tables** (Permanent - 4 tables)
- `balance_sheet` - Balance sheet data with metadata
- `income_statement` - Income statement with timestamps
- `company_info` - Company information and details
- `cash_flow_statement` - Cash flow data

**Analytics Tables** (Derived - 5 tables)
- `financial_ratios` - 15+ calculated ratios
- `balance_sheet_trends` - Growth rates and changes
- `income_statement_trends` - Margin evolution
- `company_snapshot_latest` - Latest quarter metrics
- `yoy_comparison` - Year-over-year analysis

## 🔍 Data Discovery

### List Created Tables
```bash
python3 bigquery_orchestrator.py --list-tables
```

### Get Table Information
```bash
python3 bigquery_orchestrator.py --table-info balance_sheet
```

### Query Sample Data
```sql
SELECT
  ticker,
  report_date,
  Total_Revenue,
  Net_Income,
  Diluted_EPS
FROM `project.finance_data.company_snapshot_latest`
ORDER BY Total_Revenue DESC;
```

## 📈 Analytics Examples

### 1. Revenue Growth Trends
```sql
SELECT
  ticker,
  report_date,
  Total_Revenue,
  revenue_growth_pct,
  gross_profit_margin_pct
FROM `project.finance_data.income_statement_trends`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
ORDER BY ticker, report_date DESC;
```

### 2. Financial Health Score
```sql
SELECT
  ticker,
  report_date,
  current_ratio,
  debt_to_equity,
  roe,
  profit_margin
FROM `project.finance_data.financial_ratios`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
ORDER BY ticker DESC;
```

### 3. Peer Comparison
```sql
SELECT
  ticker,
  Total_Revenue,
  Net_Income,
  gutter_margin_pct,
  ROW_NUMBER() OVER (ORDER BY Total_Revenue DESC) AS revenue_rank
FROM `project.finance_data.company_snapshot_latest`;
```

## 🛠️ Troubleshooting

### Problem: "Permission Denied" Error

**Solution:**
```bash
# Verify service account has these roles:
# - BigQuery Admin (or specific permissions)
# - Storage Object Viewer

gcloud projects get-iam-policy $GOOGLE_CLOUD_PROJECT \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount@*"
```

### Problem: "CSV File Not Found"

**Solution:**
```bash
# Verify files are in GCS
gsutil ls gs://$GCP_GCS_BUCKET/

# Upload CSV files if missing
gsutil -m cp raw_data/CSV/*.csv gs://$GCP_GCS_BUCKET/
```

### Problem: External Table Returns Empty Results

**Solution:**
```bash
# Check external table configuration
bq show --format=prettyjson \
  project:dataset.balance_sheet_external

# Verify CSV format by sampling
gsutil cat gs://$GCP_GCS_BUCKET/balance_sheet.csv | head -5
```

## 📚 Core Concepts

### Why 3 Layers?

1. **External Tables** 
   - Minimal cost (pay only for queries)
   - Direct source-of-truth connection
   - Read-only
   
2. **Managed Tables**
   - Data consistency
   - Better performance
   - Transformation foundation
   
3. **Analytics Tables**
   - Pre-calculated metrics
   - Fast queries
   - Business-ready metrics

### Financial Ratios Included

**Profitability**
- Profit Margin, EBITDA Margin, Operating Margin
- ROA (Return on Assets), ROE (Return on Equity)

**Liquidity**
- Current Ratio, Quick Ratio, Cash Ratio

**Leverage**
- Debt-to-Assets, Debt-to-Equity
- Interest Coverage Ratio

**Efficiency**
- Asset Turnover, Equity Multiplier

**Cost Structure**
- COGS Ratio, Operating Expense Ratio
- R&D and SG&A Ratios

## 🚀 Advanced Usage

### Schedule Regular Updates

```bash
# Create a Cloud Scheduler job
gcloud scheduler jobs create app-engine bigquery-pipeline \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --http-method=POST \
  --uri="https://region-project.cloudfunctions.net/bigquery-pipeline"
```

### Create Partitioned Tables

```sql
CREATE OR REPLACE TABLE `project.dataset.balance_sheet`
PARTITION BY report_date
CLUSTER BY ticker AS
SELECT * FROM balance_sheet;
```

### Setup Data Refresh

```bash
# Clear and refresh tables (careful - deletes data!)
bq rm -d -r -f finance_data
python3 bigquery_orchestrator.py
```

## 📝 SQL File Reference

### 01_create_external_tables.sql
- Creates 4 external tables
- Points to CSV files in GCS
- Handles CSV parsing options

### 02_create_managed_tables.sql
- Creates permanent managed tables
- Applies type casting
- Adds metadata columns (loaded_at)

### 03_data_transformations.sql
- 5 analytics tables
- 50+ calculated metrics
- Window functions for trends

### 04_sample_queries.sql
- 15 pre-built queries
- Common financial analysis
- Copy-paste ready

## 🔐 Security Best Practices

1. **Use Service Accounts**
   ```bash
   gcloud iam service-accounts create bigquery-pipeline
   ```

2. **Limit Permissions**
   - Only grant needed roles
   - Use custom roles if available

3. **Secure Credentials**
   - Store keys in Secret Manager
   - Use workload identity in GKE
   - Never commit .env to Git

4. **Audit Access**
   - Enable BigQuery audit logs
   - Review GCS bucket access logs

## 💰 Cost Optimization

### Reduce Query Costs
- Use external tables for one-time queries
- Partition tables by date
- Cluster by frequently filtered columns
- Set query cache to true

### Example Cost-Optimized Query
```sql
-- This only scans relevant data
SELECT *
FROM `project.dataset.balance_sheet`
WHERE report_date >= '2023-01-01'  -- Partition pruning
  AND ticker = 'AAPL'              -- Cluster pruning
```

## 📞 Support & Resources

- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Python Client Library](https://cloud.google.com/python/docs/reference/bigquery/latest)
- [GCS Documentation](https://cloud.google.com/storage/docs)

## 🎯 Next Steps

1. ✅ Run the pipeline
2. 📊 Explore the data
3. 🔍 Run sample queries
4. 📈 Build custom reports
5. 📅 Schedule regular updates
6. 🎨 Create visualizations (Power BI/Looker)

## ❓ FAQ

**Q: Can I use this with different CSV formats?**
A: Yes, modify the SQL scripts to handle your column names and data types.

**Q: How often should I refresh the data?**
A: Depends on your needs. Daily or weekly via Cloud Scheduler.

**Q: Can I add more companies to the analysis?**
A: Yes, upload their CSVs to GCS and re-run the pipeline.

**Q: What's the cost for running this pipeline?**
A: Costs depend on data size and query volume. External tables are very cheap as you only pay for query scans.

---

**Happy analyzing! 📊**
