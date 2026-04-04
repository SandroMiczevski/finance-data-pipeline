# 📊 BigQuery Finance Data Pipeline - Complete Package

## 🎯 What Was Created

I've created a complete, production-ready BigQuery pipeline to transform your CSV financial data into analytics-ready tables. Here's what you get:

### Complete File List

```
bigquery_scripts/
├── 📋 SQL Scripts
│   ├── 01_create_external_tables.sql      # External tables from GCS
│   ├── 02_create_managed_tables.sql        # Permanent managed tables
│   ├── 03_data_transformations.sql         # Analytics & transformation tables
│   └── 04_sample_queries.sql               # 15+ pre-built analysis queries
│
├── 🐍 Python Scripts
│   ├── bigquery_orchestrator.py            # Main orchestration script
│   ├── validate_setup.py                   # Pre-flight validation checker
│   ├── requirements.txt                    # Python dependencies
│   └── .env.example                        # Environment template
│
├── 🔧 Utility Scripts
│   ├── run_pipeline.sh                     # Simple pipeline runner
│   └── deploy.sh                           # Setup & deployment tools
│
└── 📖 Documentation
    ├── README.md                           # Full documentation
    ├── QUICKSTART.md                       # 5-minute quick start
    └── ARCHITECTURE.md                     # This file
```

## 🏗️ Architecture Overview

### 3-Tier Data Pipeline

```
                GCS Bucket (Your CSV Files)
                         ↓
        [External Tables] ← Read-only, no storage cost
                         ↓
        [Managed Tables] ← Permanent, indexed, queryable
                         ↓
        [Analytics Tables] ← Derived metrics, pre-calculated
```

### Tables Created

**Layer 1: External Tables (4 tables)**
- `balance_sheet_external`
- `income_statement_external`
- `company_info_external`
- `cash_flow_statement_external`

**Layer 2: Managed Tables (4 tables)**
- `balance_sheet` - Clean, typed balance sheet data
- `income_statement` - Clean income statement data
- `company_info` - Company information with metrics
- `cash_flow_statement` - Cash flow data

**Layer 3: Analytics Tables (5 tables)**
- `financial_ratios` - 15+ calculated financial metrics
- `balance_sheet_trends` - Growth rates and changes
- `income_statement_trends` - Margin analysis and growth
- `company_snapshot_latest` - Latest quarter snapshot
- `yoy_comparison` - Year-over-year analysis

## 📊 Metrics Calculated

### Financial Ratios (15+ metrics)
- **Profitability**: Profit Margin, EBITDA Margin, Operating Margin, ROA, ROE
- **Liquidity**: Current Ratio, Quick Ratio, Cash Ratio
- **Leverage**: Debt-to-Assets, Debt-to-Equity, Interest Coverage
- **Efficiency**: Asset Turnover, Equity Multiplier
- **Cost Structure**: COGS Ratio, OpEx Ratio, R&D %, SG&A %

### Trends & Growth
- Revenue growth (YoY & growth percentage)
- Margin evolution (gross, operating, net)
- Balance sheet changes
- EPS trends
- Cash position tracking

## 🚀 Quick Start

### 1. Setup (3 commands)
```bash
cd bigquery_scripts

# Setup environment
bash deploy.sh setup

# Verify everything is configured
python3 validate_setup.py
```

### 2. Run Pipeline (1 command)
```bash
# Option A - Simple
bash run_pipeline.sh

# Option B - Direct
python3 bigquery_orchestrator.py \
  --project-id YOUR_PROJECT \
  --dataset-id finance_data \
  --bucket-name YOUR_BUCKET
```

### 3. Query Your Data
```bash
# List all tables
python3 bigquery_orchestrator.py --list-tables

# Get table info
python3 bigquery_orchestrator.py --table-info balance_sheet
```

## 🛠️ Configuration

### Environment Setup

Create `.env` file (or use existing):
```bash
cp .env.example .env
# Edit with your values
```

Required:
- `GOOGLE_CLOUD_PROJECT` - Your GCP project ID
- `GCP_GCS_BUCKET` - Your GCS bucket name

Optional:
- `GOOGLE_APPLICATION_CREDENTIALS` - Path to service account key
- `BIGQUERY_DATASET_ID` - Default: `finance_data`

### GCP Authentication
Use one of:
```bash
# Option 1: Application Default Credentials
gcloud auth application-default login

# Option 2: Service Account Key
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

## 📈 Usage Examples

### Revenue & Growth Analysis
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

### Financial Health Snapshot
```sql
SELECT
  ticker,
  latest_report_date,
  Total_Revenue,
  Net_Income,
  net_profit_margin_pct,
  current_ratio,
  debt_to_equity,
  roe
FROM `project.finance_data.company_snapshot_latest`;
```

### Peer Comparison
```sql
SELECT
  ticker,
  Total_Revenue,
  Net_Income,
  EBITDA,
  net_profit_margin_pct,
  ROW_NUMBER() OVER (ORDER BY Total_Revenue DESC) AS rank
FROM `project.finance_data.company_snapshot_latest`
ORDER BY Total_Revenue DESC;
```

### Year-over-Year Growth
```sql
SELECT
  ticker,
  current_date,
  revenue_yoy_growth_pct,
  net_income_yoy_growth_pct,
  eps_yoy_growth_pct
FROM `project.finance_data.yoy_comparison`
WHERE current_date >= '2023-01-01'
ORDER BY ticker DESC;
```

## 🔐 Security & Permissions

### Required GCP Permissions
- `bigquery.datasets.create`
- `bigquery.tables.create`
- `bigquery.jobs.create`
- `storage.objects.get` (for GCS access)

### Best Practices
1. Use service accounts (not user accounts)
2. Enable BQ audit logging
3. Review GCS bucket ACLs
4. Keep credentials secure (.env not committed)
5. Use Cloud Secret Manager for prod

## 💰 Cost Optimization

### Strategies
1. **External Tables** - Only pay for queries (no storage)
2. **Partitioning** - Reduce scan costs by 90%+
3. **Clustering** - Optimize repeated queries
4. **Query Cache** - Use cached results (free)
5. **Scheduled Queries** - Batch processing at off-hours

### Example Savings
| Approach | Cost |
|----------|------|
| No optimization | $100/month |
| With partitioning | $15/month |
| With clustering | $5/month |
| External tables only | $1/month |

## 🔄 Data Refresh

### Manual Refresh
```bash
# Re-run the pipeline
python3 bigquery_orchestrator.py

# Or refresh specific tables
bash run_pipeline.sh
```

### Scheduled Refresh
```bash
# Create Cloud Scheduler job (daily at 2 AM)
gcloud scheduler jobs create http bigquery-pipeline \
  --location=us-central1 \
  --schedule="0 2 * * *" \
  --http-method=POST \
  --uri="YOUR_CLOUD_FUNCTION_URL"
```

## 📊 Visualization Ready

After pipeline completes, connect to your tools:
- **Google Looker Studio** - Free, built-in integration
- **Power BI** - Connector available
- **Tableau** - BigQuery connector
- **Custom Scripts** - Use BQ Python client

## 🧪 Validation & Testing

### Pre-flight Check
```bash
python3 validate_setup.py
```

Checks:
- ✓ Environment variables
- ✓ GCP credentials
- ✓ BigQuery access
- ✓ GCS access
- ✓ CSV files exist
- ✓ SQL scripts present
- ✓ Python dependencies

### Query Validation
```bash
# Test external tables
bq query --nouse_legacy_sql 'SELECT COUNT(*) FROM project.dataset.balance_sheet_external'

# Test managed tables
bq query --nouse_legacy_sql 'SELECT COUNT(*) FROM project.dataset.balance_sheet'

# Test analytics
bq query --nouse_legacy_sql 'SELECT * FROM project.dataset.financial_ratios LIMIT 10'
```

## 📚 File Descriptions

### SQL Files

**01_create_external_tables.sql**
- Creates 4 external tables
- Points to CSV files in GCS
- Handles CSV parsing (skip rows, allow jagged rows, etc.)
- ~50 lines

**02_create_managed_tables.sql**
- Converts external to managed tables
- Applies type casting and transformations
- Adds metadata columns (loaded_at)
- ~300 lines

**03_data_transformations.sql**
- Creates 5 analytics tables
- Calculates 50+ financial metrics
- Includes window functions for trends
- Implements YoY comparisons
- ~400 lines

**04_sample_queries.sql**
- 15 pre-built analysis queries
- Copy-paste ready examples
- Common financial analysis tasks
- Performance optimized

### Python Files

**bigquery_orchestrator.py**
- Main orchestration script
- Handles all pipeline steps
- Error handling & logging
- CLI with multiple modes
- ~350 lines

**validate_setup.py**
- Pre-flight validation checker
- Verifies all prerequisites
- Detailed error reporting
- Colored output
- ~300 lines

### Shell Scripts

**run_pipeline.sh**
- Simplified wrapper script
- Handles environment setup
- Single command execution
- Progress feedback

**deploy.sh**
- Advanced deployment options
- Setup, verify, validate modes
- GCS file validation
- Full diagnostics

## 🎓 Learning Resources

### Concepts
- [BigQuery External Tables](https://cloud.google.com/bigquery/docs/external-data-cloud-storage)
- [Financial Ratios Analysis](https://www.investopedia.com/financial-ratios-4689817)
- [Data Warehouse Design](https://cloud.google.com/bigquery/docs/best-practices-perf-overview)

### Tutorials
- [BigQuery Tutorial](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-web-ui)
- [GCS Setup](https://cloud.google.com/storage/docs/quickstart-console)
- [IAM & Security](https://cloud.google.com/bigquery/docs/access-control)

## ❓ FAQ

**Q: How much does this cost?**
A: Minimal. External tables = $0.25 per TB scanned. Managed + analytics = storage only ($0.02/GB/month).

**Q: Can I modify the SQL?**
A: Absolutely! All SQL files are editable.

**Q: How often should I refresh?**
A: As often as your CSV files update. Daily, weekly, or on-demand.

**Q: Can I add more data?**
A: Yes. Add CSVs to GCS and re-run (or create new external tables).

**Q: What about data privacy?**
A: All data stays in your GCP project. Implement IAM controls as needed.

## 🚧 Advanced Customizations

### Add Partitioning
```sql
CREATE OR REPLACE TABLE `project.dataset.balance_sheet`
PARTITION BY report_date
CLUSTER BY ticker AS
SELECT * FROM balance_sheet;
```

### Create Materialized Views
```sql
CREATE MATERIALIZED VIEW `project.dataset.financial_ratios_mv` AS
SELECT * FROM financial_ratios
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR);
```

### Add Data Validation
```sql
CREATE OR REPLACE TABLE `project.dataset.data_quality_checks` AS
SELECT
  CURRENT_TIMESTAMP() AS check_time,
  'balance_sheet' AS table_name,
  COUNT(*) AS row_count,
  COUNT(DISTINCT ticker) AS unique_tickers,
  MIN(report_date) AS earliest_date,
  MAX(report_date) AS latest_date
FROM balance_sheet;
```

## 🆘 Troubleshooting

### "Permission Denied"
```bash
# Check service account roles
gcloud projects get-iam-policy $GOOGLE_CLOUD_PROJECT

# Add missing role
gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
  --member=serviceAccount:SERVICE_ACCOUNT_EMAIL \
  --role=roles/bigquery.admin
```

### "File Not Found in GCS"
```bash
# List files
gsutil ls gs://$GCP_GCS_BUCKET/

# Upload CSV files
gsutil -m cp raw_data/CSV/*.csv gs://$GCP_GCS_BUCKET/
```

### "Query timeout"
```bash
# Use external table for large queries
# Or add LIMIT clause
# Or partition data
```

## 📞 Support

For issues:
1. Check the QUICKSTART.md
2. Run `python3 validate_setup.py`
3. Review logs in Cloud Logging
4. Check BigQuery job history

## 🎉 Summary

You now have:
- ✅ 13 BigQuery tables (4 external + 4 managed + 5 analytics)
- ✅ 50+ pre-calculated financial metrics
- ✅ Automated orchestration pipeline
- ✅ Complete validation tooling
- ✅ 15+ ready-to-use SQL queries
- ✅ Production-ready code
- ✅ Full documentation

**Next step: Run `python3 validate_setup.py` then `bash run_pipeline.sh`**

Happy analyzing! 📊
