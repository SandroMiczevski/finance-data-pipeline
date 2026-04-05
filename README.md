# Data Ingestion Pipeline

This project contains an end to end pipeline for financial market data from companies listed in the US stock market.

This section covers the complete data ingestion workflow, from fetching financial data to querying it in BigQuery.

This project depends on the python package manager `uv`. To install it execute the command:
- `python pip install uv`

After installing it, in the root folder of the project, execute the following command:
- `uv sync`

This should install all dependencies and allow you to execute the project following the explanation bellow.

## 1. Fetching Financial Data with `get_financials`

Downloads US-listed company financial statements using either yfinance or FinancialModelingPrep (FMP) as the data source.

### Basic Usage

```bash
uv run python ingestion_scripts/get_financials.py --tickers AAPL MSFT GOOGL --source yfinance
```

### Available Options

- `--tickers` (required): Ticker symbols (e.g., `AAPL MSFT GOOGL`)
- `--years` (optional): Filter by specific years (e.g., `2021 2022 2023`)
- `--source` (default: `yfinance`): Data source - choose `yfinance` or `fmp`
- `--output` (default: `financials.json`): Output filename
- `--save_dir` (default: `raw_data/JSON/`): Directory to save the JSON file
- `--pretty`: Pretty-print the JSON output

### Examples

Fetch AAPL data using yfinance:
```bash
uv run python ingestion_scripts/get_financials.py --tickers AAPL --source yfinance
```

Fetch multiple companies for specific years:
```bash
uv run python ingestion_scripts/get_financials.py --tickers AAPL MSFT --years 2020 2021 2022 --source yfinance --pretty
```

**Using FinancialModelingPrep (FMP):**

If using FMP as the data source, you must set your API key:
```bash
export FMP_API_KEY="your_fmp_api_key_here"
uv run python ingestion_scripts/get_financials.py --tickers AAPL --source fmp
```

## 2. Converting JSON to CSV with `export_data_to_csv`

Converts the fetched JSON financial data into CSV format for easier processing and uploading to GCS.

### Basic Usage

```bash
uv run python ingestion_scripts/export_data_to_csv.py --source raw_data/JSON/financials.json
```

### Available Options

- `--source` (default: `financials.json`): Path to the source JSON file

### Examples

Convert default financials.json:
```bash
uv run python ingestion_scripts/export_data_to_csv.py
```

Convert a specific JSON file:
```bash
uv run python ingestion_scripts/export_data_to_csv.py --source /workspaces/finance-data-pipeline/raw_data/JSON/financials.json
```

The script generates three CSV files in the same directory as the JSON:
- `balance_sheet.csv`
- `income_statement.csv`
- `cash_flow_statement.csv`

## 3. Setting Up GCP Environment

Before uploading data, you need to configure your GCP credentials and environment variables.

### GCP Setup Requirements

1. **Create a GCP Account and Project**
   - Create an account with your Google email ID
   - Setup a new project in the GCP Console (note down the Project ID)
   
2. **Create a Service Account**
   - Navigate to IAM & Admin → Service Accounts
   - Create a new service account
   - Grant it the `Editor` role (or `BigQuery Admin` + `Storage Admin` for minimal permissions)
   - Create and download a JSON key file

3. **Create GCS Bucket**
   - In the GCP Console, create a new GCS bucket for your data
   - Note the bucket name (e.g., `findata-bucket`)

### Storing Secrets and Environment Variables

**⚠️ IMPORTANT: Choose one method below**

#### Option A: Using `.environment/.env` file (Recommended for Development)

Create the `.environment` directory and `.env` file:

```bash
mkdir -p .environment
touch .environment/.env
```

Edit `.environment/.env` and add:

```
# GCP Project Configuration
GOOGLE_CLOUD_PROJECT=your-gcp-project-id
GCP_GCS_BUCKET=your-gcs-bucket-name

# Path to your service account JSON key
# IMPORTANT: Keep this secret, never commit to version control!
GOOGLE_APPLICATION_CREDENTIALS=./.secrets/gcp/service-account-key.json
```

Create the secrets directory and store your service account key:

```bash
mkdir -p .secrets/gcp
# Place your downloaded service-account-key.json file here
cp /path/to/downloaded/service-account-key.json .secrets/gcp/
```

**Make sure `.secrets/` is in `.gitignore`:**
```bash
echo ".secrets/" >> .gitignore
echo ".environment/" >> .gitignore
```

#### Option B: Using Environment Variables (Recommended for Production)

Set environment variables directly in your shell or deployment environment:

```bash
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export GCP_GCS_BUCKET="your-gcs-bucket-name"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account-key.json"

# Verify authentication
gcloud auth application-default login
```

#### Option C: Using Google Cloud ADC (Application Default Credentials)

If you have `gcloud` CLI installed and authenticated:

```bash
gcloud auth application-default login
```

This stores credentials securely and the scripts will use them automatically.

## 4. Uploading Data to GCP with `upload_to_gcp`

Uploads CSV and JSON files from your local directory to Google Cloud Storage.

### Prerequisites

✅ `.environment/.env` file configured (or environment variables set)  
✅ GCS bucket created in GCP  
✅ Service account credentials available

### Basic Usage

Upload all CSV files:
```bash
uv run python ingestion_scripts/upload_to_gcp.py --upload_csv
```

Upload all JSON files:
```bash
uv run python ingestion_scripts/upload_to_gcp.py --upload_json
```

Verify uploaded files:
```bash
uv run python ingestion_scripts/upload_to_gcp.py --verify
```

Get upload statistics:
```bash
uv run python ingestion_scripts/upload_to_gcp.py --stats
```

### Complete Examples

Upload CSV data and verify the upload:
```bash
uv run python ingestion_scripts/upload_to_gcp.py --upload_csv
uv run python ingestion_scripts/upload_to_gcp.py --verify
```

## Data Flow Diagram

```
         Local Raw Data (JSON/CSV)
                    ↓
         [get_financials] ← Fetch from API
                    ↓
         [export_data_to_csv] ← Convert JSON to CSV
                    ↓
         [upload_to_gcp] ← Upload to GCS Bucket
                    ↓
         GCS Bucket (CSV Files)
                    ↓
         [BigQuery External Tables] ← Read-only, no storage cost
                    ↓
         [BigQuery Managed Tables] ← Permanent, indexed, queryable
                    ↓
         [BigQuery Analytics Tables] ← Derived metrics, pre-calculated
```

## 5. Running BigQuery Pipeline with `bigquery_orchestrator`

Orchestrates the creation of BigQuery tables from GCS files and runs data transformations.

### Prerequisites

✅ `.environment/.env` file configured with:
  - `GOOGLE_CLOUD_PROJECT` = your GCP Project ID
  - `GCP_GCS_BUCKET` = your GCS bucket name
  - `GOOGLE_APPLICATION_CREDENTIALS` = path to service account JSON

✅ CSV files uploaded to your GCS bucket  
✅ BigQuery API enabled in your GCP project

### Basic Usage

Run the complete pipeline (creates external tables, managed tables, and transformations):
```bash
uv run python bigquery_scripts/bigquery_orchestrator.py --project-id your-project-id --bucket-name your-bucket-name
```

### Available Options

- `--project-id` (optional): GCP Project ID (can be set via `GOOGLE_CLOUD_PROJECT` env var)
- `--dataset-id` (default: `finance_data`): BigQuery Dataset ID
- `--bucket-name` (optional): GCS Bucket name (can be set via `GCP_GCS_BUCKET` env var)
- `--list-tables`: List all tables in the dataset (doesn't run pipeline)
- `--table-info` TABLE_NAME: Get information about a specific table

### Complete Examples

**Run full pipeline using environment variables:**
```bash
uv run python bigquery_scripts/bigquery_orchestrator.py
```

**Run full pipeline with explicit parameters:**
```bash
uv run python bigquery_scripts/bigquery_orchestrator.py \
  --project-id my-gcp-project \
  --dataset-id finance_data \
  --bucket-name findata-bucket
```

**List all tables in the dataset:**
```bash
uv run python bigquery_scripts/bigquery_orchestrator.py --list-tables
```

**Get information about a specific table:**
```bash
uv run python bigquery_scripts/bigquery_orchestrator.py --table-info income_statement
```

### What the Orchestrator Does

1. **Ensures Dataset Exists**: Creates the BigQuery dataset if it doesn't exist (location: US)
2. **Creates External Tables**: Creates read-only external tables that reference CSV files in GCS
3. **Creates Managed Tables**: Copies data from external tables into permanent, indexed managed tables
4. **Creates Analytical Tables**: Runs transformations to create cleaned and aggregated datasets

### Complete End-to-End Workflow

```bash
# Step 1: Fetch financial data
uv run python ingestion_scripts/get_financials.py --tickers AAPL MSFT GOOGL --source yfinance

# Step 2: Convert JSON to CSV
uv run python ingestion_scripts/export_data_to_csv.py

# Step 3: Upload CSV files to GCS
uv run python ingestion_scripts/upload_to_gcp.py --upload_csv

# Step 4: Verify the upload
uv run python ingestion_scripts/upload_to_gcp.py --verify

# Step 5: Run BigQuery pipeline
uv run python bigquery_scripts/bigquery_orchestrator.py

# Step 6 (Optional): List all created tables
uv run python bigquery_scripts/bigquery_orchestrator.py --list-tables
```

## Troubleshooting

**"Error: GOOGLE_APPLICATION_CREDENTIALS not found"**
- Set the environment variable pointing to your service account JSON file
- Or ensure `.environment/.env` has the correct path

**"Error: Bucket name not provided"**
- Set `GCP_GCS_BUCKET` in `.environment/.env` or as an environment variable
- Or use `--bucket-name` parameter explicitly

**"Error: Project ID not provided"**
- Set `GOOGLE_CLOUD_PROJECT` in `.environment/.env` or as an environment variable
- Or use `--project-id` parameter explicitly

**"Permission denied" errors**
- Verify your service account has appropriate roles (BigQuery Admin, Storage Admin)
- Check that the service account JSON file has correct permissions

---

Source:
[https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/2_gcp_overview.md]
