Command to run Duckdb CLI

```
uvx --from duckdb-cli duckdb -ui
```
# Downloading financial data and setting up for ingestion


* Running script to convert from JSON to CSV

```
# If you are on Github workspace
uv run python export_data_to_csv.py --source /workspaces/finance-data-pipeline/raw_data/JSON/
```


# Uploading data to GCP and Tranforming it

## Setting up GCP account and project

Initial Setup
1. Create an account with your Google email ID
2. Setup your first project if you haven't already
    * eg. "DTC DE Course", and note down the "Project ID" (we'll use this later when deploying infra with TF)
3. Setup service account & authentication for this project
    * Grant Viewer role to begin with.
    * Download service-account-keys (.json) for auth.
4. Download SDK for local setup
5. Set environment variable to point to your downloaded GCP keys


* Path used for keys: ./.secrets/gcp

```
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"

# Refresh token/session, and verify authentication
gcloud auth application-default login
```

## Running script to upload JSON and CSV files to GCP

```
uv run upload_to_gcp.py --upload_csv (or --upload_json)
```


Source:
[https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/01-docker-terraform/terraform/2_gcp_overview.md]

## Executing pipeline

```
               Locally stored CSV files
                         ↓
                GCS Bucket (CSV Files)
                         ↓
        [External Tables] ← Read-only, no storage cost
                         ↓
        [Managed Tables] ← Permanent, indexed, queryable
                         ↓
        [Analytics Tables] ← Derived metrics, pre-calculated
```

```
uv run python bigquery_orchestrator.py
```
