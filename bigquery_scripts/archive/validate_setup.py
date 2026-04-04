#!/usr/bin/env python3
"""
BigQuery Pipeline Validator

Validates BigQuery setup, credentials, and data before running the full pipeline.
"""

import os
import sys
import logging
from pathlib import Path
from typing import Tuple, List
from dotenv import load_dotenv

try:
    from google.cloud import bigquery, storage
    from google.cloud.exceptions import GoogleCloudError, NotFound, PermissionDenied
except ImportError:
    print("Error: google-cloud packages not installed")
    print("Run: pip install -r requirements.txt")
    sys.exit(1)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s'
)
logger = logging.getLogger(__name__)

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'

def print_section(title: str):
    """Print a colored section header."""
    logger.info(f"\n{Colors.BLUE}{'=' * 60}")
    logger.info(f"{title.center(60)}")
    logger.info(f"{'=' * 60}{Colors.RESET}\n")

def print_success(message: str):
    """Print a success message."""
    logger.info(f"{Colors.GREEN}✓ {message}{Colors.RESET}")

def print_error(message: str):
    """Print an error message."""
    logger.info(f"{Colors.RED}✗ {message}{Colors.RESET}")

def print_warning(message: str):
    """Print a warning message."""
    logger.info(f"{Colors.YELLOW}⚠ {message}{Colors.RESET}")

def validate_environment() -> Tuple[bool, dict]:
    """Validate environment variables."""
    print_section("Environment Variables")
    
    errors = []
    config = {}
    
    # Load .env file if it exists
        
    # load env vars from .env
    # load_dotenv(dotenv_path=f"{root}/.env/.env")
    
    root = os.path.dirname(os.getcwd())
    env_file = Path(f"{root}/.env/") / ".env"
    if env_file.exists():
        logger.info(f"Loading environment from: {env_file}")
        load_dotenv(dotenv_path=env_file)
        print_success(".env file found")
    else:
        print_warning(".env file not found, using system environment")
    
    # Check required variables
    required_vars = {
        'GOOGLE_CLOUD_PROJECT': 'GCP Project ID',
        'GCP_GCS_BUCKET': 'GCS Bucket Name',
    }
    
    for var_name, description in required_vars.items():
        value = os.environ.get(var_name)
        if value:
            config[var_name] = value
            print_success(f"{description}: {value}")
        else:
            error_msg = f"{description} ({var_name}) not set"
            print_error(error_msg)
            errors.append(error_msg)
    
    # Check optional variables
    optional_vars = {
        'GOOGLE_APPLICATION_CREDENTIALS': 'Service Account Key',
        'BIGQUERY_DATASET_ID': 'BigQuery Dataset ID',
    }
    
    for var_name, description in optional_vars.items():
        value = os.environ.get(var_name)
        if value:
            config[var_name] = value
            print_success(f"{description}: {value}")
        else:
            print_warning(f"{description} not set (using defaults)")
    
    return len(errors) == 0, config

def validate_gcp_credentials() -> bool:
    """Validate GCP credentials."""
    print_section("GCP Credentials")
    
    try:
        # Try to get default credentials
        from google.auth import default
        credentials, project = default()
        print_success(f"GCP Credentials found for project: {project}")
        return True
    except Exception as e:
        print_error(f"Failed to get GCP credentials: {e}")
        logger.info("\nTo set up credentials:")
        logger.info("  1. gcloud auth application-default login")
        logger.info("  2. Or set GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json")
        return False

def validate_bigquery_access(project_id: str) -> bool:
    """Validate BigQuery access."""
    print_section("BigQuery Access")
    
    try:
        client = bigquery.Client(project=project_id)
        
        # Try to list projects (minimal permission test)
        logger.info("Testing BigQuery connection...")
        _ = client.get_dataset_ref(project_id)
        
        print_success("BigQuery connection successful")
        
        # List some datasets
        logger.info("\nBigQuery Datasets:")
        datasets = list(client.list_datasets(max_results=5))
        for ds in datasets:
            logger.info(f"  • {ds.dataset_id}")
        
        return True
    except PermissionDenied as e:
        print_error(f"Permission denied: {e}")
        print_warning("Service account may need BigQuery permissions")
        return False
    except Exception as e:
        print_error(f"BigQuery connection failed: {e}")
        return False

def validate_gcs_access(bucket_name: str) -> bool:
    """Validate GCS bucket access."""
    print_section("Google Cloud Storage")
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Try to get bucket metadata
        logger.info(f"Checking bucket: gs://{bucket_name}")
        _ = bucket.get_labels()  # Minimal operation to test access
        
        print_success(f"GCS bucket accessible: gs://{bucket_name}")
        
        # List files in bucket
        logger.info("\nFiles in bucket:")
        blobs = list(storage_client.list_blobs(bucket_name, max_results=20))
        
        if not blobs:
            print_warning("No files found in bucket")
        else:
            csv_files = [b for b in blobs if b.name.endswith('.csv')]
            for blob in csv_files[:10]:
                size_mb = blob.size / (1024 * 1024) if blob.size else 0
                logger.info(f"  • {blob.name} ({size_mb:.2f} MB)")
            
            if csv_files:
                print_success(f"Found {len(csv_files)} CSV file(s)")
            else:
                print_warning("No CSV files found in bucket")
        
        return True
    except PermissionDenied as e:
        print_error(f"Permission denied: {e}")
        print_warning("Service account may need Storage Object Viewer role")
        return False
    except Exception as e:
        print_error(f"GCS access failed: {e}")
        return False

def validate_required_csv_files(bucket_name: str) -> bool:
    """Check for required CSV files."""
    print_section("Required CSV Files")
    
    required_files = [
        'balance_sheet.csv',
        'income_statement.csv',
        'company_info.csv',
        'cash_flow_statement.csv',
    ]
    
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        all_found = True
        for filename in required_files:
            blob = storage.Blob(filename, bucket)
            try:
                blob.reload()  # Check if blob exists
                size_mb = blob.size / (1024 * 1024)
                print_success(f"{filename} ({size_mb:.2f} MB)")
            except NotFound:
                print_error(f"{filename} - NOT FOUND")
                all_found = False
        
        return all_found
    except Exception as e:
        print_error(f"Error checking files: {e}")
        return False

def validate_sql_scripts() -> bool:
    """Validate SQL script files."""
    print_section("SQL Scripts")
    
    script_dir = Path(__file__).parent
    required_scripts = [
        '01_create_external_tables.sql',
        '02_create_managed_tables.sql',
        '03_data_transformations.sql',
        '04_sample_queries.sql',
    ]
    
    all_found = True
    for script in required_scripts:
        path = script_dir / script
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print_success(f"{script} ({size_kb:.1f} KB)")
        else:
            print_error(f"{script} - NOT FOUND")
            all_found = False
    
    return all_found

def validate_python_dependencies() -> bool:
    """Validate required Python packages."""
    print_section("Python Dependencies")
    
    required_packages = {
        'google.cloud.bigquery': 'google-cloud-bigquery',
        'google.cloud.storage': 'google-cloud-storage',
        'dotenv': 'python-dotenv',
    }
    
    all_found = True
    for import_name, package_name in required_packages.items():
        try:
            __import__(import_name)
            print_success(f"{package_name}")
        except ImportError:
            print_error(f"{package_name} - NOT INSTALLED")
            all_found = False
    
    return all_found

def run_validation():
    """Run all validations."""
    print_section("BigQuery Pipeline Validator")
    
    results = {
        'Environment': validate_environment()[0],
        'GCP Credentials': validate_gcp_credentials(),
        'Python Dependencies': validate_python_dependencies(),
    }
    
    # Get config for next checks
    env_valid, config = validate_environment()
    if not env_valid:
        print_section("Validation Failed")
        print_error("Environment variables missing")
        return False
    
    project_id = config.get('GOOGLE_CLOUD_PROJECT')
    bucket_name = config.get('GCP_GCS_BUCKET')
    
    results['BigQuery Access'] = validate_bigquery_access(project_id)
    results['GCS Access'] = validate_gcs_access(bucket_name)
    results['Required CSV Files'] = validate_required_csv_files(bucket_name)
    results['SQL Scripts'] = validate_sql_scripts()
    
    # Summary
    print_section("Validation Summary")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for check, result in results.items():
        status = f"{Colors.GREEN}PASS{Colors.RESET}" if result else f"{Colors.RED}FAIL{Colors.RESET}"
        logger.info(f"  {check}: {status}")
    
    logger.info(f"\n{Colors.BLUE}Summary: {passed}/{total} checks passed{Colors.RESET}")
    
    if passed == total:
        print_success("All validations passed! Ready to run pipeline.")
        return True
    else:
        print_error("Some validations failed. Please fix the issues above.")
        return False

def main():
    """Main entry point."""
    try:
        success = run_validation()
        
        if success:
            logger.info(f"\n{Colors.GREEN}You can now run the pipeline:{Colors.RESET}")
            logger.info("  python3 bigquery_orchestrator.py")
            logger.info("  bash run_pipeline.sh")
            return 0
        else:
            logger.info(f"\n{Colors.RED}Please fix the issues above and try again.{Colors.RESET}")
            return 1
    except KeyboardInterrupt:
        logger.info("\nValidation interrupted by user.")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == '__main__':
    sys.exit(main())
