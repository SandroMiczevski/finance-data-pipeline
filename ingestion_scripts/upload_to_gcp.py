#https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/03-data-warehouse/extras/web_to_gcs.py

import os
import pandas as pd
import glob
import argparse
from google.cloud import storage
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# Define data directories
root_dir = os.path.dirname(os.getcwd())
raw_data_dir = f"{root_dir}/raw_data"


"""
Pre-reqs: 
1. run `uv sync` from this 'extra' folder (create venv and install dependencies from pyproject.toml)
2. rename .env-example to .env (not commited thanks to .gitignore)
3. in .env, 
    - set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
    - Set GOOGLE_APPLICATION_CREDENTIALS to your project/service-account json key 
    (or don't set it if you use google ADC)
"""

# load env vars from .env
# Load environment variables
root = os.path.dirname(os.getcwd())
print(f"Setting up environment from: {root}")

# load env vars from .env
load_dotenv(dotenv_path=f"{root}/.environment/.env")

BUCKET = os.environ.get("GCP_GCS_BUCKET", "findata-bucket")
project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")

# List available files
raw_files = glob.glob(f"{raw_data_dir}*")


def print_available_files():
    print("=== Available Raw Data Files ===")
    for file in sorted(raw_files):
        file_size = Path(file).stat().st_size / (1024 * 1024)  # Convert to MB
        print(f"  • {Path(file).name} ({file_size:.2f} MB)")



def upload_to_gcs(bucket_name, object_name, local_file, project_id=None):
    """
    Upload a file to Google Cloud Storage.
    
    Args:
        bucket_name (str): GCS bucket name
        object_name (str): The path/name in GCS (e.g., "raw_data/financials.json")
        local_file (str): Local file path to upload
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        
        print(f"Uploading {local_file} to gs://{bucket_name}/{object_name}...", end=" ")
        blob.upload_from_filename(local_file)
        print("✓ Done")
        return True
    except Exception as e:
        print(f"✗ Failed: {str(e)}")
        return False

def list_gcs_files(bucket_name=BUCKET, prefix="", project_id=None):
    """
    List all files in GCS bucket with optional prefix filter.
    
    Args:
        bucket_name (str): GCS bucket name
        prefix (str): Optional prefix to filter files (e.g., "raw_data/")
        project_id (str): GCP project ID
    
    Returns:
        list: List of file names in GCS
    """
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=prefix)
        
        files = []
        for blob in blobs:
            files.append({
                "name": blob.name,
                "size_mb": blob.size / (1024 * 1024),
                "updated": blob.updated
            })
        
        return files
    except Exception as e:
        print(f"Error listing files: {str(e)}")
        return []

def upload_raw_data_files(bucket_name=BUCKET, raw_data_path=raw_data_dir, project_id=None):
    """
    Upload all raw data files (CSV and JSON) to GCS.
    
    Args:
        bucket_name (str): GCS bucket name
        raw_data_path (str): Path to raw data directory
        project_id (str): GCP project ID
    
    Returns:
        dict: Summary of uploads with success/failure counts
    """
    raw_files = glob.glob(f"{raw_data_path}*")
    
    success_count = 0
    failed_count = 0
    results = []
    
    print("=" * 60)
    print("UPLOADING RAW DATA FILES")
    print("=" * 60)
    
    for local_file in sorted(raw_files):
        file_name = Path(local_file).name
        
        # Skip non-data files
        if file_name.startswith("."):
            continue
        
        # Create GCS path
        gcs_path = f"raw_data/{file_name}"
        
        # Upload file
        if upload_to_gcs(bucket_name, gcs_path, local_file, project_id=project_id):
            success_count += 1
            results.append({"file": file_name, "status": "✓ Success"})
        else:
            failed_count += 1
            results.append({"file": file_name, "status": "✗ Failed"})
    
    print("\n" + "=" * 60)
    print(f"Summary: {success_count} succeeded, {failed_count} failed")
    print("=" * 60)
    
    return {"success": success_count, "failed": failed_count, "results": results}

def verify_uploads(bucket_name=BUCKET, project_id=None):
    """
    Verify all uploaded files in GCS.
    """
    print("=" * 60)
    print("VERIFYING UPLOADS IN GCS")
    print("=" * 60)
    
    print("\n📁 Raw Data Files:")
    raw_files = list_gcs_files(bucket_name, "raw_data/", project_id=project_id)
    if raw_files:
        for f in raw_files:
            print(f"  • {Path(f['name']).name} ({f['size_mb']:.2f} MB)")
    else:
        print("  No files found")
    
    print("\n💾 Database Files:")
    db_files = list_gcs_files(bucket_name, "database/", project_id=project_id)
    if db_files:
        for f in db_files:
            print(f"  • {Path(f['name']).name} ({f['size_mb']:.2f} MB)")
    else:
        print("  No files found")
    
    total_files = len(raw_files) + len(db_files)
    print("\n" + "=" * 60)
    print(f"Total files in GCS: {total_files}")
    print("=" * 60)



def upload_specific_file(file_name, file_type="raw_data", bucket_name=BUCKET, project_id=None):
    """
    Upload a specific file to GCS.
    
    Args:
        file_name (str): Name of the file to upload
        file_type (str): Type - 'raw_data' or 'database'
        bucket_name (str): GCS bucket name
        project_id (str): GCP project ID
    
    Returns:
        bool: True if successful
    """
    if file_type == "raw_data":
        local_file = f"{raw_data_dir}{file_name}"
    else:
        print(f"Unknown file type: {file_type}")
        return False
    
    if not Path(local_file).exists():
        print(f"File not found: {local_file}")
        return False
    
    gcs_path = f"{file_type}/{file_name}"
    return upload_to_gcs(bucket_name, gcs_path, local_file, project_id=project_id)



def get_upload_statistics(bucket_name=BUCKET, project_id=None):
    """
    Generate statistics about local and uploaded files.
    """
    print("=" * 60)
    print("UPLOAD STATISTICS")
    print("=" * 60)
    
    # Local files statistics
    print("\n📊 LOCAL FILES:")
    raw_files = glob.glob(f"{raw_data_dir}*")
    
    raw_data_size = sum(Path(f).stat().st_size for f in raw_files if not Path(f).name.startswith("."))
    
    print(f"  Raw Data: {len(raw_files)} files, {raw_data_size / (1024 * 1024):.2f} MB")
    
    # GCS files statistics
    print("\n☁️  GCS FILES:")
    try:
        all_gcs_files = list_gcs_files(bucket_name, project_id=project_id)
        gcs_size = sum(f['size_mb'] for f in all_gcs_files)
        print(f"  Total uploaded: {len(all_gcs_files)} files, {gcs_size:.2f} MB")
    except Exception as e:
        print(f"  Error retrieving GCS stats: {str(e)}")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":

    p = argparse.ArgumentParser(description="Upload local files to GCS")
    p.add_argument("--upload_json", action="store_true", help="Upload all JSON files in raw_data/")
    p.add_argument("--upload_csv", action="store_true", help="Upload all CSV files in raw_data/")
    p.add_argument("--verify", action="store_true", help="Verify uploaded files in GCS")
    p.add_argument("--stats", action="store_true", help="Get upload statistics")
    args = p.parse_args()   


    # Print available local files
    print_available_files()

    if args.upload_json:
        path = Path(f"{raw_data_dir}/JSON/")
        print(f"\nUploading JSON files in {path}...")
        
        for file in path.glob("*.json"):
            print(f"Processing file: {file}")
            # Read the JSON file
            try:
                data = pd.read_json(file)
                print(f"  ✓ Read {len(data)} records from {file.name}")
            except Exception as e:
                print(f"  ✗ Error reading {file.name}: {str(e)}")
            # if not Path(file).name.startswith("."):  # Skip hidden files
                # upload_to_gcs(BUCKET, f"raw_data/JSON/{Path(file).name}", file, project_id=project_id)

    elif args.upload_csv:
        path = Path(f"{raw_data_dir}/CSV/")
        print(f"\nUploading CSV files in {path}...")
        
        for file in path.glob("*.csv"):
            # print(f"Path(file).name: {Path(file).name}")
            
            if not Path(file).name.startswith("."):  # Skip hidden files
                upload_to_gcs(BUCKET, f"raw_data/CSV/{Path(file).name}", file, project_id=project_id)

    else:
        print("\nNo upload option specified. Use --upload_json or --upload_csv to upload files.")
    
    # Verify uploads in GCS
    if args.verify:
        verify_uploads(bucket_name=BUCKET, project_id=project_id)

    # Get upload statistics
    if args.stats:
        get_upload_statistics(project_id=project_id)



# Get statistics
# get_upload_statistics(project_id=project_id)

# Example: Upload specific files
# upload_specific_file("financials.json", "raw_data", project_id=project_id)
# upload_specific_file("finance.duckdb", "database", project_id=project_id)