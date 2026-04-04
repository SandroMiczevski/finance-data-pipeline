#!/usr/bin/env python3
"""
BigQuery Orchestrator Script

This script orchestrates the creation of BigQuery tables from GCS files
and runs transformations to create analytical datasets.

Usage:
    python bigquery_orchestrator.py --project-id <project_id> --dataset-id <dataset_id> --bucket-name <bucket_name>
"""

import argparse
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError, NotFound
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BigQueryOrchestrator:
    """Handles BigQuery operations for finance data pipeline."""
    
    def __init__(self, project_id: str, dataset_id: str, bucket_name: str):
        """
        Initialize the BigQuery orchestrator.
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            bucket_name: GCS bucket name containing the CSV files
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bucket_name = bucket_name
        self.client = bigquery.Client(project=project_id)
        self.script_dir = Path(__file__).parent
        
    def _read_sql_file(self, filename: str) -> str:
        """Read SQL file and replace placeholders."""
        file_path = self.script_dir / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        with open(file_path, 'r') as f:
            sql = f.read()
        
        # Replace placeholders
        sql = sql.replace('{project_id}', self.project_id)
        sql = sql.replace('{dataset_id}', self.dataset_id)
        sql = sql.replace('{bucket_name}', self.bucket_name)
        
        return sql
    
    def ensure_dataset_exists(self) -> None:
        """Create dataset if it doesn't exist."""
        dataset_id_full = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.client.get_dataset(dataset_id_full)
            logger.info(f"Dataset {dataset_id_full} already exists.")
        except NotFound:
            logger.info(f"Creating dataset {dataset_id_full}...")
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            logger.info(f"Created dataset {dataset_id_full}.")
    
    def create_external_tables(self) -> bool:
        """Create external tables pointing to GCS files."""
        try:
            logger.info("Creating external tables...")
            sql = self._read_sql_file('01_create_external_tables.sql')
            
            # Execute each statement separately
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}...")
                self.client.query(statement).result()
            
            logger.info("✓ External tables created successfully.")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error creating external tables: {e}")
            return False
    
    def create_managed_tables(self) -> bool:
        """Create managed tables from external tables."""
        try:
            logger.info("Creating managed tables...")
            sql = self._read_sql_file('02_create_managed_tables.sql')
            
            # Execute each statement separately
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}...")
                query_job = self.client.query(statement)
                query_job.result()
            
            logger.info("✓ Managed tables created successfully.")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error creating managed tables: {e}")
            return False
    
    def create_transformation_tables(self) -> bool:
        """Create transformation and analytical tables."""
        try:
            logger.info("Creating transformation tables...")
            sql = self._read_sql_file('03_data_transformations.sql')
            
            # Execute each statement separately
            statements = [s.strip() for s in sql.split(';') if s.strip()]
            
            for i, statement in enumerate(statements, 1):
                logger.info(f"Executing statement {i}/{len(statements)}...")
                query_job = self.client.query(statement)
                query_job.result()
            
            logger.info("✓ Transformation tables created successfully.")
            return True
            
        except GoogleCloudError as e:
            logger.error(f"Error creating transformation tables: {e}")
            return False
    
    def list_tables(self) -> None:
        """List all tables in the dataset."""
        try:
            tables = self.client.list_tables(self.dataset_id)
            
            logger.info(f"\nTables in dataset {self.dataset_id}:")
            for table in tables:
                full_id = f"{table.project}.{table.dataset_id}.{table.table_id}"
                logger.info(f"  • {full_id}")
                
        except GoogleCloudError as e:
            logger.error(f"Error listing tables: {e}")
    
    def get_table_info(self, table_name: str) -> None:
        """Get information about a specific table."""
        try:
            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
            table = self.client.get_table(table_id)
            
            logger.info(f"\nTable: {table_id}")
            logger.info(f"Rows: {table.num_rows:,}")
            logger.info(f"Bytes: {table.num_bytes:,}")
            logger.info(f"Created: {table.created}")
            logger.info(f"\nSchema:")
            for field in table.schema:
                logger.info(f"  • {field.name}: {field.field_type}")
                
        except GoogleCloudError as e:
            logger.error(f"Error getting table info: {e}")
    
    def run_full_pipeline(self) -> bool:
        """Run the complete pipeline."""
        logger.info("Starting BigQuery Finance Data Pipeline...")
        logger.info(f"Project: {self.project_id}")
        logger.info(f"Dataset: {self.dataset_id}")
        logger.info(f"Bucket: {self.bucket_name + '/CSV/'}")
        logger.info("-" * 60)
                
        # Step 1: Ensure dataset exists
        self.ensure_dataset_exists()
        
        # Step 2: Create external tables
        if not self.create_external_tables():
            logger.error("Failed to create external tables. Aborting.")
            return False
        
        print("Here we are after creating external tables. We will now create managed tables.")

        # Step 3: Create managed tables
        if not self.create_managed_tables():
            logger.error("Failed to create managed tables. Aborting.")
            return False
        
        # Step 4: Create transformation tables
        if not self.create_transformation_tables():
            logger.error("Failed to create transformation tables. Aborting.")
            return False
        
        logger.info("-" * 60)
        logger.info("✓ Pipeline completed successfully!")
        
        # List all created tables
        self.list_tables()
        
        return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='BigQuery Finance Data Pipeline Orchestrator'
    )
    parser.add_argument(
        '--project-id',
        required=False,
        help='GCP Project ID'
    )
    parser.add_argument(
        '--dataset-id',
        default='finance_data',
        help='BigQuery Dataset ID (default: finance_data)'
    )
    parser.add_argument(
        '--bucket-name',
        required=False,
        help='GCS Bucket name'
    )
    parser.add_argument(
        '--list-tables',
        action='store_true',
        help='List all tables in the dataset'
    )
    parser.add_argument(
        '--table-info',
        help='Get information about a specific table'
    )
    
    args = parser.parse_args()
    
    # Load environment variables
    root = os.path.dirname(os.getcwd())
    print(f"Setting up environment from: {root}")
    
    # load env vars from .env
    load_dotenv(dotenv_path=f"{root}/.environment/.env")
    
    # Get configuration from arguments or environment
    project_id = args.project_id or os.environ.get('GOOGLE_CLOUD_PROJECT')
    bucket_name = args.bucket_name or os.environ.get('GCP_GCS_BUCKET')

    if os.environ.get('GCP_GCS_BUCKET') and not args.bucket_name:
        bucket_name = f"{bucket_name}/raw_data/CSV"
    
    if not project_id:
        logger.error("Project ID not provided. Use --project-id or set GOOGLE_CLOUD_PROJECT.")
        return 1
    
    if not bucket_name:
        logger.error("Bucket name not provided. Use --bucket-name or set GCP_GCS_BUCKET.")
        return 1
    
    # Initialize orchestrator
    orchestrator = BigQueryOrchestrator(
        project_id=project_id,
        dataset_id=args.dataset_id,
        bucket_name=bucket_name
    )
    
    # Handle different command modes
    if args.list_tables:
        orchestrator.list_tables()
    elif args.table_info:
        orchestrator.get_table_info(args.table_info)
    else:
        # Run full pipeline
        success = orchestrator.run_full_pipeline()
        return 0 if success else 1


if __name__ == '__main__':
    exit(main())
