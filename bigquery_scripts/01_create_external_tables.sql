-- BigQuery SQL - Create External Tables from GCS
-- This script creates external tables pointing to CSV files in GCS bucket

-- External table for Balance Sheet data
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.balance_sheet_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{bucket_name}/balance_sheet.csv'],
  skip_leading_rows = 1,
  allow_jagged_rows = true,
  allow_quoted_newlines = true
);

-- External table for Income Statement data
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.income_statement_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{bucket_name}/income_statement.csv'],
  skip_leading_rows = 1,
  allow_jagged_rows = true,
  allow_quoted_newlines = true
);

-- External table for Company Info data
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.company_info_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{bucket_name}/company_info.csv'],
  skip_leading_rows = 1,
  allow_jagged_rows = true,
  allow_quoted_newlines = true
);

-- External table for Cash Flow Statement data
CREATE OR REPLACE EXTERNAL TABLE `{project_id}.{dataset_id}.cash_flow_statement_external`
OPTIONS (
  format = 'CSV',
  uris = ['gs://{bucket_name}/cash_flow_statement.csv'],
  skip_leading_rows = 1,
  allow_jagged_rows = true,
  allow_quoted_newlines = true
);
