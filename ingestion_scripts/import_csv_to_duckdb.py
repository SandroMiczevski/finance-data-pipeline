#!/usr/bin/env python3
"""
Script to import CSV files from source directory into DuckDB.
Supports replace and append modes.
"""
import argparse
import duckdb
import pandas as pd
from pathlib import Path
import sys


def map_pandas_dtype_to_sql(pandas_dtype):
    """Map pandas dtypes to SQL types"""
    dtype_str = str(pandas_dtype).lower()
    
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE'
    elif 'bool' in dtype_str:
        return 'BOOLEAN'
    elif 'datetime' in dtype_str:
        return 'TIMESTAMP'
    else:
        return 'VARCHAR'


def main():
    parser = argparse.ArgumentParser(
        description="Import CSV files from source directory into DuckDB"
    )
    parser.add_argument(
        "--source",
        type=str,
        required=True,
        help="Directory containing CSV files to import"
    )
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Directory where finance.duckdb will be saved"
    )
    parser.add_argument(
        "--mode",
        choices=["replace", "append"],
        default="replace",
        help="Import mode: replace (rename old table) or append (add to existing table)"
    )
    
    args = parser.parse_args()
    
    source_path = Path(args.source)
    output_path = Path(args.output)
    mode = args.mode
    
    # Validate source directory
    if not source_path.exists() or not source_path.is_dir():
        print(f"Error: Source directory not found: {source_path}")
        sys.exit(1)
    
    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)
    
    db_path = output_path / "finance.duckdb"
    
    print(f"Source directory: {source_path}")
    print(f"Output database: {db_path}")
    print(f"Mode: {mode}\n")
    
    # Connect to DuckDB
    conn = duckdb.connect(str(db_path))
    
    try:
        # Find all CSV files
        csv_files = sorted(source_path.glob("*.csv"))
        
        if not csv_files:
            print(f"No CSV files found in {source_path}")
            return
        
        print(f"Found {len(csv_files)} CSV file(s):\n")
        
        for csv_file in csv_files:
            table_name = csv_file.stem
            
            try:
                print(f"Processing: {csv_file.name}")
                
                # Read CSV
                df = pd.read_csv(csv_file, sep='\t')
                print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
                
                # Build schema
                schema_parts = []
                for col_name, col_dtype in zip(df.columns, df.dtypes):
                    sql_type = map_pandas_dtype_to_sql(col_dtype)
                    schema_parts.append(f'"{col_name}" {sql_type}')
                
                schema = ", ".join(schema_parts)
                
                # Handle replace mode
                if mode == "replace":
                    # Check if table exists
                    existing_tables = conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                    existing_table_names = [t[0] for t in existing_tables]
                    
                    if table_name in existing_table_names:
                        # Rename old table
                        backup_name = f"{table_name}_backup"
                        conn.execute(f"ALTER TABLE {table_name} RENAME TO {backup_name}")
                        print(f"  ✓ Renamed existing table to {backup_name}")
                    
                    # Create new table
                    create_sql = f"CREATE TABLE {table_name} ({schema})"
                    conn.execute(create_sql)
                    print(f"  ✓ Created new table with schema")
                
                elif mode == "append":
                    # Check if table exists
                    existing_tables = conn.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                    existing_table_names = [t[0] for t in existing_tables]
                    
                    if table_name not in existing_table_names:
                        # Create new table
                        create_sql = f"CREATE TABLE {table_name} ({schema})"
                        conn.execute(create_sql)
                        print(f"  ✓ Created new table with schema")
                    else:
                        print(f"  ✓ Appending to existing table")
                
                # Register dataframe as temp table and insert
                temp_table = f"temp_{table_name}"
                conn.register(temp_table, df)
                conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table}")
                conn.unregister(temp_table)
                
                # Verify
                row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
                print(f"  ✓ Inserted {len(df)} rows (total: {row_count} rows)\n")
                
            except Exception as e:
                print(f"  ✗ Error: {str(e)}\n")
                continue
        
        # Summary
        print("=" * 60)
        print("Final Database Summary:")
        print("=" * 60)
        
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY table_name"
        ).fetchall()
        
        for table in tables:
            table_name = table[0]
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            col_count = conn.execute(
                f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name='{table_name}'"
            ).fetchone()[0]
            print(f"  • {table_name}: {row_count} rows, {col_count} columns")
        
        print(f"\n✓ Import completed successfully!")
        
    finally:
        conn.close()


if __name__ == "__main__":
    main()
