import argparse
from pathlib import Path

import duckdb
import pandas as pd


def load_dataframe(source_path: Path) -> pd.DataFrame:
    suffix = source_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(source_path)
    elif suffix == ".json":
        df = pd.read_json(source_path, orient="records")
        if df.empty:
            with source_path.open("r", encoding="utf-8") as f:
                raw = pd.read_json(f)
                df = pd.json_normalize(raw)
        else:
            df = pd.json_normalize(df)
        return df
    elif suffix in {".parquet", ".pq"}:
        return pd.read_parquet(source_path)
    else:
        raise ValueError(f"Unsupported source file type: {suffix}")


def main():
    parser = argparse.ArgumentParser(description="Load data into DuckDB from CSV/JSON/Parquet")
    parser.add_argument("source", help="Path to source file")
    parser.add_argument("--db", default="financials.duckdb", help="Path to DuckDB database file")
    parser.add_argument("--table", default="financials", help="Target table name")
    parser.add_argument("--mode", choices=["replace", "append"], default="replace", help="Write mode")
    args = parser.parse_args()

    source_path = Path(args.source)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    print(f"Loading {source_path} into {args.db} table {args.table}...", flush=True)

    df = load_dataframe(source_path)

    con = duckdb.connect(str(args.db))
    if args.mode == "replace":
        con.execute(f"DROP TABLE IF EXISTS {args.table}")

    # If numeric columns are read as objects, attempt safe conversion
    for col in df.select_dtypes(include=["object"]).columns:
        conv = pd.to_numeric(df[col], errors="coerce")
        if conv.notna().sum() > 0.8 * len(df):
            df[col] = conv

    con.register("_tmp_df", df)
    if args.mode == "replace":
        con.execute(f"CREATE TABLE {args.table} AS SELECT * FROM _tmp_df")
    else:
        try:
            con.execute(f"INSERT INTO {args.table} SELECT * FROM _tmp_df")
        except duckdb.Error:
            con.execute(f"CREATE TABLE {args.table} AS SELECT * FROM _tmp_df")

    row_count = con.execute(f"SELECT COUNT(*) FROM {args.table}").fetchone()[0]
    print(f"Loaded {row_count} rows into {args.table} in database {args.db}")

    con.close()


if __name__ == "__main__":
    main()
