import argparse
from pathlib import Path

import pandas as pd

DUCKDB_TYPE_MAP = {
    "int64": "BIGINT",
    "float64": "DOUBLE",
    "bool": "BOOLEAN",
    "datetime64[ns]": "TIMESTAMP",
    "object": "VARCHAR",
    "string": "VARCHAR",
    "category": "VARCHAR",
}


def infer_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    elif suffix == ".json":
        data = pd.read_json(path, orient="records")
        if data.empty:
            # if top-level is object of records by key
            with path.open("r", encoding="utf-8") as f:
                raw = pd.read_json(f)
                data = pd.json_normalize(raw)
        else:
            data = pd.json_normalize(data)
        return data
    elif suffix in {".parquet", ".pq"}:
        return pd.read_parquet(path)
    else:
        raise ValueError(f"Unsupported file type: {suffix}")


def generate_schema(df: pd.DataFrame, table_name: str) -> str:
    lines = []
    for col, dtype in df.dtypes.items():
        dtype_name = str(dtype)
        duck_type = DUCKDB_TYPE_MAP.get(dtype_name, "VARCHAR")
        lines.append(f"    {col} {duck_type}")

    cols = ",\n".join(lines)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{cols}\n);"


def main():
    parser = argparse.ArgumentParser(description="Extract table schema from CSV/JSON/Parquet to DuckDB DDL")
    parser.add_argument("source", help="Path to source file (.csv/.json/.parquet)")
    parser.add_argument("--table", default="financials", help="Destination table name")
    parser.add_argument("--out", help="Path to output schema SQL file")

    args = parser.parse_args()
    source_path = Path(args.source)
    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    df = infer_dataframe(source_path)
    ddl = generate_schema(df, args.table)
    print(ddl)

    if args.out:
        Path(args.out).write_text(ddl, encoding="utf-8")
        print(f"Schema written to {args.out}")


if __name__ == "__main__":
    main()
