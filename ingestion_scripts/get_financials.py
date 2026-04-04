"""get_financials.py

Fetch financial statements (balance sheet, income statement, cash flow) for US-listed companies.
Supports yfinance and FinancialModelingPrep (FMP) JSON endpoints.

Usage:
  python get_financials.py --tickers AAPL MSFT --years 2021 2022 --source yfinance --output out.json
  FMP: set env var FMP_API_KEY and use --source fmp
"""

import argparse
import os
import json
from datetime import datetime

try:
    import yfinance as yf
except ImportError:
    yf = None

import requests


def _filter_by_years(df, years):
    if years is None or len(years) == 0:
        return df
    keep = []
    for col in df.columns:
        if isinstance(col, datetime):
            year = col.year
        else:
            try:
                year = int(str(col)[:4])
            except Exception:
                continue
        if year in years:
            keep.append(col)
    return df.loc[:, keep]


def get_yfinance_financials(ticker, years=None):
    if yf is None:
        raise RuntimeError("yfinance is not installed. pip install yfinance")

    ty = yf.Ticker(ticker)
    results = {
        "ticker": ticker,
        "source": "yfinance",
        "company_info": ty.info if hasattr(ty, "info") else {},
    }

    statements = {
        "balance_sheet": getattr(ty, "balance_sheet", None),
        "income_statement": getattr(ty, "financials", None),
        "cash_flow_statement": getattr(ty, "cashflow", None),
        "earnings": getattr(ty, "earnings", None),
        "quarterly_balance_sheet": getattr(ty, "quarterly_balance_sheet", None),
        "quarterly_financials": getattr(ty, "quarterly_financials", None),
        "quarterly_cashflow": getattr(ty, "quarterly_cashflow", None),
    }

    for label, df in statements.items():
        if df is None:
            results[label] = None
            continue
        filtered = _filter_by_years(df, years) if years else df
        results[label] = json.loads(filtered.fillna("").to_json(orient="index", date_format="iso"))

    return results


def _fmp_request(path, ticker, limit=1000):
    key = os.environ.get("FMP_API_KEY", "")
    if not key:
        raise RuntimeError("Environment variable FMP_API_KEY is required for source=fmp")
    url = f"https://financialmodelingprep.com/api/v3/{path}/{ticker}?limit={limit}&apikey={key}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()


def get_fmp_financials(ticker, years=None):
    results = {
        "ticker": ticker,
        "source": "fmp",
    }

    items = {
        "balance_sheet": "balance-sheet-statement",
        "income_statement": "income-statement",
        "cash_flow_statement": "cash-flow-statement",
        "ratios": "ratios",
        "cash_flow_ratios": "cash-flow-statement",
        "profile": "profile",
    }

    for label, endpoint in items.items():
        try:
            data = _fmp_request(endpoint, ticker)
            if years:
                data = [d for d in data if d.get("calendarYear") in years or d.get("date") and int(d.get("date")[:4]) in years]
            results[label] = data
        except Exception as ex:
            results[label] = {"error": str(ex)}

    return results


def get_financials(ticker, years=None, source="yfinance"):
    years_set = {int(y) for y in years} if years else None
    if source == "yfinance":
        return get_yfinance_financials(ticker, years_set)
    elif source == "fmp":
        return get_fmp_financials(ticker, years_set)
    else:
        raise ValueError("Unknown source: choose 'yfinance' or 'fmp'")


def run(tickers, years=None, source="yfinance", output=None, pretty=False, save_dir=None):
    if isinstance(tickers, str):
        tickers = [tickers]

    all_data = []
    for t in tickers:
        print(f"Fetching {t} from {source}...", flush=True)
        data = get_financials(t, years=years, source=source)
        all_data.append(data)

    if output:
        with open(f"{save_dir}/{output}", "w", encoding="utf-8") as f:
            if pretty:
                json.dump(all_data, f, indent=2)
            else:
                json.dump(all_data, f)
        print(f"Saved results to: {save_dir}/{output}")

    return all_data


def parse_args():
    p = argparse.ArgumentParser(description="Download US-listed company financial statements.")
    p.add_argument("--tickers", nargs="+", required=True, help="Ticker symbols, e.g., AAPL MSFT")
    p.add_argument("--years", nargs="*", type=int, default=[], help="Year filters, e.g., 2020 2021")
    p.add_argument("--source", choices=["yfinance", "fmp"], default="yfinance", help="Data source")
    p.add_argument("--output", default="financials.json", help="Output JSON file")
    p.add_argument("--save_dir", default="raw_data/JSON/", help="Directory to save output JSON")
    p.add_argument("--pretty", action="store_true", help="Pretty-print JSON")
    return p.parse_args()

if __name__ == "__main__":
    root_dir = os.path.dirname(os.getcwd())

    args = parse_args()
        
    result = run(args.tickers, 
                 years=args.years, 
                 source=args.source, 
                 output=args.output, 
                 pretty=args.pretty, 
                 save_dir=f"{root_dir}/{args.save_dir}")
    
    print(json.dumps(result, indent=2) if args.pretty else json.dumps(result))
