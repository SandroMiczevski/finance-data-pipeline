# If .secrets/fmp.py is a local file, add it to sys.path first:
#import sys
from pathlib import Path
root_dir = Path.cwd().parent 

import argparse
import os
import json
from datetime import datetime
import pandas as pd

try:
    import yfinance as yf
except ImportError:
    yf = None


balance_sheet_dtype = {
    "Treasury Shares Number": "Int64",
    "Ordinary Shares Number": "Int64",
    "Share Issued": "Int64",
    "Net Debt": "float64",
    "Total Debt": "float64",
    "Tangible Book Value": "float64",
    "Invested Capital": "float64",
    "Working Capital": "float64",
    "Net Tangible Assets": "float64",
    "Capital Lease Obligations": "float64",
    "Common Stock Equity": "float64",
    "Total Capitalization": "float64",
    "Total Equity Gross Minority Interest": "float64",
    "Stockholders Equity": "float64",
    "Gains Losses Not Affecting Retained Earnings": "float64",
    "Other Equity Adjustments": "float64",
    "Retained Earnings": "float64",
    "Capital Stock": "float64",
    "Common Stock": "float64",
    "Total Liabilities Net Minority Interest": "float64",
    "Total Non Current Liabilities Net Minority Interest": "float64",
    "Other Non Current Liabilities": "float64",
    "Tradeand Other Payables Non Current": "float64",
    "Long Term Debt And Capital Lease Obligation": "float64",
    "Long Term Capital Lease Obligation": "float64",
    "Long Term Debt": "float64",
    "Current Liabilities": "float64",
    "Other Current Liabilities": "float64",
    "Current Deferred Liabilities": "float64",
    "Current Deferred Revenue": "float64",
    "Current Debt And Capital Lease Obligation": "float64",
    "Current Capital Lease Obligation": "float64",
    "Current Debt": "float64",
    "Other Current Borrowings": "float64",
    "Commercial Paper": "float64",
    "Payables And Accrued Expenses": "float64",
    "Current Accrued Expenses": "float64",
    "Payables": "float64",
    "Total Tax Payable": "float64",
    "Income Tax Payable": "float64",
    "Accounts Payable": "float64",
    "Total Assets": "float64",
    "Total Non Current Assets": "float64",
    "Other Non Current Assets": "float64",
    "Non Current Deferred Assets": "float64",
    "Non Current Deferred Taxes Assets": "float64",
    "Investments And Advances": "float64",
    "Other Investments": "float64",
    "Investmentin Financial Assets": "float64",
    "Available For Sale Securities": "float64",
    "Net PPE": "float64",
    "Accumulated Depreciation": "float64",
    "Gross PPE": "float64",
    "Leases": "float64",
    "Other Properties": "float64",
    "Machinery Furniture Equipment": "float64",
    "Land And Improvements": "float64",
    "Properties": "float64",
    "Current Assets": "float64",
    "Other Current Assets": "float64",
    "Inventory": "float64",
    "Receivables": "float64",
    "Other Receivables": "float64",
    "Accounts Receivable": "float64",
    "Cash Cash Equivalents And Short Term Investments": "float64",
    "Other Short Term Investments": "float64",
    "Cash And Cash Equivalents": "float64",
    "Cash Equivalents": "float64",
    "Cash Financial": "float64"
}

income_statement_dtype = {
    "Total Revenue": "float64",
    "Operating Revenue": "float64",
    "Revenue": "float64",
    "Cost Of Revenue": "float64",
    "Gross Profit": "float64",
    "Selling General And Administration": "float64",
    "Research And Development": "float64",
    "Operating Expenses": "float64",
    "Operating Income": "float64",
    "Total Operating Expenses": "float64",
    "Operating Income Or Loss": "float64",
    "Pretax Income": "float64",
    "Income Tax Expense": "float64",
    "Income From Continuing Operations": "float64",
    "Net Income From Continuing Operations": "float64",
    "Discontinued Operation": "float64",
    "Extraordinary Items": "float64",
    "Effect Of Accounting Charges": "float64",
    "Other Items": "float64",
    "Net Income": "float64",
    "Preferred Stock And Other Adjustments": "float64",
    "Net Income Common Stockholders": "float64",
    "Basic EPS": "float64",
    "Diluted EPS": "float64",
    "Basic Average Shares": "Int64",
    "Diluted Average Shares": "Int64",
    "Total Costs": "float64",
    "Total Expenses": "float64",
    "Minority Interest Expense": "float64",
    "Tax Provision": "float64",
    "Other Income Expense": "float64",
    "Interest Expense": "float64",
    "Interest Income": "float64",
}

cash_flow_statement_dtype = {
    "Operating Cash Flow": "float64",
    "Cash Flow From Continuing Operating Activities": "float64",
    "Net Income": "float64",
    "Depreciation": "float64",
    "Deferred Income Taxes": "float64",
    "Stock Based Compensation": "float64",
    "Other Non Cash Items": "float64",
    "Changes In Working Capital": "float64",
    "Accounts Receivable": "float64",
    "Inventory": "float64",
    "Accounts Payable": "float64",
    "Other Working Capital": "float64",
    "Other Operating Activities": "float64",
    "Investing Cash Flow": "float64",
    "Investing Activities Other": "float64",
    "Capital Expenditures": "float64",
    "Net Acquisitions": "float64",
    "Other Investments": "float64",
    "Cash Flow From Continuing Investing Activities": "float64",
    "Financing Cash Flow": "float64",
    "Cash Dividends Paid": "float64",
    "Cash Paid For Taxes": "float64",
    "Issuance Of Stock": "float64",
    "Repurchase Of Stock": "float64",
    "Issuance Of Debt": "float64",
    "Repayment Of Debt": "float64",
    "Other Financing Activities": "float64",
    "Cash Flow From Continuing Financing Activities": "float64",
    "Effect Of Exchange Rate On Cash": "float64",
    "Net Change In Cash": "float64",
    "Free Cash Flow": "float64",
    "Common Stock Issued": "Int64",
    "Common Stock Repurchased": "Int64",
}

company_info_dtype = {
    'address1': 'str',
    'city': 'str',
    'state': 'str',
    'zip': 'str',
    'country': 'str',
    'phone': 'str',
    'website': 'str',
    'industry': 'str',
    'industryKey': 'str',
    'industryDisp': 'str',
    'sector': 'str',
    'sectorKey': 'str',
    'sectorDisp': 'str',
    'longBusinessSummary': 'str',
    'fullTimeEmployees': 'Int64',
    'auditRisk': 'Int64',
    'boardRisk': 'Int64',
    'compensationRisk': 'Int64',
    'shareHolderRightsRisk': 'Int64',
    'overallRisk': 'Int64',
    'governanceEpochDate': 'Int64',
    'compensationAsOfEpochDate': 'Int64',
    'irWebsite': 'str',
    'executiveTeam': 'object',
    'maxAge': 'Int64',
    'priceHint': 'Int64',
    'previousClose': 'float64',
    'open': 'float64',
    'dayLow': 'float64',
    'dayHigh': 'float64',
    'regularMarketPreviousClose': 'float64',
    'regularMarketOpen': 'float64',
    'regularMarketDayLow': 'float64',
    'regularMarketDayHigh': 'float64',
    'dividendRate': 'float64',
    'dividendYield': 'float64',
    'exDividendDate': 'Int64',
    'payoutRatio': 'float64',
    'fiveYearAvgDividendYield': 'float64',
    'beta': 'float64',
    'trailingPE': 'float64',
    'forwardPE': 'float64',
    'volume': 'Int64',
    'regularMarketVolume': 'Int64',
    'averageVolume': 'Int64',
    'averageVolume10days': 'Int64',
    'averageDailyVolume10Day': 'Int64',
    'bid': 'float64',
    'ask': 'float64',
    'bidSize': 'Int64',
    'askSize': 'Int64',
    'marketCap': 'Int64',
    'nonDilutedMarketCap': 'Int64',
    'fiftyTwoWeekLow': 'float64',
    'fiftyTwoWeekHigh': 'float64',
    'allTimeHigh': 'float64',
    'allTimeLow': 'float64',
    'priceToSalesTrailing12Months': 'float64',
    'fiftyDayAverage': 'float64',
    'twoHundredDayAverage': 'float64',
    'trailingAnnualDividendRate': 'float64',
    'trailingAnnualDividendYield': 'float64',
    'currency': 'str',
    'tradeable': 'bool',
    'enterpriseValue': 'Int64',
    'profitMargins': 'float64',
    'floatShares': 'Int64',
    'sharesOutstanding': 'Int64',
    'sharesShort': 'Int64',
    'sharesShortPriorMonth': 'Int64',
    'sharesShortPreviousMonthDate': 'Int64',
    'dateShortInterest': 'Int64',
    'sharesPercentSharesOut': 'float64',
    'heldPercentInsiders': 'float64',
    'heldPercentInstitutions': 'float64',
    'shortRatio': 'float64',
    'shortPercentOfFloat': 'float64',
    'impliedSharesOutstanding': 'Int64',
    'bookValue': 'float64',
    'priceToBook': 'float64',
    'lastFiscalYearEnd': 'Int64',
    'nextFiscalYearEnd': 'Int64',
    'mostRecentQuarter': 'Int64',
    'earningsQuarterlyGrowth': 'float64',
    'netIncomeToCommon': 'Int64',
    'trailingEps': 'float64',
    'forwardEps': 'float64',
    'lastSplitFactor': 'str',
    'lastSplitDate': 'Int64',
    'enterpriseToRevenue': 'float64',
    'enterpriseToEbitda': 'float64',
    '52WeekChange': 'float64',
    'SandP52WeekChange': 'float64',
    'lastDividendValue': 'float64',
    'lastDividendDate': 'Int64',
    'quoteType': 'str',
    'currentPrice': 'float64',
    'targetHighPrice': 'float64',
    'targetLowPrice': 'float64',
    'targetMeanPrice': 'float64',
    'targetMedianPrice': 'float64',
    'recommendationMean': 'float64',
    'recommendationKey': 'str',
    'numberOfAnalystOpinions': 'Int64',
    'totalCash': 'Int64',
    'totalCashPerShare': 'float64',
    'ebitda': 'Int64',
    'totalDebt': 'Int64',
    'quickRatio': 'float64',
    'currentRatio': 'float64',
    'totalRevenue': 'Int64',
    'debtToEquity': 'float64',
    'revenuePerShare': 'float64',
    'returnOnAssets': 'float64',
    'returnOnEquity': 'float64',
    'grossProfits': 'Int64',
    'freeCashflow': 'Int64',
    'operatingCashflow': 'Int64',
    'earningsGrowth': 'float64',
    'revenueGrowth': 'float64',
    'grossMargins': 'float64',
    'ebitdaMargins': 'float64',
    'operatingMargins': 'float64',
    'financialCurrency': 'str',
    'symbol': 'str',
    'language': 'str',
    'region': 'str',
    'typeDisp': 'str',
    'quoteSourceName': 'str',
    'triggerable': 'bool',
    'customPriceAlertConfidence': 'str',
    'postMarketTime': 'Int64',
    'regularMarketTime': 'Int64',
    'exchange': 'str',
    'messageBoardId': 'str',
    'exchangeTimezoneName': 'str',
    'exchangeTimezoneShortName': 'str',
    'gmtOffSetMilliseconds': 'Int64',
    'market': 'str',
    'esgPopulated': 'bool',
    'regularMarketChangePercent': 'float64',
    'regularMarketPrice': 'float64',
    'cryptoTradeable': 'bool',
    'earningsTimestamp': 'Int64',
    'earningsTimestampStart': 'Int64',
    'earningsTimestampEnd': 'Int64',
    'earningsCallTimestampStart': 'Int64',
    'earningsCallTimestampEnd': 'Int64',
    'isEarningsDateEstimate': 'bool',
    'epsTrailingTwelveMonths': 'float64',
    'epsForward': 'float64',
    'epsCurrentYear': 'float64',
    'priceEpsCurrentYear': 'float64',
    'fiftyDayAverageChange': 'float64',
    'fiftyDayAverageChangePercent': 'float64',
    'twoHundredDayAverageChange': 'float64',
    'twoHundredDayAverageChangePercent': 'float64',
    'sourceInterval': 'Int64',
    'exchangeDataDelayedBy': 'Int64',
    'averageAnalystRating': 'str',
    'hasPrePostMarketData': 'bool',
    'firstTradeDateMilliseconds': 'Int64',
    'postMarketChangePercent': 'float64',
    'postMarketPrice': 'float64',
    'postMarketChange': 'float64',
    'regularMarketChange': 'float64',
    'regularMarketDayRange': 'str',
    'fullExchangeName': 'str',
    'averageDailyVolume3Month': 'Int64',
    'fiftyTwoWeekLowChange': 'float64',
    'fiftyTwoWeekLowChangePercent': 'float64',
    'fiftyTwoWeekRange': 'str',
    'fiftyTwoWeekHighChange': 'float64',
    'fiftyTwoWeekHighChangePercent': 'float64',
    'fiftyTwoWeekChangePercent': 'float64',
    'dividendDate': 'Int64',
    'shortName': 'str',
    'longName': 'str',
    'corporateActions': 'object',
    'marketState': 'str',
    'displayName': 'str',
    'trailingPegRatio': 'float64'
}

def export_to_csv(data_dir: Path):

    try:
        with open(f"{data_dir / 'financials.json'}", "r") as f:
            print(f"Loading data from {data_dir / 'financials.json'}...")
            data = json.load(f)

            for company in data:
                
                ticker = company.get("ticker", "N/A")

                print(f"Processing data for {ticker}...")

                balance_sheet = pd.DataFrame(company.get('balance_sheet', {}))
                income_statement = pd.DataFrame(company.get('income_statement', {}))
                cash_flow_statement = pd.DataFrame(company.get('cash_flow_statement', {}))
                company_info = pd.DataFrame([
                    {k: v for k, v in company.get('company_info', {}).items() if k != 'companyOfficers'}
                ])  

                balance_sheet.insert(0, 'ticker', ticker)
                income_statement.insert(0, 'ticker', ticker)
                cash_flow_statement.insert(0, 'ticker', ticker)
                company_info.insert(0, 'ticker', ticker)

                # Apply dtypes to balance_sheet
                for col, typ in balance_sheet_dtype.items():
                    if col in balance_sheet.columns:
                        if typ == "float64":
                            balance_sheet[col] = pd.to_numeric(balance_sheet[col], errors='coerce')
                        elif typ == "Int64":
                            balance_sheet[col] = pd.to_numeric(balance_sheet[col], errors='coerce').astype('Int64')

                # Apply dtypes to income_statement
                for col, typ in income_statement_dtype.items():
                    if col in income_statement.columns:
                        if typ == "float64":
                            income_statement[col] = pd.to_numeric(income_statement[col], errors='coerce')
                        elif typ == "Int64":
                            income_statement[col] = pd.to_numeric(income_statement[col], errors='coerce').astype('Int64')

                # Apply dtypes to cash_flow_statement
                for col, typ in cash_flow_statement_dtype.items():
                    if col in cash_flow_statement.columns:
                        if typ == "float64":
                            cash_flow_statement[col] = pd.to_numeric(cash_flow_statement[col], errors='coerce')
                        elif typ == "Int64":
                            cash_flow_statement[col] = pd.to_numeric(cash_flow_statement[col], errors='coerce').astype('Int64')

                # Apply dtypes to company_info
                for col, typ in company_info_dtype.items():
                    if col in company_info.columns:
                        if typ == "float64":
                            company_info[col] = pd.to_numeric(company_info[col], errors='coerce')
                        elif typ == "Int64":
                            company_info[col] = pd.to_numeric(company_info[col], errors='coerce').astype('Int64')

                # Reset index to create report_date column
                balance_sheet = balance_sheet.reset_index().rename(columns={'index': 'report_date'})
                balance_sheet['report_date'] = pd.to_datetime(balance_sheet['report_date'])
                
                income_statement = income_statement.reset_index().rename(columns={'index': 'report_date'})
                income_statement['report_date'] = pd.to_datetime(income_statement['report_date'])
                
                cash_flow_statement = cash_flow_statement.reset_index().rename(columns={'index': 'report_date'})
                cash_flow_statement['report_date'] = pd.to_datetime(cash_flow_statement['report_date'])
                
                if not company_info.empty:
                    company_info = company_info.reset_index().rename(columns={'index': 'report_date'})
                    company_info['report_date'] = pd.to_datetime(company_info['report_date'])

                print(f"Exporting data for {ticker} to CSV...")

                balance_sheet.to_csv(f"{data_dir/'balance_sheet.csv'}", index=False, sep='\t', mode='a', header=not (data_dir / 'balance_sheet.csv').exists())
                income_statement.to_csv(f"{data_dir/'income_statement.csv'}", index=False, sep='\t', mode='a', header=not (data_dir / 'income_statement.csv').exists())
                cash_flow_statement.to_csv(f"{data_dir/'cash_flow_statement.csv'}", index=False, sep='\t', mode='a', header=not (data_dir / 'cash_flow_statement.csv').exists())
                company_info.to_csv(f"{data_dir/'company_info.csv'}", index=False, sep='\t', mode='a', header=not (data_dir / 'company_info.csv').exists())

                print(f"Data for {ticker} exported successfully.")
                print("")

    except Exception as ex:
        print(f"Error loading financials.json: {ex}")

    print("Data export to CSV completed successfully.")


def main():
    p = argparse.ArgumentParser(description="Export financial data to CSV files. Each company's data will be appended to the respective CSV files and saved in the same location of the original JSON file.")
    p.add_argument("--source", default="financials.json", help="Source JSON file")
    
    args = p.parse_args()
    source_path = Path(args.source)

    print(f"Starting data export from {source_path}...")

    if not source_path.exists():
        raise FileNotFoundError(f"Source file not found: {source_path}")

    export_to_csv(source_path)


if __name__ == "__main__":
    main()
    
