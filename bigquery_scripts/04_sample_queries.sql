-- BigQuery SQL - Additional Useful Queries
-- This file contains sample queries for common analytics tasks

-- Query 1: Compare Key Metrics Across Companies
SELECT
  ticker,
  report_date,
  Total_Revenue,
  Net_Income,
  EBITDA,
  Diluted_EPS,
  Total_Assets,
  Stockholders_Equity,
  Total_Debt
FROM `{project_id}.{dataset_id}.company_snapshot_latest`
ORDER BY Total_Revenue DESC;

-- Query 2: Profitability Analysis - Margin Trends
SELECT
  ticker,
  report_date,
  gross_profit_margin_pct,
  operating_margin_pct,
  SAFE_DIVIDE(Net_Income, Total_Revenue) * 100 AS net_margin_pct,
  Operating_Income,
  Net_Income
FROM `{project_id}.{dataset_id}.income_statement_trends`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY ticker, report_date DESC;

-- Query 3: Liquidity Analysis
SELECT
  ticker,
  report_date,
  current_ratio,
  quick_ratio,
  SAFE_DIVIDE(Cash_And_Cash_Equivalents, Current_Liabilities) AS cash_ratio,
  Working_Capital,
  Current_Assets,
  Current_Liabilities
FROM `{project_id}.{dataset_id}.balance_sheet_trends`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
ORDER BY ticker, report_date DESC;

-- Query 4: Leverage and Solvency Ratios
SELECT
  ticker,
  report_date,
  debt_to_equity,
  debt_to_assets,
  interest_coverage_ratio,
  Total_Debt,
  Total_Assets,
  EBIT,
  Interest_Expense,
  Stockholders_Equity
FROM `{project_id}.{dataset_id}.financial_ratios`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
ORDER BY ticker, report_date DESC;

-- Query 5: Efficiency and Profitability Ratios
SELECT
  ticker,
  report_date,
  asset_turnover,
  equity_multiplier,
  roa,
  roe,
  profit_margin,
  Total_Revenue,
  Net_Income,
  Total_Assets,
  Stockholders_Equity
FROM `{project_id}.{dataset_id}.financial_ratios`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
ORDER BY ticker, report_date DESC;

-- Query 6: Revenue and Growth Analysis
SELECT
  ticker,
  report_date,
  Total_Revenue,
  revenue_growth_pct,
  Gross_Profit,
  gross_profit_margin_pct,
  Cost_Of_Revenue,
  SAFE_DIVIDE(Cost_Of_Revenue, Total_Revenue) * 100 AS cogs_ratio
FROM `{project_id}.{dataset_id}.income_statement_trends`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
ORDER BY ticker, report_date DESC;

-- Query 7: Operating Expense Analysis
SELECT
  ticker,
  report_date,
  Total_Revenue,
  Cost_Of_Revenue,
  Research_And_Development,
  Selling_General_And_Administration,
  Operating_Expense,
  SAFE_DIVIDE(Research_And_Development, Total_Revenue) * 100 AS rd_ratio,
  SAFE_DIVIDE(Selling_General_And_Administration, Total_Revenue) * 100 AS sga_ratio
FROM `{project_id}.{dataset_id}.income_statement_trends`
ORDER BY ticker, report_date DESC
LIMIT 100;

-- Query 8: Tax Analysis
SELECT
  ticker,
  report_date,
  Pretax_Income,
  Tax_Provision,
  Net_Income,
  SAFE_DIVIDE(Tax_Provision, Pretax_Income) * 100 AS effective_tax_rate,
  SAFE_DIVIDE(Net_Income, Pretax_Income) AS after_tax_rate
FROM `{project_id}.{dataset_id}.income_statement`
WHERE Pretax_Income > 0
ORDER BY ticker, report_date DESC;

-- Query 9: Capital Structure Analysis
SELECT
  ticker,
  report_date,
  Total_Assets,
  Current_Assets,
  Cash_And_Cash_Equivalents,
  Net_PPE,
  Goodwill_And_Other_Intangible_Assets,
  Stockholders_Equity,
  Total_Liabilities_Net_Minority_Interest,
  SAFE_DIVIDE(Cash_And_Cash_Equivalents, Total_Assets) * 100 AS cash_to_assets,
  SAFE_DIVIDE(Net_PPE, Total_Assets) * 100 AS ppe_to_assets,
  SAFE_DIVIDE(Goodwill_And_Other_Intangible_Assets, Total_Assets) * 100 AS intangibles_to_assets
FROM `{project_id}.{dataset_id}.balance_sheet_trends`
ORDER BY ticker, report_date DESC;

-- Query 10: Dividend and Shareholder Analysis (requires enhanced data)
SELECT
  ticker,
  report_date,
  Net_Income,
  Diluted_Average_Shares,
  Diluted_EPS,
  Basic_EPS,
  Diluted_NI_Availto_Com_Stockholders
FROM `{project_id}.{dataset_id}.income_statement`
ORDER BY ticker, report_date DESC;

-- Query 11: Quarterly Trend - Last 8 Quarters
SELECT
  ticker,
  report_date,
  Total_Revenue,
  Gross_Profit,
  Operating_Income,
  Net_Income,
  EBITDA,
  SAFE_DIVIDE(Gross_Profit, Total_Revenue) * 100 AS gross_margin,
  SAFE_DIVIDE(Operating_Income, Total_Revenue) * 100 AS operating_margin,
  SAFE_DIVIDE(Net_Income, Total_Revenue) * 100 AS net_margin,
  SAFE_DIVIDE(EBITDA, Total_Revenue) * 100 AS ebitda_margin
FROM `{project_id}.{dataset_id}.income_statement_trends`
WHERE report_rank <= 8
ORDER BY ticker, report_date DESC;

-- Query 12: Peer Comparison - Latest Quarter
WITH latest_data AS (
  SELECT
    ticker,
    Total_Revenue,
    Net_Income,
    EBITDA,
    Total_Assets,
    Stockholders_Equity,
    Total_Debt,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS rn
  FROM `{project_id}.{dataset_id}.balance_sheet` b
  JOIN `{project_id}.{dataset_id}.income_statement` i
    ON b.ticker = i.ticker
    AND b.report_date = i.report_date
)
SELECT
  ticker,
  Total_Revenue,
  Net_Income,
  EBITDA,
  Total_Assets,
  SAFE_DIVIDE(Net_Income, Total_Revenue) * 100 AS net_margin,
  SAFE_DIVIDE(Net_Income, Total_Assets) * 100 AS roa,
  SAFE_DIVIDE(Total_Debt, Stockholders_Equity) AS debt_to_equity,
  ROW_NUMBER() OVER (ORDER BY Total_Revenue DESC) AS revenue_rank,
  ROW_NUMBER() OVER (ORDER BY Net_Income DESC) AS net_income_rank
FROM latest_data
WHERE rn = 1
ORDER BY Total_Revenue DESC;

-- Query 13: Growth Comparison - QoQ vs YoY
SELECT
  ticker,
  current_date,
  current_revenue,
  prior_revenue,
  revenue_yoy_growth_pct,
  current_net_income,
  prior_net_income,
  net_income_yoy_growth_pct,
  current_diluted_eps,
  prior_diluted_eps,
  eps_yoy_growth_pct
FROM `{project_id}.{dataset_id}.yoy_comparison`
WHERE current_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
ORDER BY ticker, current_date DESC;

-- Query 14: Cash Flow Strength (if cash flow table is populated)
SELECT
  b.ticker,
  b.report_date,
  i.Net_Income,
  b.Cash_And_Cash_Equivalents,
  b.Total_Debt,
  SAFE_DIVIDE(b.Cash_And_Cash_Equivalents, b.Total_Debt) AS cash_to_debt_ratio,
  SAFE_DIVIDE(b.Cash_And_Cash_Equivalents, i.Net_Income) AS cash_to_earnings_ratio
FROM `{project_id}.{dataset_id}.balance_sheet` b
JOIN `{project_id}.{dataset_id}.income_statement` i
  ON b.ticker = i.ticker
  AND b.report_date = i.report_date
WHERE b.Cash_And_Cash_Equivalents > 0
  AND i.Net_Income > 0
ORDER BY b.ticker, b.report_date DESC;

-- Query 15: Financial Health Score (Composite)
SELECT
  ticker,
  report_date,
  -- Normalize scores to 0-100
  ROUND(
    (LEAST(current_ratio, 2) / 2 * 20) +  -- Liquidity (20%)
    (LEAST(1 / GREATEST(debt_to_equity, 0.01), 1) * 20) +  -- Leverage (20%)
    (LEAST(roe / 100, 1) * 20) +  -- Profitability (20%)
    (LEAST(profit_margin / 0.2, 1) * 20) +  -- Margin (20%)
    20,  -- Base score
    2
  ) AS financial_health_score
FROM `{project_id}.{dataset_id}.financial_ratios`
WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
ORDER BY ticker, report_date DESC;
