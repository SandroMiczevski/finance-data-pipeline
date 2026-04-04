-- BigQuery SQL - Data Transformations and Analytics Tables
-- This script creates processed tables for analysis

-- Create Financial Ratios Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.financial_ratios` AS
SELECT
  i.report_date,
  i.ticker,
  b.report_date AS balance_sheet_date,
  -- Profitability Ratios
  SAFE_DIVIDE(i.Net_Income, i.Total_Revenue) AS profit_margin,
  SAFE_DIVIDE(i.EBITDA, i.Total_Revenue) AS ebitda_margin,
  SAFE_DIVIDE(i.Operating_Income, i.Total_Revenue) AS operating_margin,
  SAFE_DIVIDE(i.Net_Income, b.Total_Assets) AS roa,
  SAFE_DIVIDE(i.Net_Income, b.Stockholders_Equity) AS roe,
  -- Liquidity Ratios
  SAFE_DIVIDE(b.Current_Assets, b.Current_Liabilities) AS current_ratio,
  SAFE_DIVIDE(b.Current_Assets - b.Inventory, b.Current_Liabilities) AS quick_ratio,
  SAFE_DIVIDE(b.Cash_And_Cash_Equivalents, b.Current_Liabilities) AS cash_ratio,
  -- Leverage Ratios
  SAFE_DIVIDE(b.Total_Debt, b.Total_Assets) AS debt_to_assets,
  SAFE_DIVIDE(b.Total_Debt, b.Stockholders_Equity) AS debt_to_equity,
  SAFE_DIVIDE(i.EBIT, i.Interest_Expense) AS interest_coverage_ratio,
  -- Efficiency Ratios
  SAFE_DIVIDE(i.Total_Revenue, b.Total_Assets) AS asset_turnover,
  SAFE_DIVIDE(i.Total_Revenue, b.Stockholders_Equity) AS equity_multiplier,
  -- Cost Structure
  SAFE_DIVIDE(i.Cost_Of_Revenue, i.Total_Revenue) AS cost_of_goods_sold_ratio,
  SAFE_DIVIDE(i.Operating_Expense, i.Total_Revenue) AS operating_expense_ratio,
  SAFE_DIVIDE(i.Research_And_Development, i.Total_Revenue) AS rd_expense_ratio,
  SAFE_DIVIDE(i.Selling_General_And_Administration, i.Total_Revenue) AS sga_expense_ratio,
  -- EPS and Valuation (from Income Statement)
  i.Diluted_EPS,
  i.Basic_EPS,
  CURRENT_TIMESTAMP() AS calculated_at
FROM `{project_id}.{dataset_id}.income_statement` i
LEFT JOIN `{project_id}.{dataset_id}.balance_sheet` b
  ON i.ticker = b.ticker
  AND i.report_date = b.report_date
WHERE i.Net_Income IS NOT NULL
  AND b.Total_Assets IS NOT NULL;

-- Create Balance Sheet Trend Analysis Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.balance_sheet_trends` AS
SELECT
  ticker,
  report_date,
  Total_Assets,
  LAG(Total_Assets) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_total_assets,
  SAFE_DIVIDE(
    Total_Assets - LAG(Total_Assets) OVER (PARTITION BY ticker ORDER BY report_date),
    LAG(Total_Assets) OVER (PARTITION BY ticker ORDER BY report_date)
  ) * 100 AS total_assets_growth_pct,
  
  Total_Liabilities_Net_Minority_Interest,
  LAG(Total_Liabilities_Net_Minority_Interest) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_total_liabilities,
  
  Stockholders_Equity,
  LAG(Stockholders_Equity) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_stockholders_equity,
  SAFE_DIVIDE(
    Stockholders_Equity - LAG(Stockholders_Equity) OVER (PARTITION BY ticker ORDER BY report_date),
    LAG(Stockholders_Equity) OVER (PARTITION BY ticker ORDER BY report_date)
  ) * 100 AS equity_growth_pct,
  
  Current_Assets,
  Current_Liabilities,
  Working_Capital,
  LAG(Working_Capital) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_working_capital,
  
  Cash_And_Cash_Equivalents,
  Total_Debt,
  Net_Debt,
  RANK() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS report_rank,
  CURRENT_TIMESTAMP() AS calculated_at
FROM `{project_id}.{dataset_id}.balance_sheet`
WHERE Total_Assets IS NOT NULL;

-- Create Income Statement Trend Analysis Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.income_statement_trends` AS
SELECT
  ticker,
  report_date,
  Total_Revenue,
  LAG(Total_Revenue) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_revenue,
  SAFE_DIVIDE(
    Total_Revenue - LAG(Total_Revenue) OVER (PARTITION BY ticker ORDER BY report_date),
    LAG(Total_Revenue) OVER (PARTITION BY ticker ORDER BY report_date)
  ) * 100 AS revenue_growth_pct,
  
  Gross_Profit,
  SAFE_DIVIDE(Gross_Profit, Total_Revenue) * 100 AS gross_profit_margin_pct,
  
  Operating_Income,
  SAFE_DIVIDE(Operating_Income, Total_Revenue) * 100 AS operating_margin_pct,
  
  Net_Income,
  LAG(Net_Income) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_net_income,
  SAFE_DIVIDE(
    Net_Income - LAG(Net_Income) OVER (PARTITION BY ticker ORDER BY report_date),
    LAG(Net_Income) OVER (PARTITION BY ticker ORDER BY report_date)
  ) * 100 AS ni_growth_pct,
  
  EBITDA,
  LAG(EBITDA) OVER (PARTITION BY ticker ORDER BY report_date) AS prev_ebitda,
  
  Tax_Provision,
  SAFE_DIVIDE(Tax_Provision, Pretax_Income) * 100 AS effective_tax_rate_pct,
  
  Cost_Of_Revenue,
  Operating_Expense,
  Research_And_Development,
  Selling_General_And_Administration,
  
  Diluted_EPS,
  Basic_EPS,
  
  RANK() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS report_rank,
  CURRENT_TIMESTAMP() AS calculated_at
FROM `{project_id}.{dataset_id}.income_statement`
WHERE Total_Revenue IS NOT NULL;

-- Create Company Snapshot Table (Latest Quarter)
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.company_snapshot_latest` AS
WITH ranked_income AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS rn
  FROM `{project_id}.{dataset_id}.income_statement`
),
ranked_balance AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS rn
  FROM `{project_id}.{dataset_id}.balance_sheet`
),
ranked_info AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY report_date DESC) AS rn
  FROM `{project_id}.{dataset_id}.company_info`
)
SELECT
  COALESCE(i.ticker, b.ticker, c.ticker) AS ticker,
  COALESCE(i.report_date, b.report_date, c.report_date) AS latest_report_date,
  
  -- Company Info
  c.company,
  c.industry,
  c.sector,
  c.country,
  c.fullTimeEmployees,
  
  -- Income Statement KPIs
  i.Total_Revenue,
  i.Gross_Profit,
  i.Operating_Income,
  i.EBITDA,
  i.Net_Income,
  i.Diluted_EPS,
  i.Tax_Provision,
  
  -- Balance Sheet KPIs
  b.Total_Assets,
  b.Current_Assets,
  b.Current_Liabilities,
  b.Total_Liabilities_Net_Minority_Interest,
  b.Stockholders_Equity,
  b.Cash_And_Cash_Equivalents,
  b.Total_Debt,
  b.Working_Capital,
  
  -- Calculated Metrics
  SAFE_DIVIDE(i.Net_Income, i.Total_Revenue) * 100 AS net_profit_margin_pct,
  SAFE_DIVIDE(i.EBITDA, i.Total_Revenue) * 100 AS ebitda_margin_pct,
  SAFE_DIVIDE(b.Current_Assets, b.Current_Liabilities) AS current_ratio,
  SAFE_DIVIDE(b.Total_Debt, b.Stockholders_Equity) AS debt_to_equity,
  SAFE_DIVIDE(i.Net_Income, b.Total_Assets) AS roa_pct,
  SAFE_DIVIDE(i.Net_Income, b.Stockholders_Equity) AS roe_pct,
  
  CURRENT_TIMESTAMP() AS snapshot_at
FROM ranked_income i
FULL OUTER JOIN ranked_balance b
  ON i.ticker = b.ticker AND i.rn = 1 AND b.rn = 1
FULL OUTER JOIN ranked_info c
  ON COALESCE(i.ticker, b.ticker) = c.ticker AND c.rn = 1
WHERE COALESCE(i.rn, b.rn, c.rn) = 1;

-- Create Year-over-Year Comparison Table
CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.yoy_comparison` AS
SELECT
  curr.ticker,
  curr.report_date AS current_date,
  PREV.report_date AS prior_year_date,
  
  curr.Total_Revenue AS current_revenue,
  PREV.Total_Revenue AS prior_revenue,
  SAFE_DIVIDE(curr.Total_Revenue - PREV.Total_Revenue, PREV.Total_Revenue) * 100 AS revenue_yoy_growth_pct,
  
  curr.Gross_Profit AS current_gross_profit,
  PREV.Gross_Profit AS prior_gross_profit,
  SAFE_DIVIDE(curr.Gross_Profit - PREV.Gross_Profit, PREV.Gross_Profit) * 100 AS gross_profit_yoy_growth_pct,
  
  curr.Net_Income AS current_net_income,
  PREV.Net_Income AS prior_net_income,
  SAFE_DIVIDE(curr.Net_Income - PREV.Net_Income, PREV.Net_Income) * 100 AS net_income_yoy_growth_pct,
  
  curr.EBITDA AS current_ebitda,
  PREV.EBITDA AS prior_ebitda,
  
  curr.Diluted_EPS AS current_diluted_eps,
  PREV.Diluted_EPS AS prior_diluted_eps,
  SAFE_DIVIDE(curr.Diluted_EPS - PREV.Diluted_EPS, PREV.Diluted_EPS) * 100 AS eps_yoy_growth_pct,
  
  CURRENT_TIMESTAMP() AS calculated_at
FROM `{project_id}.{dataset_id}.income_statement` curr
LEFT JOIN `{project_id}.{dataset_id}.income_statement` PREV
  ON curr.ticker = PREV.ticker
  AND EXTRACT(YEAR FROM curr.report_date) = EXTRACT(YEAR FROM PREV.report_date) + 1
  AND EXTRACT(QUARTER FROM curr.report_date) = EXTRACT(QUARTER FROM PREV.report_date)
WHERE curr.Total_Revenue IS NOT NULL
  AND PREV.Total_Revenue IS NOT NULL
ORDER BY curr.ticker, curr.report_date DESC;
