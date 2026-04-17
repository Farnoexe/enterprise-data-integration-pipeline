-- ============================================
-- PROJECT 4: Enterprise Data Integration Pipeline
-- File: 03_create_clean_tables.sql
-- Purpose: Create external tables over validated
--          and cleaned source data stored in S3
-- Note: Clean tables contain only records that
--       passed source-level validation checks.
-- ============================================


-- --------------------------------------------
-- Clean HR Table
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.hr_clean (
    employee_id     STRING,
    employee_name   STRING,
    department      STRING,
    location        STRING,
    manager_id      STRING,
    hire_date       STRING,
    status          STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/clean/hr/'
TBLPROPERTIES ('skip.header.line.count' = '1');


-- --------------------------------------------
-- Clean Finance Table
-- Note: Finance data originates as a JSON-based
-- API response. During validation it is persisted
-- as CSV for consistency across all pipeline layers
-- and to support querying in Athena.
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.finance_clean (
    cost_center_id  STRING,
    department      STRING,
    monthly_budget  DOUBLE,
    currency        STRING,
    budget_month    STRING,
    last_updated    STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/clean/finance/'
TBLPROPERTIES ('skip.header.line.count' = '1');


-- --------------------------------------------
-- Clean IT Table
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.it_clean (
    asset_id        STRING,
    employee_id     STRING,
    asset_type      STRING,
    assigned_date   STRING,
    status          STRING,
    location        STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/clean/it/'
TBLPROPERTIES ('skip.header.line.count' = '1');
