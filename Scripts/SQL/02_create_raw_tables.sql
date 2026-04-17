-- ============================================
-- PROJECT 4: Enterprise Data Integration Pipeline
-- File: 02_create_raw_tables.sql
-- Purpose: Create external tables over raw source
--          data stored in S3
-- Note: Raw data is preserved exactly as ingested.
--       These tables are for inspection only.
-- ============================================


-- --------------------------------------------
-- Raw HR Table
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.hr_raw (
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
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/raw/hr/'
TBLPROPERTIES ('skip.header.line.count' = '1');


-- --------------------------------------------
-- Raw Finance Table
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.finance_raw (
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
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/raw/finance/'
TBLPROPERTIES ('skip.header.line.count' = '1');


-- --------------------------------------------
-- Raw IT Table
-- --------------------------------------------
CREATE EXTERNAL TABLE project4_enterprise_integration.it_raw (
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
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/raw/it/'
TBLPROPERTIES ('skip.header.line.count' = '1');
