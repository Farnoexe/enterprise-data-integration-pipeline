-- ============================================
-- PROJECT 4: Enterprise Data Integration Pipeline
-- File: 04_create_integrated_table.sql
-- Purpose: Create external table over the final
--          integrated output stored in S3
-- Note: This is the primary analytics table.
--       It joins HR, IT, and Finance into one
--       analytics-ready dataset.
-- ============================================


CREATE EXTERNAL TABLE project4_enterprise_integration.integrated_department_assets (
    employee_id         STRING,
    employee_name       STRING,
    department          STRING,
    employee_location   STRING,
    employee_status     STRING,
    asset_id            STRING,
    asset_type          STRING,
    asset_status        STRING,
    asset_location      STRING,
    cost_center_id      STRING,
    monthly_budget      DOUBLE,
    currency            STRING,
    budget_month        STRING,
    record_source_date  STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar'     = '"'
)
STORED AS TEXTFILE
LOCATION 's3://metroville-traffic-analytics/project4-enterprise-integration/integrated/'
TBLPROPERTIES ('skip.header.line.count' = '1');
