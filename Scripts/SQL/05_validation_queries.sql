-- ============================================
-- PROJECT 4: Enterprise Data Integration Pipeline
-- File: 05_validation_queries.sql
-- Purpose: Validate that all tables loaded correctly
--          and that data quality is maintained
--          across raw, clean, and integrated layers
-- ============================================


-- --------------------------------------------
-- 1. RAW TABLE ROW COUNTS
-- Confirms raw data was uploaded and loaded correctly
-- --------------------------------------------

SELECT COUNT(*) AS hr_raw_row_count
FROM project4_enterprise_integration.hr_raw;

SELECT COUNT(*) AS it_raw_row_count
FROM project4_enterprise_integration.it_raw;


-- --------------------------------------------
-- 2. CLEAN TABLE ROW COUNTS
-- Confirms validation reduced row counts correctly
-- HR:  19 raw -> 16 clean (3 quarantined)
-- IT:  24 raw -> 21 clean (3 quarantined)
-- --------------------------------------------

SELECT COUNT(*) AS hr_clean_row_count
FROM project4_enterprise_integration.hr_clean;

SELECT COUNT(*) AS it_clean_row_count
FROM project4_enterprise_integration.it_clean;


-- --------------------------------------------
-- 3. INTEGRATED TABLE ROW COUNT
-- Confirms integration produced expected output
-- IT clean: 21 records -> 20 integrated, 1 quarantined
-- --------------------------------------------

SELECT COUNT(*) AS integrated_row_count
FROM project4_enterprise_integration.integrated_department_assets;


-- --------------------------------------------
-- 4. CHECK FOR NULL EMPLOYEE IDs IN INTEGRATED TABLE
-- Expected: 0 rows
-- Every integrated record must have a valid employee
-- --------------------------------------------

SELECT COUNT(*) AS null_employee_id_count
FROM project4_enterprise_integration.integrated_department_assets
WHERE employee_id IS NULL
   OR TRIM(employee_id) = '';


-- --------------------------------------------
-- 5. CHECK FOR NULL ASSET IDs IN INTEGRATED TABLE
-- Expected: 0 rows
-- Every integrated record must have a valid asset
-- --------------------------------------------

SELECT COUNT(*) AS null_asset_id_count
FROM project4_enterprise_integration.integrated_department_assets
WHERE asset_id IS NULL
   OR TRIM(asset_id) = '';


-- --------------------------------------------
-- 6. CONFIRM DISTINCT DEPARTMENTS IN INTEGRATED TABLE
-- Used to verify department values look correct
-- --------------------------------------------

SELECT DISTINCT department
FROM project4_enterprise_integration.integrated_department_assets
ORDER BY department;


-- --------------------------------------------
-- 7. CONFIRM DISTINCT ASSET TYPES IN INTEGRATED TABLE
-- Used to verify asset_type values look correct
-- --------------------------------------------

SELECT DISTINCT asset_type
FROM project4_enterprise_integration.integrated_department_assets
ORDER BY asset_type;


-- --------------------------------------------
-- 8. CHECK FOR RECORDS WITHOUT BUDGET DATA
-- These are valid records where Finance had no
-- matching department — expected for some rows
-- --------------------------------------------

SELECT COUNT(*) AS records_without_budget
FROM project4_enterprise_integration.integrated_department_assets
WHERE cost_center_id IS NULL;
