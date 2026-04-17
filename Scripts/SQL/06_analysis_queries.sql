-- ============================================
-- PROJECT 4: Enterprise Data Integration Pipeline
-- File: 06_analysis_queries.sql
-- Purpose: Generate business insights from the
--          integrated department assets dataset
-- Each query includes a business decision context
-- to support operational and cost decisions
-- ============================================


-- --------------------------------------------
-- 1. TOTAL ASSETS PER DEPARTMENT
-- Business decision: Identify over-provisioned
-- departments and reallocate surplus assets before
-- approving new hardware purchase requests.
-- Departments with disproportionately high asset
-- counts relative to headcount are candidates for
-- asset reclamation.
-- --------------------------------------------

SELECT
    department,
    COUNT(asset_id) AS total_assets
FROM project4_enterprise_integration.integrated_department_assets
GROUP BY department
ORDER BY total_assets DESC;


-- --------------------------------------------
-- 2. ASSET TYPE DISTRIBUTION ACROSS THE COMPANY
-- Business decision: Use distribution data to
-- negotiate volume discounts with hardware vendors.
-- If laptops represent 60%+ of all assets, a
-- bulk procurement agreement can reduce unit cost
-- significantly compared to ad hoc purchasing.
-- --------------------------------------------

SELECT
    asset_type,
    COUNT(*) AS total_assigned
FROM project4_enterprise_integration.integrated_department_assets
GROUP BY asset_type
ORDER BY total_assigned DESC;


-- --------------------------------------------
-- 3. MONTHLY BUDGET PER DEPARTMENT
-- Business decision: Compare department budgets
-- against headcount and asset counts to identify
-- where spend may be misaligned. Departments with
-- high budgets and low headcount may have room for
-- reallocation to faster-growing teams.
-- --------------------------------------------

SELECT
    department,
    cost_center_id,
    monthly_budget,
    currency,
    budget_month
FROM project4_enterprise_integration.integrated_department_assets
WHERE cost_center_id IS NOT NULL
GROUP BY department, cost_center_id, monthly_budget, currency, budget_month
ORDER BY monthly_budget DESC;


-- --------------------------------------------
-- 4. HEADCOUNT VS ASSET COUNT PER DEPARTMENT
-- Business decision: Calculate assets per employee
-- to set a company-wide provisioning standard.
-- Departments significantly above the average ratio
-- are candidates for an asset audit before any new
-- procurement is approved.
-- --------------------------------------------

SELECT
    department,
    COUNT(DISTINCT employee_id) AS employee_count,
    COUNT(asset_id) AS total_assets,
    ROUND(
        CAST(COUNT(asset_id) AS DOUBLE) / NULLIF(COUNT(DISTINCT employee_id), 0),
        2
    ) AS assets_per_employee
FROM project4_enterprise_integration.integrated_department_assets
GROUP BY department
ORDER BY assets_per_employee DESC;


-- --------------------------------------------
-- 5. EMPLOYEES WITH MULTIPLE ASSETS
-- Business decision: Review employees with 3 or
-- more assets as a priority for asset reclamation.
-- Unused secondary devices can be reassigned to
-- new hires, reducing hardware procurement costs
-- for onboarding.
-- --------------------------------------------

SELECT
    employee_id,
    employee_name,
    department,
    COUNT(asset_id) AS asset_count
FROM project4_enterprise_integration.integrated_department_assets
GROUP BY employee_id, employee_name, department
HAVING COUNT(asset_id) > 1
ORDER BY asset_count DESC;


-- --------------------------------------------
-- 6. ASSETS ASSIGNED TO INACTIVE EMPLOYEES
-- Business decision: This is an immediate cost
-- saving opportunity. Assets held by inactive
-- employees should be reclaimed and returned to
-- inventory. Procuring new hardware while active
-- assets sit unrecovered is a direct, avoidable cost.
-- --------------------------------------------

SELECT
    employee_id,
    employee_name,
    department,
    employee_status,
    asset_id,
    asset_type,
    asset_status
FROM project4_enterprise_integration.integrated_department_assets
WHERE employee_status = 'inactive'
ORDER BY department, employee_id;


-- --------------------------------------------
-- 7. ASSETS PER OFFICE LOCATION
-- Business decision: Use location-level asset data
-- to balance inventory across offices. Locations
-- with surplus assets can supply new hires at other
-- locations before triggering a central procurement
-- request, reducing lead time and shipping costs.
-- --------------------------------------------

SELECT
    asset_location,
    COUNT(asset_id) AS total_assets
FROM project4_enterprise_integration.integrated_department_assets
GROUP BY asset_location
ORDER BY total_assets DESC;
