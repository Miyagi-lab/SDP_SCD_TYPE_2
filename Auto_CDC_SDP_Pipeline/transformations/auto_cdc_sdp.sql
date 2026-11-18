CREATE OR REFRESH STREAMING TABLE workspace.default.customer_bronze(

CONSTRAINT valid_customer_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT(operation IN ('new','updated', 'deleted'))  ON VIOLATION DROP ROW,
CONSTRAINT valid_email EXPECT(operation = 'deleted' OR (email IS NOT NULL AND email LIKE '%@%.%'))
)
COMMENT "Raw daily custmer CDC events from databricks_stimulated_retail_customer_data"
AS
SELECT
    city,
    customer_id,
    email,
    first_name,
    last_name,
    loyalty_tier,
    operation,
    CAST(signup_date AS Date) AS signup_date,
    source_subsidiary,
    CAST(timestamp AS TIMESTAMP) AS event_timestamp,
    current_timestamp() as ingested_at
FROM STREAM read_files(
    '/Volumes/databricks_simulated_retail_customer_data/v02/customer_changes_daily',
    format => 'json'
);


-- Silver Target: Declare the SCD Type 2 streaming table ---

-- SCD Type 2 customer dimension with full historical tracking
CREATE OR REFRESH STREAMING TABLE workspace.default.customer_silver;

-- Apply AUTO CDC with SCD Type 2

CREATE FLOW customer_scd2_flow AS
AUTO CDC INTO workspace.default.customer_silver
FROM STREAM(workspace.default.customer_bronze)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'delete'
SEQUENCE BY event_timestamp
COLUMNS * EXCEPT (operation, event_timestamp, ingested_at)
STORED AS SCD TYPE 2;


-- ================================================================================
-- Gold materialised views (they auto refresh automatically)
-- ===============================================================================
CREATE OR REFRESH MATERIALIZED VIEW workspace.default.gold_active_customers
COMMENT "Power BI Dimension: Currently active customers"
AS
SELECT
xxhash64(customer_id, __START_AT) AS customer_sk,
customer_id,
concat_ws(' ', first_name, last_name) AS customer_name,
email,
city,
loyalty_tier,
source_subsidiary,
signup_date,
__START_AT AS effective_from
FROM workspace.default.customer_silver
WHERE __END_AT IS NULL;

-- ================================================================================
-- Gold view 2 : failed customers
--=================================================================================
CREATE OR REFRESH MATERIALIZED VIEW workspace.default.gold_failed_customers
COMMENT "Power BI Dimension: Failed customers"
AS
SELECT
customer_id,
first_name,
last_name,
email,
city,
loyalty_tier,
operation,
source_subsidiary,
signup_date,
event_timestamp,
ingested_at,
CASE 
    WHEN operation NOT IN ('new','updated','deleted') OR operation IS NULL THEN 'invalid_operation'
    WHEN operation != 'deleted' AND (email IS NULL OR email NOT LIKE '%@%.%') THEN 'invalid_email'
    ELSE 'Other Validation Failure'
END AS failure_reason
FROM workspace.default.customer_bronze
WHERE operation NOT IN ('new','updated','deleted')
OR operation IS NULL
OR (operation != 'deleted' AND (email IS NULL OR email NOT LIKE '%@%.%'));