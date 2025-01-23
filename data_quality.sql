-- Step 1: Drop Existing Database and Schema
DROP TABLE IF EXISTS consumer_profiles;
DROP TABLE IF EXISTS product_brands;
DROP TABLE IF EXISTS transaction_receipts;
DROP TABLE IF EXISTS transaction_items;
DROP TABLE IF EXISTS staging_consumer_profiles;
DROP TABLE IF EXISTS staging_product_brands;
DROP TABLE IF EXISTS staging_transaction_receipts;

DROP SCHEMA IF EXISTS consumer_behavior.fetch_rewards_schema CASCADE;
DROP DATABASE IF EXISTS consumer_behavior;

-- Step 2: Create Database and Schema
CREATE DATABASE consumer_behavior;
CREATE SCHEMA consumer_behavior.fetch_rewards_schema;
USE SCHEMA consumer_behavior.fetch_rewards_schema;

-- Step 3: Create Staging Tables for Raw Data
CREATE OR REPLACE TABLE staging_consumer_profiles (raw_data VARIANT);
CREATE OR REPLACE TABLE staging_product_brands (raw_data VARIANT);
CREATE OR REPLACE TABLE staging_transaction_receipts (raw_data VARIANT);

-- Step 4: Create Structured Tables
-- Consumer Profiles Table (Users)
CREATE OR REPLACE TABLE consumer_profiles (
    user_id STRING PRIMARY KEY, -- Extracted from `_id`
    state STRING,
    created_date TIMESTAMP, -- Extracted from `createdDate`
    last_login TIMESTAMP, -- Extracted from `lastLogin`
    is_active BOOLEAN -- Mapped from `active`
);

-- Product Brands Table
CREATE OR REPLACE TABLE product_brands (
    brand_id STRING PRIMARY KEY, -- Extracted from `_id`
    barcode STRING UNIQUE, -- Unique field
    category STRING,
    category_code STRING, -- Extracted from `categoryCode`
    name STRING,
    top_brand BOOLEAN -- Mapped from `topBrand`
);

-- Transaction Receipts Table
CREATE OR REPLACE TABLE transaction_receipts (
    receipt_id STRING PRIMARY KEY, -- Extracted from `_id`
    user_id STRING REFERENCES consumer_profiles(user_id), -- Foreign key
    bonus_points_awarded INT, -- Mapped from `bonusPointsEarned`
    purchase_date TIMESTAMP, -- Extracted from `purchaseDate`
    total_spent DECIMAL(10, 2), -- Mapped from `totalSpent`
    receipt_status STRING, -- Mapped from `rewardsReceiptStatus`
    create_date TIMESTAMP, -- Extracted from `createDate`
    modify_date TIMESTAMP, -- Extracted from `modifyDate`
    rewards_receipt_items VARIANT -- JSON field for nested item details
);

-- Transaction Items Table
CREATE OR REPLACE TABLE transaction_items (
    item_id STRING PRIMARY KEY, -- Generated UUID
    receipt_id STRING REFERENCES transaction_receipts(receipt_id), -- Foreign key
    barcode STRING REFERENCES product_brands(barcode), -- Foreign key
    description STRING, -- Mapped from `description`
    quantity INT, -- Mapped from `quantityPurchased`
    final_price DECIMAL(10, 2), -- Mapped from `finalPrice`
    original_price DECIMAL(10, 2), -- Mapped from `itemPrice`
    needs_fetch_review BOOLEAN -- Mapped from `needsFetchReview`
);



-- Step 1: Create a Stage to Load Files
CREATE OR REPLACE STAGE consumer_behavior_stage
FILE_FORMAT = (TYPE = JSON);

-- Step 2: Load Data into Staging Tables
-- Load Users Data
COPY INTO staging_consumer_profiles
FROM @consumer_behavior_stage/users.json
FILE_FORMAT = (TYPE = JSON);

-- Load Brands Data
COPY INTO staging_product_brands
FROM @consumer_behavior_stage/brands.json
FILE_FORMAT = (TYPE = JSON);

-- Load Receipts Data
COPY INTO staging_transaction_receipts
FROM @consumer_behavior_stage/receipts.json
FILE_FORMAT = (TYPE = JSON);


INSERT INTO consumer_profiles (user_id, state, created_date, last_login, is_active)
SELECT DISTINCT
    raw_data:_id::STRING AS user_id, -- Extract user_id from `_id`
    raw_data:state::STRING AS state, -- Extract state
    DATEADD(SECOND, TRY_CAST(raw_data:createdDate:"$date"::STRING AS NUMBER) / 1000, TIMESTAMP '1970-01-01 00:00:00') AS created_date, -- Convert `createdDate` to TIMESTAMP
    DATEADD(SECOND, TRY_CAST(raw_data:lastLogin:"$date"::STRING AS NUMBER) / 1000, TIMESTAMP '1970-01-01 00:00:00') AS last_login, -- Convert `lastLogin` to TIMESTAMP
    raw_data:active::BOOLEAN AS is_active -- Extract `active` as BOOLEAN
FROM staging_consumer_profiles
WHERE raw_data:_id IS NOT NULL; -- Ensure `_id` exists




INSERT INTO product_brands (brand_id, barcode, category, category_code, name, top_brand)
SELECT DISTINCT
    raw_data:_id::STRING AS brand_id, -- Extract brand_id from `_id`
    raw_data:barcode::STRING AS barcode, -- Extract barcode
    raw_data:category::STRING AS category, -- Extract category
    raw_data:categoryCode::STRING AS category_code, -- Extract categoryCode
    raw_data:name::STRING AS name, -- Extract name
    raw_data:topBrand::BOOLEAN AS top_brand -- Extract `topBrand` as BOOLEAN
FROM staging_product_brands
WHERE raw_data:_id IS NOT NULL; -- Ensure `_id` exists



INSERT INTO transaction_receipts (receipt_id, user_id, bonus_points_awarded, purchase_date, total_spent, receipt_status, create_date, modify_date, rewards_receipt_items)
SELECT DISTINCT
    raw_data:_id::STRING AS receipt_id, -- Extract receipt_id from `_id`
    raw_data:userId::STRING AS user_id, -- Extract user_id
    raw_data:bonusPointsEarned::INT AS bonus_points_awarded, -- Extract bonusPointsEarned
    DATEADD(SECOND, TRY_CAST(raw_data:purchaseDate:"$date"::STRING AS NUMBER) / 1000, TIMESTAMP '1970-01-01 00:00:00') AS purchase_date, -- Convert `purchaseDate` to TIMESTAMP
    raw_data:totalSpent::DECIMAL(10, 2) AS total_spent, -- Extract totalSpent
    raw_data:rewardsReceiptStatus::STRING AS receipt_status, -- Extract rewardsReceiptStatus
    DATEADD(SECOND, TRY_CAST(raw_data:createDate:"$date"::STRING AS NUMBER) / 1000, TIMESTAMP '1970-01-01 00:00:00') AS create_date, -- Convert `createDate` to TIMESTAMP
    DATEADD(SECOND, TRY_CAST(raw_data:modifyDate:"$date"::STRING AS NUMBER) / 1000, TIMESTAMP '1970-01-01 00:00:00') AS modify_date, -- Convert `modifyDate` to TIMESTAMP
    raw_data:rewardsReceiptItemList AS rewards_receipt_items -- Retain the nested JSON data for `rewardsReceiptItemList`
FROM staging_transaction_receipts
WHERE raw_data:_id IS NOT NULL; -- Ensure `_id` exists




INSERT INTO transaction_items (item_id, receipt_id, barcode, description, quantity, final_price, original_price, needs_fetch_review)
SELECT
    UUID_STRING() AS item_id, -- Generate a unique ID for each item
    raw_data:_id::STRING AS receipt_id, -- Extract receipt_id
    i.value:barcode::STRING AS barcode, -- Extract barcode from the nested list
    i.value:description::STRING AS description, -- Extract description
    i.value:quantityPurchased::INT AS quantity, -- Extract quantityPurchased
    i.value:finalPrice::DECIMAL(10, 2) AS final_price, -- Extract finalPrice
    i.value:itemPrice::DECIMAL(10, 2) AS original_price, -- Extract itemPrice
    i.value:needsFetchReview::BOOLEAN AS needs_fetch_review -- Extract needsFetchReview as BOOLEAN
FROM staging_transaction_receipts,
     LATERAL FLATTEN(input => raw_data:rewardsReceiptItemList) i -- Flatten the nested `rewardsReceiptItemList`
WHERE raw_data:_id IS NOT NULL; -- Ensure `_id` exists



----Question 1: What are the top 5 brands by receipts scanned for most recent month?
WITH most_recent_month AS (
    -- Determine the most recent month
    SELECT DATE_TRUNC('month', MAX(purchase_date)) AS recent_month_start
    FROM transaction_receipts
),
brand_receipts AS (
    -- Count receipts per brand for the most recent month
    SELECT
        COALESCE(pb.name, 'Unknown Brand') AS brand_name, -- Handle missing brand names
        COUNT(DISTINCT tr.receipt_id) AS receipt_count
    FROM transaction_receipts tr
    LEFT JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
    LEFT JOIN product_brands pb ON ti.barcode = pb.barcode
    WHERE tr.purchase_date >= (SELECT recent_month_start FROM most_recent_month)
      AND tr.purchase_date < (SELECT recent_month_start FROM most_recent_month) + INTERVAL '1 MONTH'
    GROUP BY COALESCE(pb.name, 'Unknown Brand') -- Group by brand name
)
-- Retrieve the top 5 brands
SELECT brand_name, receipt_count
FROM brand_receipts
ORDER BY receipt_count DESC
LIMIT 5;


--Question 2: How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?

WITH recent_and_previous_months AS (
    -- Determine the most recent and previous months
    SELECT 
        DATE_TRUNC('month', MAX(purchase_date)) AS recent_month_start,
        DATE_TRUNC('month', MAX(purchase_date)) - INTERVAL '1 MONTH' AS previous_month_start
    FROM transaction_receipts
),
brand_receipts AS (
    -- Count receipts per brand for both recent and previous months
    SELECT
        COALESCE(pb.name, 'Unknown Brand') AS brand_name, -- Handle missing brand names
        DATE_TRUNC('month', tr.purchase_date) AS month,
        COUNT(DISTINCT tr.receipt_id) AS receipt_count
    FROM transaction_receipts tr
    LEFT JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
    LEFT JOIN product_brands pb ON ti.barcode = pb.barcode
    WHERE tr.purchase_date >= (SELECT previous_month_start FROM recent_and_previous_months)
      AND tr.purchase_date < (SELECT recent_month_start FROM recent_and_previous_months) + INTERVAL '1 MONTH'
    GROUP BY COALESCE(pb.name, 'Unknown Brand'), DATE_TRUNC('month', tr.purchase_date)
),
ranked_brands AS (
    -- Rank brands within each month by receipt count
    SELECT
        brand_name,
        month,
        receipt_count,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY receipt_count DESC) AS rank
    FROM brand_receipts
),
comparison AS (
    -- Compare rankings between the recent and previous months
    SELECT 
        rb_recent.brand_name AS recent_brand_name,
        rb_recent.rank AS recent_rank,
        rb_previous.brand_name AS previous_brand_name,
        rb_previous.rank AS previous_rank
    FROM ranked_brands rb_recent
    LEFT JOIN ranked_brands rb_previous
      ON rb_recent.brand_name = rb_previous.brand_name
      AND rb_previous.month = (SELECT previous_month_start FROM recent_and_previous_months)
    WHERE rb_recent.month = (SELECT recent_month_start FROM recent_and_previous_months)
)
-- Output the comparison of rankings
SELECT 
    COALESCE(recent_brand_name, 'No Match') AS recent_brand,
    recent_rank,
    COALESCE(previous_brand_name, 'No Match') AS previous_brand,
    previous_rank
FROM comparison
ORDER BY recent_rank
LIMIT 5;



---Question 3 :When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT
    RECEIPT_STATUS AS receipt_status,
    AVG(TOTAL_SPENT) AS average_spend
FROM transaction_receipts
WHERE RECEIPT_STATUS IN ('FINISHED', 'REJECTED') -- Compare FINISHED and REJECTED
GROUP BY RECEIPT_STATUS
ORDER BY average_spend DESC;

--Question 4: When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
SELECT
    tr.RECEIPT_STATUS AS receipt_status,
    SUM(ti.quantity) AS total_items_purchased
FROM transaction_receipts tr
JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
WHERE tr.RECEIPT_STATUS IN ('FINISHED', 'REJECTED') -- Filter for FINISHED and REJECTED statuses
GROUP BY tr.RECEIPT_STATUS
ORDER BY total_items_purchased DESC;


---Question 5: Which brand has the most spend among users who were created within the past 6 months?

UPDATE consumer_profiles
SET user_id = PARSE_JSON(user_id):"$oid"::STRING
WHERE user_id LIKE '{"$oid"%';

WITH max_date AS (
    -- Get the maximum `created_date` to determine the 6-month cutoff
    SELECT MAX(created_date) AS max_created_date
    FROM consumer_profiles
),
recent_users AS (
    -- Find users created within the past 6 months
    SELECT user_id
    FROM consumer_profiles, max_date
    WHERE created_date >= DATEADD(MONTH, -6, max_created_date)
),
brand_spend AS (
    -- Sum total_spent for each brand among recent users
    SELECT
        pb.name AS brand_name,
        SUM(tr.total_spent) AS total_spend
    FROM transaction_receipts tr
    JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
    JOIN product_brands pb ON ti.barcode = pb.barcode
    WHERE tr.user_id IN (SELECT user_id FROM recent_users)
    GROUP BY pb.name
)
-- Retrieve the brand with the most spend
SELECT brand_name, total_spend
FROM brand_spend
ORDER BY total_spend DESC
LIMIT 1;
--Question 6: Which brand has the most transactions among users who were created within the past 6 months?
WITH max_date AS (
    -- Get the maximum `created_date` to determine the 6-month cutoff
    SELECT MAX(created_date) AS max_created_date
    FROM consumer_profiles
),
recent_users AS (
    -- Find users created within the past 6 months
    SELECT user_id
    FROM consumer_profiles, max_date
    WHERE created_date >= DATEADD(MONTH, -6, max_created_date)
),
brand_transactions AS (
    -- Count transactions for each brand among recent users
    SELECT
        pb.name AS brand_name,
        COUNT(DISTINCT tr.receipt_id) AS transaction_count
    FROM transaction_receipts tr
    JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
    JOIN product_brands pb ON ti.barcode = pb.barcode
    WHERE tr.user_id IN (SELECT user_id FROM recent_users)
    GROUP BY pb.name
)
-- Retrieve the brand with the most transactions
SELECT brand_name, transaction_count
FROM brand_transactions
ORDER BY transaction_count DESC
LIMIT 1;


-------
-- Check for items with barcodes not present in the product_brands table
-- Purpose: Identify missing or unmatched barcodes between transaction_items and product_brands.
SELECT ti.barcode
FROM transaction_items ti
LEFT JOIN product_brands pb ON ti.barcode = pb.barcode
WHERE pb.barcode IS NULL;


-- Find distinct barcodes in transaction_items that are not in product_brands
-- Purpose: Validate all barcodes in transaction_items have corresponding records in product_brands.
SELECT DISTINCT barcode
FROM transaction_items
WHERE barcode NOT IN (SELECT barcode FROM product_brands);

-- Check for orphaned receipts (receipts with no items associated)
-- Purpose: Identify transaction receipts with no matching records in transaction_items.
SELECT tr.receipt_id
FROM transaction_receipts tr
LEFT JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
WHERE ti.receipt_id IS NULL;


-- Analyze receipt distribution by brand, including "Unknown Brand"
-- Purpose: Count receipts per brand and calculate the percentage of total receipts.
SELECT
    COALESCE(pb.name, 'Unknown Brand') AS brand_name,
    COUNT(DISTINCT tr.receipt_id) AS receipt_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () AS percentage_of_total
FROM transaction_receipts tr
LEFT JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
LEFT JOIN product_brands pb ON ti.barcode = pb.barcode
GROUP BY COALESCE(pb.name, 'Unknown Brand');


-- Analyze monthly receipt distribution by brand
-- Purpose: Track monthly receipt counts for each brand and observe trends or anomalies.

WITH monthly_data AS (
    SELECT
        DATE_TRUNC('month', tr.purchase_date) AS month,
        COALESCE(pb.name, 'Unknown Brand') AS brand_name,
        COUNT(DISTINCT tr.receipt_id) AS receipt_count
    FROM transaction_receipts tr
    LEFT JOIN transaction_items ti ON tr.receipt_id = ti.receipt_id
    LEFT JOIN product_brands pb ON ti.barcode = pb.barcode
    GROUP BY month, COALESCE(pb.name, 'Unknown Brand')
)
SELECT month, brand_name, receipt_count
FROM monthly_data
ORDER BY month, receipt_count DESC;



-- Data Quality Analysis for "Unknown Brand"
-- 1. The "Unknown Brand" dominates the dataset with 98.8% of total receipts.
-- 2. This indicates a potential data quality issue where the brand name is missing or not mapped correctly.
-- 3. Other brands like Swanson, Tostitos, and Cracker Barrel Cheese contribute less than 1% combined, showing disproportionate representation.
-- 4. Temporal analysis shows "Unknown Brand" receipts spiked significantly in January 2021, making up 498 out of 1119 total.
-- 5. Barcodes for "Unknown Brand" may be missing or invalid, contributing to its misclassification.



-- Step 1: Analyze the overall brand receipt distribution to identify "Unknown Brand" dominance
-- This query calculates the count and percentage of receipts for each brand.
SELECT 
    brand_name,                            -- Brand name
    COUNT(*) AS receipt_count,             -- Total receipts for the brand
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total -- Percentage of total receipts
FROM 
    your_table                             -- Replace 'your_table' with your actual table name
GROUP BY 
    brand_name
ORDER BY 
    percentage_of_total DESC;

-- Step 2: Identify monthly trends for "Unknown Brand" to observe any significant spikes or patterns
-- This query groups data by month and calculates the count of receipts for "Unknown Brand."
SELECT 
    DATE_TRUNC('month', receipt_date) AS month, -- Group data by month
    brand_name,                                -- Brand name
    COUNT(*) AS receipt_count                  -- Total receipts for the brand in each month
FROM 
    your_table                                 -- Replace 'your_table' with your actual table name
WHERE 
    brand_name = 'Unknown Brand'               -- Filter for "Unknown Brand"
GROUP BY 
    DATE_TRUNC('month', receipt_date), brand_name
ORDER BY 
    month;

-- Step 3: Validate the barcode data for "Unknown Brand" to check for missing or invalid entries
-- This query analyzes the barcode details associated with "Unknown Brand."
SELECT 
    barcode,                                    -- Barcode value
    COUNT(*) AS receipt_count                  -- Total receipts with this barcode
FROM 
    your_table                                 -- Replace 'your_table' with your actual table name
WHERE 
    brand_name = 'Unknown Brand'               -- Filter for "Unknown Brand"
GROUP BY 
    barcode
ORDER BY 
    receipt_count DESC;

-- Step 4: Compare receipt counts for "Unknown Brand" across months to identify anomalies
-- This query compares monthly receipt counts for "Unknown Brand."
SELECT 
    DATE_TRUNC('month', receipt_date) AS month, -- Month-wise grouping
    COUNT(*) AS receipt_count                  -- Total receipts for the month
FROM 
    your_table                                 -- Replace 'your_table' with your actual table name
WHERE 
    brand_name = 'Unknown Brand'               -- Filter for "Unknown Brand"
GROUP BY 
    DATE_TRUNC('month', receipt_date)
ORDER BY 
    month;



-------

-- Data Quality Check for `product_brands`
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN brand_id IS NULL OR brand_id = '' THEN 1 ELSE 0 END) AS missing_brand_ids,
    SUM(CASE WHEN barcode IS NULL OR barcode = '' THEN 1 ELSE 0 END) AS missing_barcodes,
    SUM(CASE WHEN category IS NULL OR category = '' THEN 1 ELSE 0 END) AS missing_categories,
    SUM(CASE WHEN category_code IS NULL OR category_code = '' THEN 1 ELSE 0 END) AS missing_category_codes,
    SUM(CASE WHEN name IS NULL OR name = '' THEN 1 ELSE 0 END) AS missing_names
FROM 
    product_brands;

-- Data Quality Check for `transaction_receipts`
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN receipt_id IS NULL OR receipt_id = '' THEN 1 ELSE 0 END) AS missing_receipt_ids,
    SUM(CASE WHEN user_id IS NULL OR user_id = '' THEN 1 ELSE 0 END) AS missing_user_ids,
    SUM(CASE WHEN bonus_points_awarded < 0 THEN 1 ELSE 0 END) AS invalid_bonus_points_awarded,
    SUM(CASE WHEN purchase_date IS NULL THEN 1 ELSE 0 END) AS missing_purchase_dates,
    SUM(CASE WHEN purchase_date > CURRENT_DATE THEN 1 ELSE 0 END) AS future_dates,
    SUM(CASE WHEN total_spent < 0 THEN 1 ELSE 0 END) AS invalid_total_spent,
    SUM(CASE WHEN receipt_status IS NULL OR receipt_status = '' THEN 1 ELSE 0 END) AS missing_receipt_statuses
FROM 
    transaction_receipts;

-- Data Quality Check for `transaction_items`
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN item_id IS NULL OR item_id = '' THEN 1 ELSE 0 END) AS missing_item_ids,
    SUM(CASE WHEN receipt_id IS NULL OR receipt_id = '' THEN 1 ELSE 0 END) AS missing_receipt_ids,
    SUM(CASE WHEN barcode IS NULL OR barcode = '' THEN 1 ELSE 0 END) AS missing_barcodes,
    SUM(CASE WHEN description IS NULL OR description = '' THEN 1 ELSE 0 END) AS missing_descriptions,
    SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS invalid_quantities,
    SUM(CASE WHEN final_price < 0 THEN 1 ELSE 0 END) AS invalid_final_prices,
    SUM(CASE WHEN original_price < 0 THEN 1 ELSE 0 END) AS invalid_original_prices
FROM 
    transaction_items;

-- Data Quality Check for Nested JSON in `rewards_receipt_items`
SELECT 
    COUNT(*) AS total_receipts,
    SUM(CASE WHEN rewards_receipt_items IS NULL THEN 1 ELSE 0 END) AS missing_rewards_receipt_items
FROM 
    transaction_receipts;

-- Duplicate Records Check for `product_brands`
SELECT 
    brand_id, barcode, category, category_code, name, top_brand,
    COUNT(*) AS duplicate_count
FROM 
    product_brands
GROUP BY 
    brand_id, barcode, category, category_code, name, top_brand
HAVING 
    COUNT(*) > 1;

-- Duplicate Records Check for `transaction_receipts`
SELECT 
    receipt_id, user_id, purchase_date, total_spent,
    COUNT(*) AS duplicate_count
FROM 
    transaction_receipts
GROUP BY 
    receipt_id, user_id, purchase_date, total_spent
HAVING 
    COUNT(*) > 1;

-- Duplicate Records Check for `transaction_items`
SELECT 
    item_id, receipt_id, barcode, description, final_price,
    COUNT(*) AS duplicate_count
FROM 
    transaction_items
GROUP BY 
    item_id, receipt_id, barcode, description, final_price
HAVING 
    COUNT(*) > 1;

-- Orphaned Records Check: Items Without Matching Receipts
SELECT 
    ti.item_id, ti.receipt_id
FROM 
    transaction_items ti
LEFT JOIN 
    transaction_receipts tr ON ti.receipt_id = tr.receipt_id
WHERE 
    tr.receipt_id IS NULL;

-- Orphaned Records Check: Receipts Without Items
SELECT 
    tr.receipt_id
FROM 
    transaction_receipts tr
LEFT JOIN 
    transaction_items ti ON tr.receipt_id = ti.receipt_id
WHERE 
    ti.item_id IS NULL;

-- Proportion Check for Categories in `product_brands`
SELECT 
    category,
    COUNT(*) AS category_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS percentage_of_total
FROM 
    product_brands
GROUP BY 
    category
ORDER BY 
    category_count DESC;

-- Check for Invalid Boolean Flags
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN top_brand IS NULL THEN 1 ELSE 0 END) AS missing_top_brand_flags,
    SUM(CASE WHEN needs_fetch_review IS NULL THEN 1 ELSE 0 END) AS missing_needs_fetch_review_flags
FROM 
    transaction_items;

SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN brand_id IS NULL THEN 1 ELSE 0 END) AS MISSING_BRAND_IDS,
    SUM(CASE WHEN barcode IS NULL THEN 1 ELSE 0 END) AS MISSING_BARCODES,
    SUM(CASE WHEN category IS NULL THEN 1 ELSE 0 END) AS MISSING_CATEGORIES,
    SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END) AS MISSING_CATEGORY_CODES,
    SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS MISSING_NAMES
FROM product_brands;

-- Output:
-- TOTAL_RECORDS: 1167
-- MISSING_BRAND_IDS: 0
-- MISSING_BARCODES: 0
-- MISSING_CATEGORIES: 155
-- MISSING_CATEGORY_CODES: 650
-- MISSING_NAMES: 0

-- Note:
-- - 155 missing categories and 650 missing category codes are significant and need attention for proper categorization.
SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN receipt_id IS NULL THEN 1 ELSE 0 END) AS MISSING_RECEIPT_IDS,
    SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) AS MISSING_USER_IDS,
    SUM(CASE WHEN bonus_points_awarded < 0 THEN 1 ELSE 0 END) AS INVALID_BONUS_POINTS_AWARDED,
    SUM(CASE WHEN purchase_date IS NULL THEN 1 ELSE 0 END) AS MISSING_PURCHASE_DATES,
    SUM(CASE WHEN purchase_date > CURRENT_TIMESTAMP THEN 1 ELSE 0 END) AS FUTURE_DATES,
    SUM(CASE WHEN total_spent < 0 THEN 1 ELSE 0 END) AS INVALID_TOTAL_SPENT,
    SUM(CASE WHEN receipt_status IS NULL THEN 1 ELSE 0 END) AS MISSING_RECEIPT_STATUSES
FROM transaction_receipts;

-- Output:
-- TOTAL_RECORDS: 1119
-- MISSING_RECEIPT_IDS: 0
-- MISSING_USER_IDS: 0
-- INVALID_BONUS_POINTS_AWARDED: 0
-- MISSING_PURCHASE_DATES: 448
-- FUTURE_DATES: 0
-- INVALID_TOTAL_SPENT: 0
-- MISSING_RECEIPT_STATUSES: 0

-- Note:
-- - 448 missing purchase dates is critical for analyzing transaction timelines.


SELECT 
    COUNT(*) AS TOTAL_RECORDS,
    SUM(CASE WHEN item_id IS NULL THEN 1 ELSE 0 END) AS MISSING_ITEM_IDS,
    SUM(CASE WHEN receipt_id IS NULL THEN 1 ELSE 0 END) AS MISSING_RECEIPT_IDS,
    SUM(CASE WHEN barcode IS NULL THEN 1 ELSE 0 END) AS MISSING_BARCODES,
    SUM(CASE WHEN description IS NULL THEN 1 ELSE 0 END) AS MISSING_DESCRIPTIONS,
    SUM(CASE WHEN quantity < 0 THEN 1 ELSE 0 END) AS INVALID_QUANTITIES,
    SUM(CASE WHEN final_price < 0 THEN 1 ELSE 0 END) AS INVALID_FINAL_PRICES,
    SUM(CASE WHEN original_price < 0 THEN 1 ELSE 0 END) AS INVALID_ORIGINAL_PRICES
FROM transaction_items;

-- Output:
-- TOTAL_RECORDS: 6941
-- MISSING_ITEM_IDS: 0
-- MISSING_RECEIPT_IDS: 0
-- MISSING_BARCODES: 3851
-- MISSING_DESCRIPTIONS: 381
-- INVALID_QUANTITIES: 0
-- INVALID_FINAL_PRICES: 0
-- INVALID_ORIGINAL_PRICES: 0

-- Note:
-- - 3851 missing barcodes and 381 missing descriptions suggest incomplete transaction data.


SELECT 
    COUNT(*) AS TOTAL_RECEIPTS,
    SUM(CASE WHEN rewards_receipt_items IS NULL THEN 1 ELSE 0 END) AS MISSING_REWARDS_RECEIPT_ITEMS
FROM transaction_receipts;

-- Output:
-- TOTAL_RECEIPTS: 1119
-- MISSING_REWARDS_RECEIPT_ITEMS: 440

-- Note:
-- - 440 receipts have no associated rewards receipt items, impacting reward tracking.



SELECT 
    category,
    COUNT(*) AS CATEGORY_COUNT,
    ROUND((COUNT(*) * 100.0) / (SELECT COUNT(*) FROM product_brands), 2) AS PERCENTAGE_OF_TOTAL
FROM product_brands
GROUP BY category
ORDER BY CATEGORY_COUNT DESC;

-- Output Example:
-- CATEGORY                     CATEGORY_COUNT   PERCENTAGE_OF_TOTAL
-- Baking                       369             31.62
-- <Empty Category>             155             13.28
-- Beer Wine Spirits            90              7.71
-- Snacks                       75              6.43
-- Candy & Sweets               71              6.08
-- .

-- Note:
-- - A significant portion of categories are missing or incomplete, with 13.28% as empty.

