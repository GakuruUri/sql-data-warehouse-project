-- crm_cust_info
-- Check for nulls or duplicates in Primary Key
-- Expectation; No result

SELECT  cst_id,
        COUNT(*)
FROM    bronze.crm_cust_info
GROUP   BY  cst_id
HAVING  COUNT(*) > 1 OR cst_id IS NULL


-- See some of the duplicates

SELECT  *
FROM    bronze.crm_cust_info
WHERE   cst_id = 29466

-- use window function to view duplicates

SELECT  *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM    bronze.crm_cust_info

--Filter the results of the above table

SELECT  *
FROM    (
    SELECT  *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM    bronze.crm_cust_info
)t 
WHERE   flag_last = 1
-- WHERE   flag_last != 1


-- Check for unwanted spaces in string values
-- TRIM removes leading and trailing spaces from a string

/*
If the original value is not equal to the same value after trimmoing, it means there are spaces
*/

SELECT  cst_lastname
FROM    bronze.crm_cust_info
WHERE   cst_lastname != TRIM(cst_lastname)

SELECT  cst_firstname
FROM    bronze.crm_cust_info
WHERE   cst_firstname != TRIM(cst_firstname)


SELECT  cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        cst_material_status,
        cst_gndr,
        cst_create_date
FROM    (
        SELECT  *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM    bronze.crm_cust_info
        WHERE   cst_id IS NOT NULL
)t 
WHERE   flag_last = 1


-- Quality Check: Check the consistency of values in low cardinality columns
-- Data standardization and consistency

SELECT  DISTINCT cst_gndr
FROM    bronze.crm_cust_info

-- Marital status column
SELECT  DISTINCT cst_material_status
FROM    bronze.crm_cust_info

-- In the datawarehouse, we aim to store clear, clean and meaningful data, rather than using abbreviated terms.


INSERT  INTO    silver.crm_cust_info(
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date
)
SELECT  cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,
        --cst_material_status,
        CASE    
            WHEN    UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
            WHEN    UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
            ELSE    'n/a'
        END cst_material_status,
        -- cst_gndr,
        CASE    
            WHEN    UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN    UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE    'n/a'
        END cst_gndr,
        cst_create_date
FROM    (
        SELECT  *,
        ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM    bronze.crm_cust_info
        WHERE   cst_id IS NOT NULL
)t 
WHERE   flag_last = 1




---- crm_prod:


-- Main Query


------------------------------------------------------------------


SELECT  prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        -- prd_line,
        CASE    UPPER(TRIM(prd_line))
            WHEN  'M' THEN 'Mountain'
            WHEN  'R' THEN 'Road'
            WHEN  'S' THEN 'Other Sales'
            WHEN  'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        -- prd_start_dt,
        -- prd_end_dt
        CAST(prd_end_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt)  OVER    (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM    bronze.crm_prd_info
WHERE   SUBSTRING(prd_key, 7, LEN(prd_key)) NOT IN (
    SELECT  sls_prd_key  FROM bronze.crm_sales_details
)



--------------------------------------------------------------------------



-- Search for data quality issues
-- Check for duplicates in the primary key.

SELECT  prd_id,
        COUNT(*)
FROM    bronze.crm_prd_info
GROUP   BY  prd_id
HAVING  COUNT(*) > 1 OR prd_id IS NULL


-- Check for unwanted spaces in prd_nm

SELECT  prd_nm
FROM    bronze.crm_prd_info
WHERE   prd_nm != TRIM(prd_nm)


-- Check for NULLs or Negative Numbers
SELECT  prd_cost
FROM    bronze.crm_prd_info
WHERE   prd_cost < 0 OR prd_cost IS NULL


-- Data standardization and Consistency
SELECT  DISTINCT    prd_line
FROM    bronze.crm_prd_info
        -- prd_line,
        CASE
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,

--  Check for invalid date orders
SELECT  *
FROM    bronze.crm_prd_info
WHERE   prd_end_dt < prd_start_dt

SELECT  DISTINCT id FROM bronze.erp_px_cat_g1v2

SELECT  sls_prd_key  FROM bronze.crm_sales_details


---------------------------------------------------------------------------------------------
SELECT  prd_id,
        prd_key,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
FROM    bronze.crm_prd_info
WHERE   REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') NOT IN (
    SELECT  DISTINCT id FROM bronze.erp_px_cat_g1v2
)


-- Date range tests

SELECT  prd_id,
        prd_key,
        prd_nm,
        prd_start_dt,
        CAST(prd_end_dt AS DATE) AS prd_start_dt,
        CAST(LEAD(prd_start_dt)  OVER    (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt_test
FROM    bronze.crm_prd_info
WHERE   prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')


-- silver.crp_prd_info Check
SELECT  prd_id,
        COUNT(*)
FROM    silver.crm_prd_info
GROUP   BY  prd_id
HAVING  COUNT(*) > 1 OR prd_id IS NULL

SELECT  prd_nm
FROM    silver.crm_prd_info
WHERE   prd_nm != TRIM(prd_nm)

SELECT  prd_cost
FROM    silver.crm_prd_info
WHERE   prd_cost < 0 OR prd_cost IS NULL


SELECT  DISTINCT prd_line
FROM    silver.crm_prd_info



SELECT  *
FROM    silver.crm_prd_info
WHERE   prd_end_dt < prd_start_dt


SELECT  *
FROM    silver.crm_prd_info


--- crm sales

----------------------------------------------------------------------------------
-- Main Query
----------------------------------------------------------------------------------


SELECT  sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        --sls_order_dt,
        CASE
            WHEN    sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
            ELSE    CAST(CAST(sls_order_dt AS NVARCHAR) AS DATE)
        END AS sls_order_dt,

        --sls_ship_dt,
        CASE
            WHEN    sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
            ELSE    CAST(CAST(sls_ship_dt AS NVARCHAR) AS DATE)
        END AS sls_ship_dt,

        -- sls_due_dt,
        CASE
            WHEN    sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
            ELSE    CAST(CAST(sls_due_dt AS NVARCHAR) AS DATE)
        END AS sls_due_dt,
        --sls_sales,
        CASE    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE    sls_sales
        END     AS sls_sales,
        sls_quantity,
        --sls_price
        CASE    WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
        END     AS sls_price
FROM    bronze.crm_sales_details


----------------------------------------------------------------------------------


-- Check spaces in order_num
SELECT  sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
FROM    bronze.crm_sales_details
WHERE   sls_ord_num != TRIM(sls_ord_num)



-- check FK and IDs- product keys
SELECT  sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
FROM    bronze.crm_sales_details
WHERE   sls_prd_key NOT IN (
    SELECT  prd_key FROM silver.crm_prd_info
)

-- check cust info
SELECT  sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
FROM    bronze.crm_sales_details
WHERE   sls_cust_id NOT IN (
    SELECT cst_id  FROM silver.crm_cust_info
)


-- Check for invalid dates

SELECT  NULLIF(sls_order_dt, 0) sls_order_dt
FROM    bronze.crm_sales_details
WHERE   sls_order_dt <= 0 
        OR LEN(sls_order_dt) != 8
        OR sls_order_dt > 20500101
        OR sls_order_dt < 19000101



SELECT  NULLIF(sls_ship_dt, 0) sls_ship_dt
FROM    bronze.crm_sales_details
WHERE   sls_ship_dt <= 0 
        OR LEN(sls_ship_dt) != 8
        OR sls_ship_dt > 20500101
        OR sls_ship_dt < 19000101




SELECT  NULLIF(sls_due_dt, 0) sls_due_dt
FROM    bronze.crm_sales_details
WHERE   sls_due_dt <= 0 
        OR LEN(sls_due_dt) != 8
        OR sls_due_dt > 20500101
        OR sls_due_dt < 19000101



-- Check for invalid date orders

SELECT  *
FROM    bronze.crm_sales_details
WHERE   sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

-- silver

SELECT  *
FROM    silver.crm_sales_details
WHERE   sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt


-- Check data consistency between sales, quantity and price
-- >> Sales = Quantity * Price
-- >> Values must not be NULL, Zero or Negative

SELECT  sls_sales AS old_sls_sales,
        sls_quantity,
        sls_price AS old_sls_price,
        CASE    WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE    sls_sales
        END     AS sls_sales,

        CASE    WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
        END     AS sls_price
    

FROM    bronze.crm_sales_details
WHERE   sls_sales != sls_quantity * sls_price
        OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER   BY sls_sales, sls_quantity, sls_price



-- silver

SELECT  sls_sales ,
        sls_quantity,
        sls_price 
FROM    silver.crm_sales_details
WHERE   sls_sales != sls_quantity * sls_price
        OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
        OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0

ORDER   BY sls_sales, sls_quantity, sls_price


SELECT  * FROM silver.crm_sales_details


--- erp_cust

SELECT  CASE    
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END cid,
        CASE    
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,
        CASE
            WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(160), ''), CHAR(13), '')) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(REPLACE(REPLACE(TRIM(gen), CHAR(160), ''), CHAR(13), '')) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen
FROM    bronze.erp_cust_az12




WHERE   CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END NOT IN (SELECT DISTINCT cst_key FROM silver.crm_cust_info)



--WHERE   cid LIKE '%AW00011000%'

--SELECT  *   FROM silver.crm_cust_info


--- Identify out of range dates

SELECT bdate
FROM    bronze.erp_cust_az12
WHERE   bdate < '1924-01-01' OR bdate > GETDATE()


-- silver

SELECT bdate
FROM    silver.erp_cust_az12
WHERE   bdate < '1924-01-01' OR bdate > GETDATE()

SELECT  DISTINCT gen,
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END gen
FROM    bronze.erp_cust_az12


SELECT  gen
FROM    bronze.erp_cust_az12

-- silver
SELECT  DISTINCT gen
FROM    silver.erp_cust_az12


SELECT  * FROM silver.erp_cust_az12


--- erp_loc

----------------------------------------------------------------------------------
-- Main query
----------------------------------------------------------------------------------

SELECT  REPLACE(cid, '-', '') cid,
    CASE
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(160), ''), CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(160), ''), CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(160), ''), CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(160), ''), CHAR(13), ''))
    END AS cntry
FROM    bronze.erp_loc_a101;


----------------------------------------------------------------------------------




SELECT  REPLACE(cid, '-', '') cid,
        cntry
FROM    bronze.erp_loc_a101
WHERE   REPLACE(cid, '-', '') NOT IN
    (SElECT  cst_key FROM silver.crm_cust_info)


SELECT  REPLACE(cid, '-', '') cid,
        cntry
FROM    silver.erp_loc_a101
WHERE   REPLACE(cid, '-', '') NOT IN
    (SElECT  cst_key FROM silver.crm_cust_info)

-- Data standardization and consistency


SELECT DISTINCT 
    cntry AS old_cntry,
    CASE
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(160), ''), CHAR(13), '')) = 'DE' THEN 'Germany'
        WHEN UPPER(REPLACE(REPLACE(TRIM(cntry), CHAR(160), ''), CHAR(13), '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(REPLACE(REPLACE(cntry, CHAR(160), ''), CHAR(13), '')) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(REPLACE(REPLACE(cntry, CHAR(160), ''), CHAR(13), ''))
    END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry




SELECT  DISTINCT cntry AS old_cntry,
        CASE    
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry
FROM    bronze.erp_loc_a101
ORDER   BY cntry


SELECT  DISTINCT cntry
FROM    silver.erp_loc_a101
ORDER   BY cntry


SELECT  *   FROM silver.erp_loc_a101


--- px_cat

SELECT  id,
        cat,
        subcat,
        TRIM(REPLACE(REPLACE(maintenance, CHAR(160), ''), CHAR(13), '')) AS maintenance
FROM    bronze.erp_px_cat_g1v2


-- Check unwanted spaces
SELECT  *
FROM    bronze.erp_px_cat_g1v2
WHERE   cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Check standardization and consistency
SELECT DISTINCT 
    TRIM(REPLACE(REPLACE(maintenance, CHAR(160), ''), CHAR(13), '')) AS maintenance
FROM bronze.erp_px_cat_g1v2

SELECT  *   FROM silver.erp_px_cat_g1v2



