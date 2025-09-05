/***************************************************************************************************
    NOTE FOR USERS:

    This script builds the Gold layer tables (dim_customers, dim_products, fact_sales) 
    from Silver layer sources. 

    ⚠️ Performance Consideration:
    - If your environment has limited processing power or constrained resources, 
      it is generally more efficient to source directly from the underlying tables 
      rather than querying views. 
    - Views can add overhead because they are essentially stored queries that must 
      be resolved at runtime, which may slow down large ETL jobs.

    Recommendation:
    - Use base tables wherever possible to minimize execution time.
    - Reserve views for abstraction, security, or readability only when system 
      capacity comfortably allows it.
***************************************************************************************************/


IF OBJECT_ID('gold.dim_customers','U') is not null
	DROP TABLE gold.dim_customers;
	GO

SELECT 
ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marital_status,
CASE WHEN ci.cst_gndr='n/a' and ca.gen<>'n/a' and ca.gen is not null THEN ca.gen
ELSE cst_gndr
END gender,
ca.bdate AS birth_date,
ci.cst_create_date AS create_date
INTO gold.dim_customers
from silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca on ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la on la.cid=ci.cst_key;
GO

IF OBJECT_ID('gold.dim_products','U')IS NOT NULL
	DROP TABLE gold.dim_products;
	GO

SELECT 
ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt,cpi.sls_prd_key) product_key,
cpi.prd_id AS product_id,
cpi.sls_prd_key AS product_number,
cpi.prd_nm AS product_name,
cpi.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance maintenance,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS start_date,
cpi.prd_end_dt AS end_date
INTO gold.dim_products
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 pc 
ON pc.id=cpi.cat_id
WHERE cpi.prd_end_dt IS NULL --To only obtain products that are currently available.Filtering out historical data
;
GO

IF OBJECT_ID('gold.fact_sales','U')IS NOT NULL
	DROP TABLE gold.fact_sales;
GO

SELECT 
sd.sls_ord_num AS order_number,
c.customer_key AS customer_key,
p.product_key AS product_key,
sls_order_dt AS order_date,
sls_ship_dt AS ship_date,
sls_due_dt AS due_date,
sls_sales AS sales,
sls_quantity AS quantity,
sls_price AS price
INTO gold.fact_sales
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_customers c
ON sd.sls_cust_id=c.customer_id
LEFT JOIN gold.dim_products p
ON sd.sls_prd_key=p.product_number
;
GO
