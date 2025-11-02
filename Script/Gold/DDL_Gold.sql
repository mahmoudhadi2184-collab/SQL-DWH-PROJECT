/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
	DROP VIEW gold.dim_customers

GO

CREATE VIEW gold.dim_customers
AS
	SELECT 
		ROW_NUMBER() OVER(ORDER BY CSI.cst_id) AS customer_key,
		CSI.cst_id				AS customer_id,
		CSI.cst_key				AS customer_number,
		CSI.cst_firstname		AS first_name,
		CSI.cst_lastname		AS last_name ,
		CSI.cst_material_status AS marital_status,
		CASE
			WHEN CSI.cst_gndr <> 'n/a' THEN CSI.cst_gndr
			ELSE COALESCE(ECA.gen , 'n/a')
		END						AS gender ,
		CSI.cst_create_date		AS create_date,
		ECA.bdate				AS birthdate,
		ELA.cntry				AS country
	FROM silver.crm_cust_info AS CSI
	LEFT JOIN silver.erd_cust_az12 AS ECA
		ON CSI.cst_key = ECA.cid
	LEFT JOIN silver.erd_loc_a101 AS ELA
		ON CSI.cst_key = ELA.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
	DROP VIEW gold.dim_product

GO

CREATE VIEW gold.dim_product AS 
	SELECT 
		ROW_NUMBER() OVER(ORDER BY CBI.prd_start_dt , CBI.prd_key) AS product_key ,
		CBI.prd_id		 AS product_id ,
		CBI.prd_key		 AS product_number ,
		CBI.prd_nm		 AS product_name ,
		CBI.cat_id		 AS category_id,
		EPC.cat			 AS	cayegory_name,
		EPC.subcat		 AS subcategory,
		EPC.maintenance  AS maintenance,
		CBI.prd_cost	 AS cost,
		CBI.prd_line	 AS product_line,
		CBI.prd_start_dt AS [start_date]
	FROM silver.crm_prd_info AS CBI
	LEFT JOIN silver.erp_px_cat_g1v2 AS EPC
	ON CBI.cat_id = EPC.id
	WHERE prd_end_dt IS NULL

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
	DROP VIEW gold.fact_sales

GO

CREATE VIEW gold.fact_sales AS
	SELECT 
		CSD.sls_ord_num  AS order_number,
		DC.customer_key  AS customer_key,
		DP.product_key   AS product_key,
		CSD.sls_order_dt AS order_date,
		CSD.sls_ship_dt  AS ship_date,
		CSD.sls_due_dt   AS due_date,
		CSD.sls_sales    AS sales_amount,
		CSD.sls_quantity AS quantity,
		CSD.sls_price	 AS unit_price
	FROM silver.crm_sales_details AS CSD
	LEFT JOIN gold.dim_customers AS DC
		ON CSD.sls_cust_id = DC.customer_id
	LEFT JOIN gold.dim_product AS DP
		ON CSD.sls_prd_key = DP.product_number
