-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Addresses Data
-- MAGIC 1. Create one record for each customer with 2 sets of address columns, 1 for shipping and 1 for billing address 
-- MAGIC 1. Write transformed data to the Silver schema  

-- COMMAND ----------

use catalog pavan_catalog_all;
use schema bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Ingest raw data into Bronze
-- MAGIC raw_df = (
-- MAGIC     spark.read
-- MAGIC     .option("delimiter", "\t")
-- MAGIC     .option("header", "true")
-- MAGIC     .csv("abfss://landing@parkaru15sa.dfs.core.windows.net/addresses/")
-- MAGIC )
-- MAGIC display(raw_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC raw_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("pavan_catalog_all.bronze.bronze_addresses")

-- COMMAND ----------

create or replace view 
  pavan_catalog_all.bronze.v_addresses
AS
SELECT * from bronze.bronze_addresses

-- COMMAND ----------

SELECT customer_id,
       address_type,
       address_line_1,
       city,
       state,
       postcode
  FROM pavan_catalog_all.bronze.v_addresses;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Create one record for each customer with both addresses, one for each address_type
-- MAGIC > [Documentation for PIVOT clause](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-pivot)

-- COMMAND ----------

SELECT *
 FROM (SELECT customer_id,
            address_type,
            address_line_1,
            city,
            state,
            postcode
        FROM pavan_catalog_all.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing')
       );

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Write transformed data to the Silver schema 

-- COMMAND ----------

CREATE TABLE pavan_catalog_all.silver.addresses
AS
SELECT *
 FROM (SELECT customer_id,
            address_type,
            address_line_1,
            city,
            state,
            postcode
        FROM pavan_catalog_all.bronze.v_addresses)
PIVOT (MAX(address_line_1) AS address_line_1,
       MAX(city) AS city,
       MAX(state) AS state,
       MAX(postcode) AS postcode
       FOR address_type IN ('shipping', 'billing')
       );

-- COMMAND ----------

SELECT * FROM pavan_catalog_all.silver.addresses;
