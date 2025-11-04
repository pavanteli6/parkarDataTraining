-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Transform Memberships Data
-- MAGIC 1. Extract customer_id from the file path 
-- MAGIC 1. Write transformed data to the Silver schema  

-- COMMAND ----------

use catalog pavan_catalog_all;
use schema bronze;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Read all PNG files (any folder level) from ADLS path
-- MAGIC binary_df = (
-- MAGIC     spark.read
-- MAGIC     .format("binaryFile")
-- MAGIC     .load("abfss://landing@parkaru15sa.dfs.core.windows.net/memberships/**/*.png")
-- MAGIC )
-- MAGIC
-- MAGIC display(binary_df)
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC binary_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("pavan_catalog_all.bronze.memberships")

-- COMMAND ----------

create or replace view 
  pavan_catalog_all.bronze.v_memberships
AS
SELECT * from bronze.memberships

-- COMMAND ----------


SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
       content AS membership_card
  FROM pavan_catalog_all.bronze.v_memberships;


-- COMMAND ----------

-- MAGIC ### 2. Write transformed data to the Silver schema 

-- COMMAND ----------

CREATE TABLE pavan_catalog_all.silver.memberships
AS
SELECT regexp_extract(path, '.*/([0-9]+)\\.png$', 1) AS customer_id,
       content AS membership_card
  FROM pavan_catalog_all.bronze.v_memberships;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Extract customer_id from the file path
-- MAGIC > [Documentation for regexp_extract Function](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/regexp_extract)  
-- MAGIC > [Documentation for Regex Pattern](https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Write transformed data to the Silver schema 

-- COMMAND ----------

SELECT * FROM pavan_catalog_all.silver.memberships;
