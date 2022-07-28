-- Databricks notebook source
-- DBTITLE 1,Exercício 1
SELECT *
FROM silver_olist.customers
WHERE descCity = 'sao paulo'

-- COMMAND ----------

-- DBTITLE 1,Exercício 2
SELECT *
FROM silver_olist.customers
WHERE descState = 'SP'

-- COMMAND ----------

-- DBTITLE 1,Exercício 3
SELECT *
FROM silver_olist.sellers
WHERE descCity = 'rio de janeiro' OR descState = 'SP'

-- COMMAND ----------

SELECT (10 + 2) * 2

-- COMMAND ----------

-- DBTITLE 1,Exercício 4
SELECT *
FROM silver_olist.products
WHERE (descCategoryName = 'perfumaria' OR descCategoryName = 'bebes') AND vlHeightCm > 5

-- COMMAND ----------

-- DBTITLE 1,Exercício 4 - plus
SELECT *
FROM silver_olist.products
WHERE descCategoryName IN ('perfumaria', 'bebes', 'artes') AND vlHeightCm > 5
