-- Databricks notebook source
SELECT *
FROM bronze_olist.olist_orders_dataset

-- COMMAND ----------

SELECT order_id,
       customer_id,
       order_delivered_customer_date

FROM bronze_olist.olist_orders_dataset
