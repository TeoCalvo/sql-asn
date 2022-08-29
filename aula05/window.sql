-- Databricks notebook source
select idOrder,
       idSeller,
       dtShippingLimit,
       ROW_NUMBER() OVER (PARTITION BY idSeller ORDER BY dtShippingLimit DESC) as rn,
       LAG(dtShippingLimit) OVER (PARTITION BY idSeller ORDER BY dtShippingLimit DESC) as lagShippingLimit

from silver_olist.order_items

-- COMMAND ----------

WITH order_seller AS(

  SELECT
        t1.idSeller,
        t1.idOrder,
        t2.dtApproved,
        SUM(vlPrice) as totalPrice

  FROM silver_olist.order_items AS t1
  
  LEFT JOIN silver_olist.orders AS t2
  ON t1.idOrder = t2.idOrder
  
  GROUP BY idSeller, t1.idOrder, t2.dtApproved

),

rn_seller AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY idSeller ORDER BY dtApproved DESC) AS rn,
         SUM(totalPrice) OVER (PARTITION BY idSeller ORDER BY dtApproved DESC) AS sumAcum
  FROM order_seller

),

result AS (

SELECT idSeller,
       avg(totalPrice) as avgPrice

FROM rn_seller
WHERE rn <= 3

group by idSeller
)

SELECT * FROM rn_seller

-- COMMAND ----------

create table sandbox_apoiadores.tb_fodase AS

WITH order_seller AS(

  SELECT
        t1.idSeller,
        t1.idOrder,
        t2.dtApproved,
        SUM(vlPrice) as totalPrice

  FROM silver_olist.order_items AS t1
  
  LEFT JOIN silver_olist.orders AS t2
  ON t1.idOrder = t2.idOrder
  
  GROUP BY idSeller, t1.idOrder, t2.dtApproved

),

rn_seller AS (

  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY idSeller ORDER BY dtApproved DESC) AS rn,
         SUM(totalPrice) OVER (PARTITION BY idSeller ORDER BY dtApproved DESC) AS sumAcum
  FROM order_seller

),

result AS (

SELECT idSeller,
       avg(totalPrice) as avgPrice

FROM rn_seller
WHERE rn <= 3

group by idSeller
)

SELECT *
FROM rn_seller

