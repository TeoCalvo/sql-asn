-- Databricks notebook source
SELECT COUNT(*) AS qtLinhas, -- numero e linhas
       COUNT(DISTINCT idSeller) AS qtSeller, -- sellers distintos
       COUNT(DISTINCT descState) AS qtEstadosDistinct, -- estados distintos
       COUNT(descState) AS qtEstadosDistinct -- estados não nulos

FROM silver_olist.sellers

-- COMMAND ----------

SELECT
       COUNT(DISTINCT idOrder) AS qtPedidos,
       AVG(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) AS avgDiasAtraso,
       STD(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) AS stdDiasAtraso,
       
       AVG(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) + 1.96 * STD(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) as nrLitSup,

       MIN(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) AS minDiasAtraso,
       MAX(DATEDIFF(dtDeliveredCustomer, dtEstimatedDelivered)) AS minDiasAtraso

FROM silver_olist.orders

WHERE DATE(dtDeliveredCustomer) > DATE(dtEstimatedDelivered)

-- COMMAND ----------

SELECT 
      descCategoryName,
      COUNT(DISTINCT idProduct) AS qtProduto,
      AVG(vlWeightGramas) as avgPeso

FROM silver_olist.products

GROUP BY descCategoryName

-- COMMAND ----------

SELECT 
      CASE WHEN DATE(dtDeliveredCustomer) > dtEstimatedDelivered THEN 'atraso'
      ELSE 'nao atraso' END AS flAtraso,
      COUNT(idOrder) AS qtPedido

FROM silver_olist.orders

GROUP BY 1

-- COMMAND ----------

SELECT 
      descCategoryName,
      COUNT(DISTINCT idProduct) AS qtProduto,
      AVG(vlWeightGramas) as avgPeso

FROM silver_olist.products

WHERE descCategoryName IS NOT NULL

GROUP BY descCategoryName

ORDER BY qtProduto DESC

-- COMMAND ----------

SELECT 
      descCategoryName,
      COUNT(DISTINCT idProduct) AS qtProduto,
      AVG(vlWeightGramas) as avgPeso

FROM silver_olist.products

WHERE descCategoryName IS NOT NULL

GROUP BY descCategoryName

-- HAVING qtProduto >= 50
HAVING COUNT(DISTINCT idProduct) >= 50

ORDER BY qtProduto ASC, avgPeso ASC
