-- Databricks notebook source
-- Qual estado tem mais vendedores?

SELECT descState

FROM silver_olist.sellers

GROUP BY descState
ORDER BY COUNT(DISTINCT idSeller) DESC

LIMIT 1

-- COMMAND ----------

SELECT DATE(dtApproved) AS dtPedido,
       COUNT(DISTINCT idOrder) AS qtPedido

FROM silver_olist.orders

GROUP BY dtPedido
ORDER BY dtPedido

-- COMMAND ----------

-- IDENTIFICANDO PEDIDO DUPLICADOS

SELECT idOrder,
       count(*)
  
FROM silver_olist.orders

GROUP BY idOrder

HAVING count(*) > 1

-- COMMAND ----------

-- IDENTIFICA CLIENTES QUE FIZERAM MAIS DE 1 COMPRA

SELECT idCustomer,
       count(*)
       
FROM silver_olist.orders

GROUP BY idCustomer

HAVING COUNT(*) > 1

-- COMMAND ----------

SELECT idUniqueCustomer,
       COUNT(*),
       COUNT(DISTINCT idCustomer)

FROM silver_olist.customers

GROUP BY idUniqueCustomer
ORDER BY COUNT(*) ASC

-- COMMAND ----------

-- Qual categoria tem maior peso médio de produto?

SELECT descCategoryName,
       avg(vlWeightGramas) AS avgPeso

FROM silver_olist.products

GROUP BY descCategoryName
ORDER BY avgPeso DESC

LIMIT 1

-- COMMAND ----------

-- Quantos produtos são de construção?

SELECT COUNT(DISTINCT idProduct) as qtProduto

FROM silver_olist.products

WHERE descCategoryName IN ('casa_construcao',
                           'construcao_ferramentas_construcao',
                           'construcao_ferramentas_ferramentas',
                           'construcao_ferramentas_iluminacao',
                           'construcao_ferramentas_jardim',
                           'construcao_ferramentas_seguranca')

-- count(*) -- numero de linhas nao nulas
-- count(idProduct) -- numero de linhas da coluna idProduct
-- count(DISTINCT idProduct) -- numero de valores distintos da coluna idProduct
