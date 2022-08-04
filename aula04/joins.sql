-- Databricks notebook source
SELECT DATE(COALESCE(dtApproved, dtDeliveredCarrier, dtPurchase)) AS dtAprovado,
       COUNT(DISTINCT silver_olist.orders.idOrder) AS qtPedidos,
       SUM(vlPrice) AS vlDinheiro

FROM silver_olist.orders

LEFT JOIN silver_olist.order_items
ON silver_olist.orders.idOrder = silver_olist.order_items.idOrder

WHERE dtApproved IS NOT NULL

GROUP BY dtAprovado
ORDER BY dtAprovado

-- COMMAND ----------

SELECT idOrder,
       dtPurchase,
      dtApproved,
      dtDeliveredCarrier,
      
      CASE WHEN dtApproved IS NOT NULL THEN dtApproved
           WHEN dtDeliveredCarrier IS NOT NULL THEN dtDeliveredCarrier
           WHEN dtPurchase IS NOT NULL THEN dtPurchase
           ELSE NULL
     END AS dtPedido,
     
     COALESCE(dtApproved, dtDeliveredCarrier, dtPurchase) AS dtPedido

FROM silver_olist.orders
WHERE dtApproved IS NULL

-- COMMAND ----------

-- Qual categoria tem mais produtos vendidos?

-- silver_olist.products
-- silver_olist.order_items

SELECT t2.descCategoryName,
       COUNT(*) AS qtVendas,
       COUNT(DISTINCT t1.idOrder, t1.idOrderItem) AS qtVenda

FROM silver_olist.order_items AS t1 -- apelido para a tabela

LEFT JOIN silver_olist.products AS t2 -- apelido para a tabela
ON t1.idProduct = t2.idProduct

GROUP BY t2.descCategoryName
ORDER BY COUNT(*) DESC

-- COMMAND ----------

-- Qual categoria tem produtos mais caros, em média?

-- order_items (idProduct, preço)
-- products (idProduct, descCategoryName)

SELECT 
       t2.descCategoryName,
       AVG(vlPrice) as avgPreco,
       STD(vlPrice) as stdPreco

FROM silver_olist.order_items AS t1

LEFT JOIN silver_olist.products AS t2
ON t1.idProduct = t2.idProduct

GROUP BY t2.descCategoryName
ORDER BY avgPreco DESC

-- COMMAND ----------

SELECT 
       t1.*,
       t2.descCategoryName

FROM silver_olist.order_items AS t1

RIGHT JOIN silver_olist.products AS t2
ON t1.idProduct = t2.idProduct

-- COMMAND ----------

-- Qual categoria tem maiores fretes, em média?

-- products (descCategoryName) (idProduct PK)
-- order_items (vlFreight) (idProduct FK)

SELECT 
       t2.descCategoryName,
       AVG(t1.vlFreight) AS avgFrete

FROM silver_olist.order_items AS t1

LEFT JOIN silver_olist.products AS t2
ON t1.idProduct = t2.idProduct

GROUP BY t2.descCategoryName
ORDER BY avgFrete DESC

-- COMMAND ----------

-- Os clientes de qual estado pagam maior valor frete, em média?

--> order_items (idOrder, vlFreight)
--> orders (idOrder, idCustomer)
--> customers (descState, idCustomer)

SELECT 
       t3.descState,
       AVG(t1.vlFreight) AS avgFrete

FROM silver_olist.order_items AS t1

LEFT JOIN silver_olist.orders AS t2
ON t1.idOrder = t2.idOrder

LEFT JOIN silver_olist.customers as t3
ON t2.idCustomer = t3.idCustomer

GROUP BY t3.descState
ORDER BY avgFrete DESC

-- COMMAND ----------

-- Quando o frete é mais caro que a mercadoria, os clientes preferem qual meio de pagamento?

--> order_items (idOrder, vlFreigth, vlPrice)
--> order_payment (idOrder, descType)

SELECT 
      t2.descType,
      COUNT(t1.idOrder) AS qtdPedido

FROM silver_olist.order_items AS t1

LEFT JOIN silver_olist.order_payment AS t2
ON t2.idOrder = t1.idOrder

WHERE t1.vlPrice < t1.vlFreight

GROUP BY t2.descType



-- COMMAND ----------


