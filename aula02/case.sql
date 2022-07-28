-- Databricks notebook source
SELECT *,
      CASE WHEN descState = 'SP' THEN 'paulista'
           WHEN descState = 'RJ' THEN 'fluminense'
           WHEN descState = 'PR' THEN 'paranaense'
      ELSE 'fodase' END AS descCidadania

FROM silver_olist.sellers

-- COMMAND ----------

SELECT *,
        
        CASE WHEN DATE(dtDeliveredCustomer) > DATE(dtEstimatedDelivered) THEN 'Entregue com atraso'
        ELSE 'Chegou antes do prazo' END AS descAtraso,
        
        CASE WHEN DATE(dtDeliveredCustomer) > DATE(dtEstimatedDelivered) THEN 1
        ELSE 0 END AS flAtraso

FROM silver_olist.orders

-- COMMAND ----------

-- DBTITLE 1,Maneira menos verbosa
select
  case
    descCity
    when 'franca' then 'SP'
    when 'sao paulo' then 'SP'
    when 'jaragua do sul' then 'SC'
    else 'OUTRA'
  END as UF
FROM silver_olist.customers

-- COMMAND ----------

-- para frete inferior à 10%: ‘10%’
-- para frete entre 10% e 25%: ‘10% a 25%’
-- para frete entre 25% e 50%: ‘25% a 50%’
-- para frete maior que 50%: ‘+50%’

SELECT *,
       ROUND(100 * vlFreight / (vlPrice + vlFreight) , 2) AS pctFrete,
       
       CASE WHEN vlFreight / (vlPrice + vlFreight) < 0.1 THEN '10%'
            WHEN vlFreight / (vlPrice + vlFreight) < 0.25  THEN '10% A 25%'
            WHEN vlFreight / (vlPrice + vlFreight) < 0.5  THEN '25% A 50%'
            ELSE '+50%'
       END AS descPropFrete       

FROM silver_olist.order_items

