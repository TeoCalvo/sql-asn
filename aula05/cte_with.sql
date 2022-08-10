-- Databricks notebook source
SELECT *

FROM (
  
  SELECT *,
         case when descCity = 'sao paulo' then 'paulistano'
              when descCity = 'presidente prudente' then 'prudentino'
              else 'fodase'
         end as descNaturalidade

  FROM silver_olist.sellers
  WHERE descState = 'SP'

)

WHERE descNaturalidade = 'prudentino'

-- COMMAND ----------

WITH sellers_paulistas AS (

  SELECT *,
         case when descCity = 'sao paulo' then 'paulistano'
              when descCity = 'presidente prudente' then 'prudentino'
              else 'fodase'
         end as descNaturalidade

  FROM silver_olist.sellers
  WHERE descState = 'SP'

)

SELECT *
FROM sellers_paulistas
WHERE descNaturalidade = 'prudentino'

-- COMMAND ----------

-- subquery para filtro no where

SELECT *

FROM silver_olist.order_items

WHERE idOrder in (
  SELECT idOrder
  FROM orders_2items
)

-- COMMAND ----------

-- Quando o frete é mais caro que a mercadoria, os clientes preferem qual meio de pagamento?

WITH orders_freight AS (
  SELECT idOrder,
         SUM(vlPrice) AS vlPrice,
         SUM(vlFreight) AS vlFreight,
         CASE WHEN SUM(vlPrice) < SUM(vlFreight) THEN 1 ELSE 0 END AS flfreteCaro

  FROM silver_olist.order_items
  GROUP BY idOrder
),

order_payment AS (

  SELECT *,
         case when descType = 'boleto' then 1 else 0 end as fl_boleto,
         case when descType = 'not_defined' then 1 else 0 end as fl_not_defined,
         case when descType = 'credit_card' then 1 else 0 end as fl_credit_card,
         case when descType = 'voucher' then 1 else 0 end as fl_voucher,
         case when descType = 'debit_card' then 1 else 0 end as fl_debit_card

  FROM silver_olist.order_payment

),

order_qt_payment AS (

  SELECT 
        idOrder,
        SUM(fl_boleto) as qt_boleto,
        SUM(fl_not_defined) as qt_not_defined,
        SUM(fl_credit_card) as qt_credit_card,
        SUM(fl_voucher) as qt_voucher,
        SUM(fl_debit_card) as qt_debit_card

  FROM order_payment

  GROUP BY idOrder
)

select 
       flfreteCaro,
       count( distinct t1.idOrder) as qtPedido,
       avg(qt_boleto) as avg_boleto,
       avg(qt_not_defined) as avg_not_defined,
       avg(qt_credit_card) as avg_credit_card,
       avg(qt_voucher) as avg_voucher,
       avg(qt_debit_card) as avg_debit_card
       

from orders_freight as t1

left join order_qt_payment as t2
on t1.idOrder = t2.idOrder

GROUP BY flfreteCaro
ORDER BY flfreteCaro

-- COMMAND ----------


