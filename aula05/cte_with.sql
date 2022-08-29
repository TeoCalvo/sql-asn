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
        MAX(fl_boleto) as qt_boleto,
        MAX(fl_not_defined) as qt_not_defined,
        MAX(fl_credit_card) as qt_credit_card,
        MAX(fl_voucher) as qt_voucher,
        MAX(fl_debit_card) as qt_debit_card

  FROM order_payment

  GROUP BY idOrder
)

select 
       flfreteCaro,
       count( distinct t1.idOrder) as qtPedido,
       avg(qt_boleto) as avg_boleto,
       std(qt_boleto) as std_boleto,
       avg(qt_boleto) + 1.96 * std(qt_boleto) /  sqrt(count( distinct t1.idOrder)) as lf_sup_boleto,
       avg(qt_boleto) - 1.96 * std(qt_boleto) /  sqrt(count( distinct t1.idOrder)) as lf_inf_boleto,
       
       avg(qt_not_defined) as avg_not_defined,
       std(qt_not_defined) as std_not_defined,
       avg(qt_not_defined) + 1.96 * std(qt_not_defined) /  sqrt(count( distinct t1.idOrder)) as lf_sup_not_defined,
       avg(qt_not_defined) - 1.96 * std(qt_not_defined) /  sqrt(count( distinct t1.idOrder)) as lf_inf_not_defined,
       
       avg(qt_credit_card) as avg_credit_card,
       std(qt_credit_card) as std_credit_card,
       avg(qt_credit_card) + 1.96 * std(qt_credit_card) /  sqrt(count( distinct t1.idOrder)) as lf_sup_credit_card,
       avg(qt_credit_card) - 1.96 * std(qt_credit_card) /  sqrt(count( distinct t1.idOrder)) as lf_inf_credit_card,
       
       avg(qt_voucher) as avg_voucher,
       std(qt_voucher) as std_voucher,
       avg(qt_voucher) + 1.96 * std(qt_voucher) /  sqrt(count( distinct t1.idOrder)) as lf_sup_voucher,
       avg(qt_voucher) - 1.96 * std(qt_voucher) /  sqrt(count( distinct t1.idOrder)) as lf_inf_voucher,
       
       avg(qt_debit_card) as avg_debit_card,
       std(qt_debit_card) as std_debit_card,
       avg(qt_debit_card) + 1.96 * std(qt_debit_card) /  sqrt(count( distinct t1.idOrder)) as lf_sup_debit_card,
       avg(qt_debit_card) - 1.96 * std(qt_debit_card) /  sqrt(count( distinct t1.idOrder)) as lf_inf_debit_card

from orders_freight as t1

left join order_qt_payment as t2
on t1.idOrder = t2.idOrder

GROUP BY flfreteCaro
ORDER BY flfreteCaro

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
        MAX(fl_boleto) as qt_boleto,
        MAX(fl_not_defined) as qt_not_defined,
        MAX(fl_credit_card) as qt_credit_card,
        MAX(fl_voucher) as qt_voucher,
        MAX(fl_debit_card) as qt_debit_card

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
