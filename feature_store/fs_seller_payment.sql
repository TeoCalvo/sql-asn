with tb_join_all as (

  select t2.*,
         t3.idSeller

  from silver_olist.orders as t1

  left join silver_olist.order_payment as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.order_items as t3
  on t1.idOrder = t3.idOrder

  where t1.dtPurchase < '{date}'
  and t1.dtPurchase >= add_months('{date}', -6)

)

select '{date}' as dtReference,
       idSeller,
       count(distinct descType) as qtPaymentType,
       avg(nrInstallments) as qtAvgInstallments,
       max(nrInstallments) as qtMaxInstallments,
       min(nrInstallments) as qtMinInstallments,
       
       sum(case when descType = 'boleto' then 1 else 0 end) / count(distinct idOrder) as pctBoletoCount,
       sum(case when descType = 'not_defined' then 1 else 0 end) / count(distinct idOrder) as pctNotDefinedCount,
       sum(case when descType = 'credit_card' then 1 else 0 end) / count(distinct idOrder) as pctCreditCardCount,
       sum(case when descType = 'voucher' then 1 else 0 end) / count(distinct idOrder) as pctVoucherCount,
       sum(case when descType = 'debit_card' then 1 else 0 end) / count(distinct idOrder) as pctDebitCardCount,
       
       sum(case when descType = 'boleto' then vlPayment  else 0 end) / sum(vlPayment) as pctBoletoRevenue,
       sum(case when descType = 'not_defined' then vlPayment  else 0 end) / sum(vlPayment) as pctNotDefinedRevenue,
       sum(case when descType = 'credit_card' then vlPayment  else 0 end) / sum(vlPayment) as pctCreditCardRevenue,
       sum(case when descType = 'voucher' then vlPayment  else 0 end) / sum(vlPayment) as pctVoucherRevenue,
       sum(case when descType = 'debit_card' then vlPayment  else 0 end) / sum(vlPayment) as pctDebitCardRevenue

from tb_join_all

group by 1, idSeller