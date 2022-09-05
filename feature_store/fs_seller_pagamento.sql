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

select '{date}' as dtReferencia,
       idSeller,
       count(distinct descType) as qtTipoPagamento,
       avg(nrInstallments) as qtMediaParcelas,
       max(nrInstallments) as qtMaxParcelas,
       min(nrInstallments) as qtMinParcelas,
       
       sum(case when descType = 'boleto' then 1 else 0 end) / count(distinct idOrder) as pctBoleto,
       sum(case when descType = 'not_defined' then 1 else 0 end) / count(distinct idOrder) as pctMeioPgmtNaoIdentificado,
       sum(case when descType = 'credit_card' then 1 else 0 end) / count(distinct idOrder) as pctCartaoCredito,
       sum(case when descType = 'voucher' then 1 else 0 end) / count(distinct idOrder) as pctVoucher,
       sum(case when descType = 'debit_card' then 1 else 0 end) / count(distinct idOrder) as pctCartaoDebito,
       
       sum(case when descType = 'boleto' then vlPayment  else 0 end) / sum(vlPayment) as pctReceitaBoleto,
       sum(case when descType = 'not_defined' then vlPayment  else 0 end) / sum(vlPayment) as pctReceitaMeioPgmtNaoIdentificado,
       sum(case when descType = 'credit_card' then vlPayment  else 0 end) / sum(vlPayment) as pctReceitaCartaoCredito,
       sum(case when descType = 'voucher' then vlPayment  else 0 end) / sum(vlPayment) as pctReceitaVoucher,
       sum(case when descType = 'debit_card' then vlPayment  else 0 end) / sum(vlPayment) as pctReceitaCartaoDebito

from tb_join_all

group by 1, idSeller