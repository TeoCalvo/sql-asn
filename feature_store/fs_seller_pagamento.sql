with tb_join_all as (

  select t2.*,
         t3.idVendedor

  from silver.olist.pedido as t1

  left join silver.olist.pagamento_pedido as t2
  on t1.idPedido = t2.idPedido

  left join silver.olist.item_pedido as t3
  on t1.idPedido = t3.idPedido

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}', -6)

)

select '{date}' as dtReferencia,
       idVendedor,
       count(distinct descTipoPagamento) as qtTipoPagamento,
       avg(nrParcelas) as qtMediaParcelas,
       max(nrParcelas) as qtMaxParcelas,
       min(nrParcelas) as qtMinParcelas,
       
       sum(case when descTipoPagamento = 'boleto' then 1 else 0 end) / count(distinct idPedido) as pctBoleto,
       sum(case when descTipoPagamento = 'not_defined' then 1 else 0 end) / count(distinct idPedido) as pctMeioPgmtNaoIdentificado,
       sum(case when descTipoPagamento = 'credit_card' then 1 else 0 end) / count(distinct idPedido) as pctCartaoCredito,
       sum(case when descTipoPagamento = 'voucher' then 1 else 0 end) / count(distinct idPedido) as pctVoucher,
       sum(case when descTipoPagamento = 'debit_card' then 1 else 0 end) / count(distinct idPedido) as pctCartaoDebito,
       
       sum(case when descTipoPagamento = 'boleto' then vlPagamento  else 0 end) / sum(vlPagamento) as pctReceitaBoleto,
       sum(case when descTipoPagamento = 'not_defined' then vlPagamento  else 0 end) / sum(vlPagamento) as pctReceitaMeioPgmtNaoIdentificado,
       sum(case when descTipoPagamento = 'credit_card' then vlPagamento  else 0 end) / sum(vlPagamento) as pctReceitaCartaoCredito,
       sum(case when descTipoPagamento = 'voucher' then vlPagamento  else 0 end) / sum(vlPagamento) as pctReceitaVoucher,
       sum(case when descTipoPagamento = 'debit_card' then vlPagamento  else 0 end) / sum(vlPagamento) as pctReceitaCartaoDebito

from tb_join_all

group by 1, idVendedor