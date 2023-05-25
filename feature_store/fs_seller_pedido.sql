with tb_join_all as(

  select t1.*,
         t2.idProduto,
         t2.idVendedor,
         t2.dtLimiteEnvio,
         t2.vlPreco,
         t2.vlFrete,
         t3.descCidade as descCidadeCustomer,
         t3.descUF as descUFCustomer       

  from silver.olist.pedido as t1

  left join silver.olist.item_pedido as t2
  on t1.idPedido = t2.idPedido

  left join silver.olist.cliente as t3
  on t1.idCliente = t3.idCliente

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}', -6)
  and t2.idVendedor is not null
),

tb_summary (

  select t1.idVendedor,
         count(distinct t1.idPedido) as qtPedidos,
         count(*) as qtItens,
         count(distinct date_format(dtPedido, 'y-M')) as qtMes,
         count(distinct case when t1.dtEnvio > t1.dtLimiteEnvio then idPedido end) as qtPostadoAtraso,
         count(distinct t1.idPedido) / 6 as qtPedidoMes6M,
         count(distinct t1.idPedido) / count(distinct date_format(dtPedido, 'y-M')) as qtPedidoMes,
         count(*) / 6 as qtItensMes6M,
         count(*) / count(distinct date_format(dtPedido, 'y-M')) as qtItensMes,
         sum(vlFrete) / sum(vlPreco + vlFrete) as propFreteReceitaTotal,
         sum(vlFrete / (vlPreco + vlFrete)) / count(distinct idPedido) as vlMediaFreteReceitaProp,
         count(distinct case when descSituacao = 'canceled' then idPedido end) as qtPedidoCancelado,
         count(*) / count(distinct idPedido) as propItemPedido,
         sum(vlFrete) / count(distinct idPedido) as vlMedioFretePedido,
         sum(vlPreco) as vlReceita,
         sum(vlPreco) / count(distinct idPedido) as vlTicketMedio,
         count(distinct case when dayofweek(dtPedido) = 1 then idPedido end) / count(distinct idPedido) as pctPedidoDomingo,
         count(distinct case when dayofweek(dtPedido) = 2 then idPedido end) / count(distinct idPedido) as pctPedidoSegunda,
         count(distinct case when dayofweek(dtPedido) = 3 then idPedido end) / count(distinct idPedido) as pctPedidoTerca,
         count(distinct case when dayofweek(dtPedido) = 4 then idPedido end) / count(distinct idPedido) as pctPedidoQuarta,
         count(distinct case when dayofweek(dtPedido) = 5 then idPedido end) / count(distinct idPedido) as pctPedidoQuinta,
         count(distinct case when dayofweek(dtPedido) = 6 then idPedido end) / count(distinct idPedido) as pctPedidoSexta,
         count(distinct case when dayofweek(dtPedido) = 7 then idPedido end) / count(distinct idPedido) as pctPedidoSabado,
         count(distinct case when date(dtEntregue) < date(dtEstimativaEntrega) then idPedido end) as qtEntregaAntecipada,
         count(distinct case when date(dtEntregue) > date(dtEstimativaEntrega) then idPedido end) as qtEntregaAtrasada,
         avg( datediff(date(dtEntregue), date(dtEstimativaEntrega)) ) as qtDiasMediaEntregaPrevista,
         avg( datediff(date(dtEnvio), date(dtLimiteEnvio)) ) as qtMediaDiasEntregaDespacho,
         count(distinct descUFCustomer) as qtEstadosEntrega,
         
         count(distinct case when datediff('{date}',dtPedido) < 30 then idPedido end) / (count(distinct t1.idPedido) / count(distinct date_format(dtPedido, 'y-M')))  as qtRazaoPedidoMesVsMedia,
         count(distinct case when datediff('{date}',dtPedido) < 30 then idPedido end) / count(distinct case when datediff('{date}',dtPedido) >= 30 and datediff('{date}',dtPedido) < 60 then idPedido end)  as qtRazaoPedidoMesVsMes1,
         
          sum(case when datediff('{date}',dtPedido) < 30 then vlPreco end) / (sum(vlPreco) / count(distinct date_format(dtPedido, 'y-M')))  as qtRazaoReceitaMesVsMedia,
         sum(case when datediff('{date}',dtPedido) < 30 then vlPreco end) / sum(case when datediff('{date}',dtPedido) >= 30 and datediff('{date}',dtPedido) < 60 then vlPreco end)  as qtRazaoReceitaMesVsMes1

  from tb_join_all as t1

  group by t1.idVendedor
),

tb_seller_pedido as (

  select idVendedor,
         idPedido,
         dtPedido,
         date(dtPedido) as dtVenda,
         sum(vlPreco) as vlPedido

  from tb_join_all
  group by idVendedor, idPedido, dtPedido, dtVenda
),

tb_lag as (

  select *,
         lag(dtVenda) over (partition by idVendedor order by dtVenda) as lagDtVenda
  from tb_seller_pedido
  order by idVendedor, dtVenda

),

tb_summary_2 as (

  select idVendedor,
         avg(datediff(dtVenda, lagDtVenda)) as avgDiffDataVendas,
         max(vlPedido) as vlMaxPedido

  from tb_lag
  group by idVendedor

),

tb_seller_state as (

  select idVendedor,
         descUFCustomer,
         count(distinct idPedido) as qtPedido

  from tb_join_all

  group by idVendedor, descUFCustomer
  order by idVendedor, descUFCustomer
),

tb_top_estado as (

  select *,
         row_number() over (partition by idVendedor order by qtPedido desc) as rankEstado

  from tb_seller_state
  qualify rankEstado = 1
)

select '{date}' as dtReferencia,
       t1.*,
       t2.avgDiffDataVendas as qtMediaDiasEntreVendas,
       t2.vlMaxPedido,
       t3.descUFCustomer as descTopEstado
       
from tb_summary as t1

left join tb_summary_2 as t2
on t1.idVendedor = t2.idVendedor

left join tb_top_estado as t3
on t1.idVendedor = t3.idVendedor