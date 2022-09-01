-- Databricks notebook source
with tb_join_all as(

  select t1.*,
         t2.idProduct,
         t2.idSeller,
         t2.dtShippingLimit,
         t2.vlPrice,
         t2.vlFreight,
         t3.descCity as descCityCustomer,
         t3.descState as descStateCustomer       

  from silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.customers as t3
  on t1.idCustomer = t3.idCustomer

  where t1.dtPurchase < '{date}'
  and t1.dtPurchase >= add_months('{date}', -6)
  and t2.idSeller is not null
),

tb_summary (

  select t1.idSeller,
         count(distinct t1.idOrder) as qtPedidos,
         count(*) as qtItens,
         count(distinct date_format(dtPurchase, 'y-M')) as qtMes,
         count(distinct case when t1.dtDeliveredCarrier > t1.dtShippingLimit then idOrder end) as qtPostadoAtraso,
         count(distinct t1.idOrder) / 6 as qtPedidoMes6M,
         count(distinct t1.idOrder) / count(distinct date_format(dtPurchase, 'y-M')) as qtPedidoMes,
         count(*) / 6 as qtItensMes6M,
         count(*) / count(distinct date_format(dtPurchase, 'y-M')) as qtItensMes,
         sum(vlFreight) / sum(vlPrice + vlFreight) as propFreteTotal,
         sum(vlFreight / (vlPrice + vlFreight)) / count(distinct idOrder) as avgFreteProp,
         count(distinct case when descStatus = 'canceled' then idOrder end) as qtPedidoCancelado,
         count(*) / count(distinct idOrder) as propItemPedido,
         sum(vlFreight) / count(distinct idOrder) as vlFretePedido,
         sum(vlPrice) as vlReceita,
         sum(vlPrice) / count(distinct idOrder) as vlTicketMedio,
         count(distinct case when dayofweek(dtPurchase) = 1 then idOrder end) / count(distinct idOrder) as pctPedidoDomingo,
         count(distinct case when dayofweek(dtPurchase) = 2 then idOrder end) / count(distinct idOrder) as pctPedidoSegunda,
         count(distinct case when dayofweek(dtPurchase) = 3 then idOrder end) / count(distinct idOrder) as pctPedidoTerca,
         count(distinct case when dayofweek(dtPurchase) = 4 then idOrder end) / count(distinct idOrder) as pctPedidoQuarta,
         count(distinct case when dayofweek(dtPurchase) = 5 then idOrder end) / count(distinct idOrder) as pctPedidoQuinta,
         count(distinct case when dayofweek(dtPurchase) = 6 then idOrder end) / count(distinct idOrder) as pctPedidoSexta,
         count(distinct case when dayofweek(dtPurchase) = 7 then idOrder end) / count(distinct idOrder) as pctPedidoSabado,
         count(distinct case when date(dtDeliveredCustomer) < date(dtEstimatedDelivered) then idOrder end) as qtEntregaAntecipada,
         count(distinct case when date(dtDeliveredCustomer) > date(dtEstimatedDelivered) then idOrder end) as qtEntregaAtrasada,
         avg( datediff(date(dtDeliveredCustomer), date(dtEstimatedDelivered)) ) as avgDiasEntregaPrevista,
         avg( datediff(date(dtDeliveredCarrier), date(dtShippingLimit)) ) as avgDiasEntregaDespacho,
         count(distinct descStateCustomer) as qtEstadosEntrega,
         
         count(distinct case when datediff('{date}',dtPurchase) < 30 then idOrder end) / (count(distinct t1.idOrder) / count(distinct date_format(dtPurchase, 'y-M')))  as qtRazaoPedidoMesVsMedia,
         count(distinct case when datediff('{date}',dtPurchase) < 30 then idOrder end) / count(distinct case when datediff('{date}',dtPurchase) >= 30 and datediff('{date}',dtPurchase) < 60 then idOrder end)  as qtRazaoPedidoMesVsMes1,
         
          sum(case when datediff('{date}',dtPurchase) < 30 then vlPrice end) / (sum(vlPrice) / count(distinct date_format(dtPurchase, 'y-M')))  as qtRazaoReceitaMesVsMedia,
         sum(case when datediff('{date}',dtPurchase) < 30 then vlPrice end) / sum(case when datediff('{date}',dtPurchase) >= 30 and datediff('{date}',dtPurchase) < 60 then vlPrice end)  as qtRazaoRceitaMesVsMes1

  from tb_join_all as t1

  group by t1.idSeller
),

tb_seller_pedido as (

  select idSeller,
         idOrder,
         dtPurchase,
         date(dtPurchase) as dtVenda,
         sum(vlPrice) as vlPedido

  from tb_join_all
  group by idSeller, idOrder, dtPurchase, dtVenda
),

tb_lag as (

  select *,
         lag(dtVenda) over (partition by idSeller order by dtVenda) as lagDtVenda
  from tb_seller_pedido
  order by idSeller, dtVenda

),

tb_summary_2 as (

  select idSeller,
         avg(datediff(dtVenda, lagDtVenda)) as avgDiffDataVendas,
         max(vlPedido) as vlMaxPedido

  from tb_lag
  group by idSeller

),

tb_seller_state as (

  select idSeller,
         descStateCustomer,
         count(distinct idOrder) as qtPedido

  from tb_join_all

  group by idSeller, descStateCustomer
  order by idSeller, descStateCustomer
),

tb_top_estado as (

  select *,
         row_number() over (partition by idSeller order by qtPedido desc) as rankEstado

  from tb_seller_state
  qualify rankEstado = 1

)

select t1.*,
       t2.avgDiffDataVendas,
       t2.vlMaxPedido,
       t3.descStateCustomer as descTopEstado
       
from tb_summary as t1

left join tb_summary_2 as t2
on t1.idSeller = t2.idSeller

left join tb_top_estado as t3
on t1.idSeller = t3.idSeller

-- COMMAND ----------

select *

from silver_olist.orders
