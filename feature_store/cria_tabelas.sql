-- Databricks notebook source
-- DBTITLE 1,fs_seller_atividade
create table silver_olist.fs_seller_atividade

with tb_join_all as (
  select t1.*,
         t2.*

  from silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t2.idOrder = t1.idOrder

  where t1.dtPurchase < '2018-01-01' -- meu hoje é o dia 2018-01-01
  and t1.dtPurchase >= add_months('2018-01-01',-6)

),

tb_summary as (

  select '2018-01-01' as dtReference,
         t1.idSeller,
         count(distinct date(t1.dtPurchase)) as qtActiveDays,
         min(datediff('2018-01-01', date(t1.dtPurchase))) as qtRecency,
         sum(vlPrice) as vlRevenue

  from tb_join_all as t1

  group by 1, t1.idSeller

),

tb_pos_top as (

  select *,
         row_number() over (partition by dtReference order by vlRevenue desc) as vlTop
  from tb_summary

)

select 
       t1.dtReference as dtReferencia,
       t1.idSeller,
       t1.qtActiveDays as qtDiasAtivos,
       t1.qtRecency as qtRecencia,
       t1.vlTop,
       case when t1.vlTop <= 10 then 1 else 0 end as flTop10,
       case when t1.vlTop <= 100 then 1 else 0 end as flTop100,
       t2.descCity as descCidade,
       t2.descState as descEstado

from tb_pos_top as t1

left join silver_olist.sellers as t2
on t1.idSeller = t2.idSeller

-- COMMAND ----------

-- DBTITLE 1,fs_seller_avaliacao
create table silver_olist.fs_seller_avaliacao

with tb_join_all as(

  SELECT t1.idOrder,
         t1.dtPurchase,
         t1.dtDeliveredCustomer,
         t2.idSeller,
         t3.idReview,
         t3.vlScore,
         t3.dtCreation,
         t3.dtAnswer,
         t3.descMessage

  FROM silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.order_review as t3
  on t1.idOrder = t3.idOrder

  where t1.dtPurchase < '2018-01-01'
  and t1.dtPurchase >= add_months('2018-01-01', -6)
  and t2.idSeller is not null

),

tb_order_seller_review as (

  select idOrder, idSeller, dtPurchase, dtDeliveredCustomer,
        count( distinct idReview) as qtReviews,
         avg(vlScore) as avgScoreReview,
         min(dtCreation) as minDtReview,
         max(dtCreation) as maxDtReview,
         min(dtAnswer) as minDtAnswer,
         sum(case when descMessage is not null then 1 else 0 end) as qtMensagem

  from tb_join_all
  group by 1,2,3,4

),

tb_summary (

  select idSeller,
         avg(case when avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo,
         avg(case when avgScoreReview >=3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro,
         avg(case when avgScoreReview >=4 then 1 else 0 end) as pctScorePositivo,
         avg(case when minDtAnswer is not null then 1 else 0 end) as pctResposta,
         sum(qtReviews) as qtReviews,
         sum(qtMensagem) as qtMensagem,
         sum(qtMensagem) / sum(qtReviews) as pctMensagem,
         avg(datediff(minDtAnswer, minDtReview)) as avgTempoResposta,
         avg(datediff(minDtReview, dtDeliveredCustomer)) as avgTempoReview,

         avg(case when datediff( '2018-01-01',dtPurchase) < 30 and avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo1M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 30 and avgScoreReview >=3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro1M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 30 and avgScoreReview >=4 then 1 else 0 end) as pctScorePositivo1M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 30 and minDtAnswer is not null then 1 else 0 end) as pctResposta1M,
         sum(case when datediff( '2018-01-01',dtPurchase) < 30 then qtReviews else 0 end) as qtReviews1M,
         sum(case when datediff( '2018-01-01',dtPurchase) < 30 then qtMensagem else 0 end) as qtMensagem1M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 30 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta1M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 30 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview1M,

          avg(case when datediff( '2018-01-01',dtPurchase) < 90 and avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo3M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 90 and avgScoreReview >=3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro3M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 90 and avgScoreReview >=4 then 1 else 0 end) as pctScorePositivo3M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 90 and minDtAnswer is not null then 1 else 0 end) as pctResposta3M,
         sum(case when datediff( '2018-01-01',dtPurchase) < 90 then qtReviews else 0 end) as qtReviews3M,
         sum(case when datediff( '2018-01-01',dtPurchase) < 90 then qtMensagem else 0 end) as qtMensagem3M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 90 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta3M,
         avg(case when datediff( '2018-01-01',dtPurchase) < 90 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview3M

  from tb_order_seller_review

  group by idSeller

)

select  '2018-01-01' as dtReferencia,
        idSeller,
        pctScoreNegativo,
        pctScoreNeutro,
        pctScorePositivo,
        pctResposta,
        qtReviews as qtAvaliacoes,
        qtMensagem,
        pctMensagem,
        avgTempoResposta,
        avgTempoReview as vlTempoMedioAvaliacao,
        pctScoreNegativo1M,
        pctScoreNeutro1M,
        pctScorePositivo1M,
        pctResposta1M,
        qtReviews1M as qtAvaliacoes1M,
        qtMensagem1M,
        qtMensagem1M / qtReviews1M as pctMensagem1M,
        avgTempoResposta1M,
        avgTempoReview1M as vlTempoMedioAvaliacao1M,
        pctScoreNegativo3M,
        pctScoreNeutro3M,
        pctScorePositivo3M,
        pctResposta3M,
        qtReviews3M qtAvaliacoes3M,
        qtMensagem3M,
        qtMensagem3M / qtReviews3M as pctMensagem3M,
        avgTempoResposta3M,
        avgTempoReview3M as vlTempoMedioAvaliacao3M

from tb_summary

-- COMMAND ----------

-- DBTITLE 1,fs_seller_pagamento
create table silver_olist.fs_seller_pagamento

with tb_join_all as (

  select t2.*,
         t3.idSeller

  from silver_olist.orders as t1

  left join silver_olist.order_payment as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.order_items as t3
  on t1.idOrder = t3.idOrder

  where t1.dtPurchase < '2018-01-01'
  and t1.dtPurchase >= add_months('2018-01-01', -6)

)

select '2018-01-01' as dtReferencia,
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

-- COMMAND ----------

-- DBTITLE 1,fs_seller_pedido
create table silver_olist.fs_seller_pedido

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

  where t1.dtPurchase < '2018-01-01'
  and t1.dtPurchase >= add_months('2018-01-01', -6)
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
         sum(vlFreight) / sum(vlPrice + vlFreight) as propFreteReceitaTotal,
         sum(vlFreight / (vlPrice + vlFreight)) / count(distinct idOrder) as vlMediaFreteReceitaProp,
         count(distinct case when descStatus = 'canceled' then idOrder end) as qtPedidoCancelado,
         count(*) / count(distinct idOrder) as propItemPedido,
         sum(vlFreight) / count(distinct idOrder) as vlMedioFretePedido,
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
         avg( datediff(date(dtDeliveredCustomer), date(dtEstimatedDelivered)) ) as qtDiasMediaEntregaPrevista,
         avg( datediff(date(dtDeliveredCarrier), date(dtShippingLimit)) ) as qtMediaDiasEntregaDespacho,
         count(distinct descStateCustomer) as qtEstadosEntrega,
         
         count(distinct case when datediff('2018-01-01',dtPurchase) < 30 then idOrder end) / (count(distinct t1.idOrder) / count(distinct date_format(dtPurchase, 'y-M')))  as qtRazaoPedidoMesVsMedia,
         count(distinct case when datediff('2018-01-01',dtPurchase) < 30 then idOrder end) / count(distinct case when datediff('2018-01-01',dtPurchase) >= 30 and datediff('2018-01-01',dtPurchase) < 60 then idOrder end)  as qtRazaoPedidoMesVsMes1,
         
          sum(case when datediff('2018-01-01',dtPurchase) < 30 then vlPrice end) / (sum(vlPrice) / count(distinct date_format(dtPurchase, 'y-M')))  as qtRazaoReceitaMesVsMedia,
         sum(case when datediff('2018-01-01',dtPurchase) < 30 then vlPrice end) / sum(case when datediff('2018-01-01',dtPurchase) >= 30 and datediff('2018-01-01',dtPurchase) < 60 then vlPrice end)  as qtRazaoReceitaMesVsMes1

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

select '2018-01-01' as dtReferencia,
       t1.*,
       t2.avgDiffDataVendas as qtMediaDiasEntreVendas,
       t2.vlMaxPedido,
       t3.descStateCustomer as descTopEstado
       
from tb_summary as t1

left join tb_summary_2 as t2
on t1.idSeller = t2.idSeller

left join tb_top_estado as t3
on t1.idSeller = t3.idSeller

-- COMMAND ----------

-- DBTITLE 1,fs_seller_produto
create table silver_olist.fs_seller_produto

with tb_join_all as (

  select t2.idSeller,
         t3.*,
         t1.*

  from silver_olist.orders as t1

  inner join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.products as t3
  on t2.idProduct = t3.idProduct

  where t1.dtPurchase < '2018-01-01'
  and t1.dtPurchase >= add_months('2018-01-01',-6)

),

tb_summary as (

  select idSeller,
         avg(vlWeightGramas) as vlMedioPeso,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 30 then vlWeightGramas end),0) as vlMedioPeso1M,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 90 then vlWeightGramas end),0) as vlMedioPeso3M,

         avg(nrNameLength) as vlMedioTamanhoNome,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 30 then nrNameLength end),0) as vlMedioTamanhoNome1M,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 90 then nrNameLength end),0) as vlMedioTamanhoNome3M,

         avg(nrPhotos) as qtMediaFotos,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 30 then nrPhotos end),0) as qtMediaFotos1M,
         coalesce(avg(case when datediff('2018-01-01', dtPurchase) < 90 then nrPhotos end),0) as qtMediaFotos3M,

         avg(vlLengthCm * vlHeightCm * vlWidthCm) as vlMedioVolume,
         avg(case when datediff('2018-01-01', dtPurchase) < 30 then vlLengthCm * vlHeightCm * vlWidthCm else 0 end) as vlMedioVolume1M,
         avg(case when datediff('2018-01-01', dtPurchase) < 90 then vlLengthCm * vlHeightCm * vlWidthCm else 0 end) as vlMedioVolume3M,

         count(distinct idProduct) as qtProductos,
         count(distinct case when datediff('2018-01-01', dtPurchase) < 30 then idProduct end) as qtProductos1M,
         count(distinct case when datediff('2018-01-01', dtPurchase) < 90 then idProduct end) as qtProductos3M,

         count(distinct descCategoryName) as qtTiposCategorias,
         count(distinct case when datediff('2018-01-01', dtPurchase) < 30 then descCategoryName end) as qtTiposCategorias1M,
         count(distinct case when datediff('2018-01-01', dtPurchase) < 90 then descCategoryName end) as qtTiposCategorias3M

  from tb_join_all

  group by idSeller
  
),

tb_seller_category as (

  select idSeller,
         descCategoryName,
         count(*) as qtCategory

  from tb_join_all
  group by idSeller, descCategoryName

),

tb_best_category as (

  select *,
        row_number() over (partition by idSeller order by qtCategory desc) as rankCategory
  from tb_seller_category
  qualify rankCategory = 1
)

select '2018-01-01' as dtReferencia,
       t1.*,
       t2.descCategoryName as descTopCategoria

from tb_summary as t1

left join tb_best_category as t2
on t1.idSeller = t2.idSeller


-- COMMAND ----------

with tb_seller_features as (

  select
        t1.dtReferencia,
        t1.idSeller,
        qtDiasAtivos,
        qtRecencia,
        vlTop,
        flTop10,
        flTop100,
        descCidade,
        descEstado,
        pctScoreNegativo,
        pctScoreNeutro,
        pctScorePositivo,
        pctResposta,
        qtAvaliacoes,
        qtMensagem,
        pctMensagem,
        avgTempoResposta,
        vlTempoMedioAvaliacao,
        pctScoreNegativo1M,
        pctScoreNeutro1M,
        pctScorePositivo1M,
        pctResposta1M,
        qtAvaliacoes1M,
        qtMensagem1M,
        pctMensagem1M,
        avgTempoResposta1M,
        vlTempoMedioAvaliacao1M,
        pctScoreNegativo3M,
        pctScoreNeutro3M,
        pctScorePositivo3M,
        pctResposta3M,
        qtAvaliacoes3M,
        qtMensagem3M,
        pctMensagem3M,
        avgTempoResposta3M,
        vlTempoMedioAvaliacao3M,
        qtTipoPagamento,
        qtMediaParcelas,
        qtMaxParcelas,
        qtMinParcelas,
        pctBoleto,
        pctMeioPgmtNaoIdentificado,
        pctCartaoCredito,
        pctVoucher,
        pctCartaoDebito,
        pctReceitaBoleto,
        pctReceitaMeioPgmtNaoIdentificado,
        pctReceitaCartaoCredito,
        pctReceitaVoucher,
        pctReceitaCartaoDebito,
        qtPedidos,
        qtItens,
        qtMes,
        qtPostadoAtraso,
        qtPedidoMes6M,
        qtPedidoMes,
        qtItensMes6M,
        qtItensMes,
        propFreteReceitaTotal,
        vlMediaFreteReceitaProp,
        qtPedidoCancelado,
        propItemPedido,
        vlMedioFretePedido,
        vlReceita,
        vlTicketMedio,
        pctPedidoDomingo,
        pctPedidoSegunda,
        pctPedidoTerca,
        pctPedidoQuarta,
        pctPedidoQuinta,
        pctPedidoSexta,
        pctPedidoSabado,
        qtEntregaAntecipada,
        qtEntregaAtrasada,
        qtDiasMediaEntregaPrevista,
        qtMediaDiasEntregaDespacho,
        qtEstadosEntrega,
        qtRazaoPedidoMesVsMedia,
        qtRazaoPedidoMesVsMes1,
        qtRazaoReceitaMesVsMedia,
        qtRazaoReceitaMesVsMes1,
        qtMediaDiasEntreVendas,
        vlMaxPedido,
        descTopEstado,
        vlMedioPeso,
        vlMedioPeso1M,
        vlMedioPeso3M,
        vlMedioTamanhoNome,
        vlMedioTamanhoNome1M,
        vlMedioTamanhoNome3M,
        qtMediaFotos,
        qtMediaFotos1M,
        qtMediaFotos3M,
        vlMedioVolume,
        vlMedioVolume1M,
        vlMedioVolume3M,
        qtProductos,
        qtProductos1M,
        qtProductos3M,
        qtTiposCategorias,
        qtTiposCategorias1M,
        qtTiposCategorias3M,
        descTopCategoria

  from silver_olist.fs_seller_atividade as t1

  left join silver_olist.fs_seller_avaliacao as t2
  on t1.idSeller = t2.idSeller and t1.dtReferencia = t2.dtReferencia

  left join silver_olist.fs_seller_pagamento as t3
  on t1.idSeller = t3.idSeller and t1.dtReferencia = t3.dtReferencia

  left join silver_olist.fs_seller_pedido as t4
  on t1.idSeller = t4.idSeller and t1.dtReferencia = t4.dtReferencia

  left join silver_olist.fs_seller_produto as t5
  on t1.idSeller = t5.idSeller and t1.dtReferencia = t5.dtReferencia
  
  ),
  
tb_carimbo_dias as (
  
  select distinct t2.idSeller,
         date(t1.dtPurchase) as dtAtivacao

  from silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  where t2.idSeller is not null

  order by 1,2
),

tb_carimbo_flag as (

  select t1.idSeller,
         min(case when t2.dtAtivacao is null then 1 else 0 end) as flNaoVenda30d

  from tb_seller_features as  t1

  left join tb_carimbo_dias as t2
  on t1.idSeller = t2.idSeller
  and datediff(t2.dtAtivacao,t1.dtReferencia) < 30
  and datediff(t2.dtAtivacao,t1.dtReferencia) >= 0

  group by 1

)

select t1.*,
       t2.flNaoVenda30d
       
from tb_seller_features as t1

left join tb_carimbo_flag as t2
on t1.idSeller = t2.idSeller

-- COMMAND ----------

select datediff(now(), '2022-09-01')
