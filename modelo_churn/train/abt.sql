-- Databricks notebook source
drop table if exists silver_olist.abt_churn;
create table silver_olist.abt_churn

with tb_features as (

  select t1.*,

         t2.pctScoreNegativo,
         t2.pctScoreNeutro,
         t2.pctScorePositivo,
         t2.pctResposta,
         t2.qtAvaliacoes,
         t2.qtMensagem,
         t2.pctMensagem,
         t2.avgTempoResposta,
         t2.vlTempoMedioAvaliacao,
         t2.pctScoreNegativo1M,
         t2.pctScoreNeutro1M,
         t2.pctScorePositivo1M,
         t2.pctResposta1M,
         t2.qtAvaliacoes1M,
         t2.qtMensagem1M,
         t2.pctMensagem1M,
         t2.avgTempoResposta1M,
         t2.vlTempoMedioAvaliacao1M,
         t2.pctScoreNegativo3M,
         t2.pctScoreNeutro3M,
         t2.pctScorePositivo3M,
         t2.pctResposta3M,
         t2.qtAvaliacoes3M,
         t2.qtMensagem3M,
         t2.pctMensagem3M,
         t2.avgTempoResposta3M,
         t2.vlTempoMedioAvaliacao3M,

          t3.qtTipoPagamento,
          t3.qtMediaParcelas,
          t3.qtMaxParcelas,
          t3.qtMinParcelas,
          t3.pctBoleto,
          t3.pctMeioPgmtNaoIdentificado,
          t3.pctCartaoCredito,
          t3.pctVoucher,
          t3.pctCartaoDebito,
          t3.pctReceitaBoleto,
          t3.pctReceitaMeioPgmtNaoIdentificado,
          t3.pctReceitaCartaoCredito,
          t3.pctReceitaVoucher,
          t3.pctReceitaCartaoDebito,

          t4.qtPedidos,
          t4.qtItens,
          t4.qtMes,
          t4.qtPostadoAtraso,
          t4.qtPedidoMes6M,
          t4.qtPedidoMes,
          t4.qtItensMes6M,
          t4.qtItensMes,
          t4.propFreteReceitaTotal,
          t4.vlMediaFreteReceitaProp,
          t4.qtPedidoCancelado,
          t4.propItemPedido,
          t4.vlMedioFretePedido,
          t4.vlReceita,
          t4.vlTicketMedio,
          t4.pctPedidoDomingo,
          t4.pctPedidoSegunda,
          t4.pctPedidoTerca,
          t4.pctPedidoQuarta,
          t4.pctPedidoQuinta,
          t4.pctPedidoSexta,
          t4.pctPedidoSabado,
          t4.qtEntregaAntecipada,
          t4.qtEntregaAtrasada,
          t4.qtDiasMediaEntregaPrevista,
          t4.qtMediaDiasEntregaDespacho,
          t4.qtEstadosEntrega,
          t4.qtRazaoPedidoMesVsMedia,
          t4.qtRazaoPedidoMesVsMes1,
          t4.qtRazaoReceitaMesVsMedia,
          t4.qtRazaoReceitaMesVsMes1,
          t4.qtMediaDiasEntreVendas,
          t4.vlMaxPedido,
          t4.descTopEstado,

          t5.descTopCategoria,
          t5.vlMedioPeso,
          t5.vlMedioPeso1M,
          t5.vlMedioPeso3M,
          t5.vlMedioTamanhoNome,
          t5.vlMedioTamanhoNome1M,
          t5.vlMedioTamanhoNome3M,
          t5.qtMediaFotos,
          t5.qtMediaFotos1M,
          t5.qtMediaFotos3M,
          t5.vlMedioVolume,
          t5.vlMedioVolume1M,
          t5.vlMedioVolume3M,
          t5.qtProductos,
          t5.qtProductos1M,
          t5.qtProductos3M,
          t5.qtTiposCategorias,
          t5.qtTiposCategorias1M,
          t5.qtTiposCategorias3M

  from silver_olist.fs_seller_atividade as t1

  left join silver_olist.fs_seller_avaliacao as t2
  on t1.dtReferencia = t2.dtReferencia and t1.idSeller = t2.idSeller

  left join silver_olist.fs_seller_pagamento as t3
  on t1.dtReferencia = t3.dtReferencia and t1.idSeller = t3.idSeller

  left join silver_olist.fs_seller_pedido as t4
  on t1.dtReferencia = t4.dtReferencia and t1.idSeller = t4.idSeller

  left join silver_olist.fs_seller_produto as t5
  on t1.dtReferencia = t5.dtReferencia and t1.idSeller = t5.idSeller
  
  where t1.idSeller is not null
  and t1.dtReferencia <= '2018-02-01'
),

tb_date_sell as (

  select  distinct
          date(dtPurchase) as dtTarget,
          idSeller

  from silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

)

select 
       distinct
       t1.*,
       case when t2.idSeller is null then 1 else 0 end as flChurn

from tb_features as t1

left join tb_date_sell as t2
on t1.idSeller = t2.idSeller
and datediff(t2.dtTarget, t1.dtReferencia) <= 30 and datediff(t2.dtTarget, t1.dtReferencia) >= 0
order by t1.dtReferencia;

-- COMMAND ----------

select * from silver_olist.abt_churn
