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

  where t1.dtPurchase < '{date}'
  and t1.dtPurchase >= add_months('{date}', -6)
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

         avg(case when datediff( '{date}',dtPurchase) < 30 and avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo1M,
         avg(case when datediff( '{date}',dtPurchase) < 30 and avgScoreReview >=3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro1M,
         avg(case when datediff( '{date}',dtPurchase) < 30 and avgScoreReview >=4 then 1 else 0 end) as pctScorePositivo1M,
         avg(case when datediff( '{date}',dtPurchase) < 30 and minDtAnswer is not null then 1 else 0 end) as pctResposta1M,
         sum(case when datediff( '{date}',dtPurchase) < 30 then qtReviews else 0 end) as qtReviews1M,
         sum(case when datediff( '{date}',dtPurchase) < 30 then qtMensagem else 0 end) as qtMensagem1M,
         avg(case when datediff( '{date}',dtPurchase) < 30 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta1M,
         avg(case when datediff( '{date}',dtPurchase) < 30 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview1M,

          avg(case when datediff( '{date}',dtPurchase) < 90 and avgScoreReview < 3 then 1 else 0 end) as pctScoreNegativo3M,
         avg(case when datediff( '{date}',dtPurchase) < 90 and avgScoreReview >=3 and avgScoreReview < 4 then 1 else 0 end) as pctScoreNeutro3M,
         avg(case when datediff( '{date}',dtPurchase) < 90 and avgScoreReview >=4 then 1 else 0 end) as pctScorePositivo3M,
         avg(case when datediff( '{date}',dtPurchase) < 90 and minDtAnswer is not null then 1 else 0 end) as pctResposta3M,
         sum(case when datediff( '{date}',dtPurchase) < 90 then qtReviews else 0 end) as qtReviews3M,
         sum(case when datediff( '{date}',dtPurchase) < 90 then qtMensagem else 0 end) as qtMensagem3M,
         avg(case when datediff( '{date}',dtPurchase) < 90 then datediff(minDtAnswer, minDtReview) end) as avgTempoResposta3M,
         avg(case when datediff( '{date}',dtPurchase) < 90 then datediff(minDtReview, dtDeliveredCustomer) end) as avgTempoReview3M

  from tb_order_seller_review

  group by idSeller

)

select  '{date}' as dtReferencia,
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