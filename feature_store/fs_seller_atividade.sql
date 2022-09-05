with tb_join_all as (
  select t1.*,
         t2.*

  from silver_olist.orders as t1

  left join silver_olist.order_items as t2
  on t2.idOrder = t1.idOrder

  where t1.dtPurchase < '{date}' -- meu hoje é o dia {date}
  and t1.dtPurchase >= add_months('{date}',-6)

),

tb_summary as (

  select '{date}' as dtReference,
         t1.idSeller,
         count(distinct date(t1.dtPurchase)) as qtActiveDays,
         min(datediff('{date}', date(t1.dtPurchase))) as qtRecency,
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