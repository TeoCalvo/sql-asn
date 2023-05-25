with tb_join_all as (
  select t1.*,
         t2.*

  from silver.olist.pedido as t1

  left join silver.olist.item_pedido as t2
  on t2.idPedido = t1.idPedido

  where t1.dtPedido < '{date}' -- meu hoje é o dia {date}
  and t1.dtPedido >= add_months('{date}',-6)

),

tb_summary as (

  select '{date}' as dtReference,
         t1.idVendedor,
         count(distinct date(t1.dtPedido)) as qtActiveDays,
         min(datediff('{date}', date(t1.dtPedido))) as qtRecency,
         sum(t1.vlPreco) as vlRevenue

  from tb_join_all as t1

  group by 1, t1.idVendedor

),

tb_pos_top as (

  select *,
         row_number() over (partition by dtReference order by vlRevenue desc) as vlTop
  from tb_summary

)

select 
       t1.dtReference as dtReferencia,
       t1.idVendedor,
       t1.qtActiveDays as qtDiasAtivos,
       t1.qtRecency as qtRecencia,
       t1.vlTop,
       case when t1.vlTop <= 10 then 1 else 0 end as flTop10,
       case when t1.vlTop <= 100 then 1 else 0 end as flTop100,
       t2.descCidade as descCidade,
       t2.descUF as descEstado

from tb_pos_top as t1

left join silver.olist.vendedor as t2
on t1.idVendedor = t2.idVendedor