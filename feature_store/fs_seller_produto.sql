with tb_join_all as (

  select t2.idVendedor,
         t3.*,
         t1.*

  from silver.olist.pedido as t1

  inner join silver.olist.item_pedido as t2
  on t1.idPedido = t2.idPedido

  left join silver.olist.produto as t3
  on t2.idProduto = t3.idProduto

  where t1.dtPedido < '{date}'
  and t1.dtPedido >= add_months('{date}',-6)

),

tb_summary as (

  select idVendedor,
         avg(vlPesoGramas) as vlMedioPeso,
         coalesce(avg(case when datediff('{date}', dtPedido) < 30 then vlPesoGramas end),0) as vlMedioPeso1M,
         coalesce(avg(case when datediff('{date}', dtPedido) < 90 then vlPesoGramas end),0) as vlMedioPeso3M,

         avg(nrTamanhoNome) as vlMedioTamanhoNome,
         coalesce(avg(case when datediff('{date}', dtPedido) < 30 then nrTamanhoNome end),0) as vlMedioTamanhoNome1M,
         coalesce(avg(case when datediff('{date}', dtPedido) < 90 then nrTamanhoNome end),0) as vlMedioTamanhoNome3M,

         avg(nrFotos) as qtMediaFotos,
         coalesce(avg(case when datediff('{date}', dtPedido) < 30 then nrFotos end),0) as qtMediaFotos1M,
         coalesce(avg(case when datediff('{date}', dtPedido) < 90 then nrFotos end),0) as qtMediaFotos3M,

         avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as vlMedioVolume,
         avg(case when datediff('{date}', dtPedido) < 30 then vlComprimentoCm * vlAlturaCm * vlLarguraCm else 0 end) as vlMedioVolume1M,
         avg(case when datediff('{date}', dtPedido) < 90 then vlComprimentoCm * vlAlturaCm * vlLarguraCm else 0 end) as vlMedioVolume3M,

         count(distinct idProduto) as qtProductos,
         count(distinct case when datediff('{date}', dtPedido) < 30 then idProduto end) as qtProductos1M,
         count(distinct case when datediff('{date}', dtPedido) < 90 then idProduto end) as qtProductos3M,

         count(distinct descCategoria) as qtTiposCategorias,
         count(distinct case when datediff('{date}', dtPedido) < 30 then descCategoria end) as qtTiposCategorias1M,
         count(distinct case when datediff('{date}', dtPedido) < 90 then descCategoria end) as qtTiposCategorias3M

  from tb_join_all

  group by idVendedor
  
),

tb_seller_category as (

  select idVendedor,
         descCategoria,
         count(*) as qtCategory

  from tb_join_all
  group by idVendedor, descCategoria

),

tb_best_category as (

  select *,
        row_number() over (partition by idVendedor order by qtCategory desc) as rankCategory
  from tb_seller_category
  qualify rankCategory = 1
)

select '{date}' as dtReferencia,
       t1.*,
       t2.descCategoria as descTopCategoria

from tb_summary as t1

left join tb_best_category as t2
on t1.idVendedor = t2.idVendedor
