with tb_join_all as (

  select t2.idSeller,
         t3.*,
         t1.*

  from silver_olist.orders as t1

  inner join silver_olist.order_items as t2
  on t1.idOrder = t2.idOrder

  left join silver_olist.products as t3
  on t2.idProduct = t3.idProduct

  where t1.dtPurchase < '{date}'
  and t1.dtPurchase >= add_months('{date}',-6)

),

tb_summary as (

  select idSeller,
         avg(vlWeightGramas) as vlMedioPeso,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then vlWeightGramas end),0) as vlMedioPeso1M,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then vlWeightGramas end),0) as vlMedioPeso3M,

         avg(nrNameLength) as vlMedioTamanhoNome,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then nrNameLength end),0) as vlMedioTamanhoNome1M,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then nrNameLength end),0) as vlMedioTamanhoNome3M,

         avg(nrPhotos) as qtMediaFotos,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 30 then nrPhotos end),0) as qtMediaFotos1M,
         coalesce(avg(case when datediff('{date}', dtPurchase) < 90 then nrPhotos end),0) as qtMediaFotos3M,

         avg(vlLengthCm * vlHeightCm * vlWidthCm) as vlMedioVolume,
         avg(case when datediff('{date}', dtPurchase) < 30 then vlLengthCm * vlHeightCm * vlWidthCm else 0 end) as vlMedioVolume1M,
         avg(case when datediff('{date}', dtPurchase) < 90 then vlLengthCm * vlHeightCm * vlWidthCm else 0 end) as vlMedioVolume3M,

         count(distinct idProduct) as qtProductos,
         count(distinct case when datediff('{date}', dtPurchase) < 30 then idProduct end) as qtProductos1M,
         count(distinct case when datediff('{date}', dtPurchase) < 90 then idProduct end) as qtProductos3M,

         count(distinct descCategoryName) as qtTiposCategorias,
         count(distinct case when datediff('{date}', dtPurchase) < 30 then descCategoryName end) as qtTiposCategorias1M,
         count(distinct case when datediff('{date}', dtPurchase) < 90 then descCategoryName end) as qtTiposCategorias3M

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

select '{date}' as dtReferencia,
       t1.*,
       t2.descCategoryName as descTopCategoria

from tb_summary as t1

left join tb_best_category as t2
on t1.idSeller = t2.idSeller
