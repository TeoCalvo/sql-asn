# Databricks notebook source
tables = [
    'silver_olist.customers',
    'silver_olist.order_items',
    'silver_olist.order_payment',
    'silver_olist.order_review',
    'silver_olist.orders',
    'silver_olist.products',
    'silver_olist.sellers',
]

for t in tables:
    
    
    df = spark.table(t)
    
    if t == 'silver_olist.sellers':
        df.coalesce(1).write.mode('overwrite').format('parquet').option('header', 'true').save(f"/mnt/datalake/tmp/{t}")
    else:
        df.coalesce(1).write.mode('overwrite').format('csv').option('set',';').option('header', 'true').save(f"/mnt/datalake/tmp/{t}")
