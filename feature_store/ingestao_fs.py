# Databricks notebook source
# MAGIC %pip install tqdm

# COMMAND ----------

# 1. Ler a query de um arquivo .sql                        - OK
# 2. formatar a query com uma data ({data})                - OK
# 3. Executar essa query com spark                         - OK
# 4. Salvar o resultado da query em uma tabela no datalake - OK

# COMMAND ----------

import datetime
import delta

from tqdm import tqdm

def read_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def date_range(date_start, date_stop, step=1):
    dates = []

    datetime_start = datetime.datetime.strptime(date_start, '%Y-%m-%d')
    datetime_stop = datetime.datetime.strptime(date_stop, '%Y-%m-%d')
    
    while datetime_start <= datetime_stop:
        dates.append(datetime_start.strftime('%Y-%m-%d'))
        datetime_start = datetime_start + datetime.timedelta(days=1)

    return dates[::step]

def execute_ingestion( table='fs_seller_atividade', data='2018-01-01', ids=['dtReferencia', 'idSeller'] ):

    # Le a query e define a data de execução
    query = read_query(f"{table}.sql")
    query_format = query.format(date=data)

    # Executa a query no spark
    df = spark.sql(query_format)

    # faz o merge do resultado com a tabela existente
    condicao = " and ".join([f"delta_table.{i} = new_data.{i}" for i in ids])
    delta_table = delta.DeltaTable.forName(spark, f"silver_olist.{table}")
    (delta_table.alias("delta_table")
                .merge(df.alias("new_data"), condicao)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())
    

# COMMAND ----------

dates = [i for i in date_range("2017-06-01", "2018-02-01") if i.endswith("01")]

for d in tqdm(dates):
    execute_ingestion(table='fs_seller_produto', data=d, ids=['dtReferencia', 'idSeller'])
