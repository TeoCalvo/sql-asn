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

def table_exists(database, table):
    count = (spark.sql(f"SHOW TABLES FROM {database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1

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


def save_first_load(df, table_full_name):
    (df.write
        .format('delta')
        .mode('overwrite')
        .option('overwriteSchema', 'true')
        .partitionBy('dtReferencia')
        .saveAsTable(table_full_name)
        )


def save_upsert(df, delta_table, ids):
    # faz o merge do resultado com a tabela existente
    condicao = " and ".join([f"delta_table.{i} = new_data.{i}" for i in ids])
    (delta_table.alias("delta_table")
                .merge(df.alias("new_data"), condicao)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute())


def execute_ingestion(database, table, ids, dates):
    query = read_query(f"{table}.sql")

    if not table_exists(database, table):
        print("Executando a primeira carga...")
        df = spark.sql(query.format(date=dates.pop(0)))
        save_first_load(df, f'{database}.{table}')

    print("Executando cargas incrementais...")
    delta_table = delta.DeltaTable.forName(spark, f'{database}.{table}')
    for d in dates:
        df = spark.sql(query.format(date=d))
        save_upsert(df, delta_table, ids)


    

# COMMAND ----------

table_name = dbutils.widgets.get('table_name')
ids = dbutils.widgets.get('ids').split(",")
date_start = dbutils.widgets.get('date_start')
date_stop = dbutils.widgets.get('date_stop')

dates = [i for i in date_range(date_start, date_stop) if i.endswith("01")]

execute_ingestion('analytics.asn', table_name, ids, dates)
