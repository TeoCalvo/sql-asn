# Databricks notebook source
# MAGIC %pip install feature-engine

# COMMAND ----------

# Le o modelo de algum lugar físico
model = pd.read_pickle("model_rf.pkl")

# Extrai a base onde será realizado a escoragem
df = spark.table("analytics.asn.abt_olist_churn").filter("dtReferencia = '2018-02-01'").toPandas()

# Faz escoragem e salva em uma coluna
df_predict = df[['dtReferencia','idVendedor']].copy()
df_predict['proba'] = model['model'].predict_proba(df[model['features']])[:,1]
df_predict = df_predict.sort_values('proba', ascending=False)
