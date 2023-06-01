# Databricks notebook source
# MAGIC %pip install feature-engine

# COMMAND ----------

# DBTITLE 1,PySpark para Pandas
df = spark.table("analytics.asn.abt_olist_churn")
df = df.toPandas()
df.head(4)

# COMMAND ----------

# DBTITLE 1,Setup Features
# variáveis para identificação e variável resposta
target = 'flChurn'
id_columns = ['dtReferencia','idVendedor']

# Variáveis regressoras ou covariáveis ou features
features = df.columns.tolist()
features = list(set(features) - set(id_columns + [target]))
features.sort()

# Separação entre variáveis numéricas e categórias
cat_features = df[features].dtypes[df[features].dtypes == 'object'].index.tolist()
num_features = list(set(features) - set(cat_features))

# COMMAND ----------

# DBTITLE 1,Sample
from sklearn import model_selection

# Base out of time
df_oot = df[df['dtReferencia'] == '2018-02-01']

# Base de treino + teste
df_train = df[df['dtReferencia'] != '2018-02-01']

X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                                    df_train[target],
                                                                    test_size=0.2,
                                                                    random_state=42)

# COMMAND ----------

# DBTITLE 1,Explore
X_train[cat_features].describe()

# COMMAND ----------

X_train[cat_features].isna().sum()

# COMMAND ----------

# DBTITLE 1,Modify (Raiz)
# Maneira raiz
from feature_engine import encoding
from feature_engine import imputation

# Método de imputação de dados
cat_imputer = imputation.CategoricalImputer(variables=cat_features, fill_value='Faltante')
cat_imputer.fit(X_train, y_train)

# dado transformado para imput de categoria
X_transform = cat_imputer.transform(X_train)

# Método de encoding
mean_encoder = encoding.MeanEncoder(variables=['descCidade','descTopCategoria','descTopEstado'])
mean_encoder.fit(X_transform, y_train)

X_transform = mean_encoder.transform(X_transform)

# Método de OneHot
onehot_encoder = encoding.OneHotEncoder(variables=['descEstado'], drop_last=True)
onehot_encoder.fit(X_transform, y_train)

X_transform = onehot_encoder.transform(X_transform)

# COMMAND ----------

# DBTITLE 1,Modify (Pipeline)
# Maneira raiz
from feature_engine import encoding
from feature_engine import imputation
from sklearn import pipeline

# Método de imputação de dados
cat_imputer = imputation.CategoricalImputer(variables=cat_features, fill_value='Faltante')

# Método de encoding
mean_encoder = encoding.MeanEncoder(variables=['descCidade','descTopCategoria','descTopEstado'])

# Método de imputação para cidades não obervadas em treino
mean_encode_imputer = imputation.MeanMedianImputer(variables=['descCidade','descTopCategoria','descTopEstado'],
                                                   imputation_method='mean')

# Método de OneHot
onehot_encoder = encoding.OneHotEncoder(variables=['descEstado'], drop_last=True)

pipeline_transform = pipeline.Pipeline( [('Imputer de Categoria', cat_imputer),
                                         ('Média de Categoria', mean_encoder),
                                         ('Média para Categorias Transformadas', mean_encode_imputer),
                                         ('OneHot de Categoria', onehot_encoder), ])

pipeline_transform.fit(X_train, y_train)

# COMMAND ----------

X_transform = pipeline_transform.transform(X_test)
X_transform['descCidade'].isna().sum()
