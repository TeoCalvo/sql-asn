# Databricks notebook source
# MAGIC %pip install feature-engine scikit-plot

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

descritiva_num = X_train[num_features].describe().T
missing_columns =  descritiva_num[ descritiva_num['count'] < X_train.shape[0] ]
missing_columns['count'] / X_train.shape[0]
missing_columns

imput_999 = ['qtRazaoPedidoMesVsMes1',
             'qtMediaDiasEntregaDespacho',
             'qtRazaoReceitaMesVsMes1',
             'qtMediaDiasEntreVendas',
             'vlTempoMedioAvaliacao1M',
             'qtRazaoReceitaMesVsMedia',
             'vlTempoMedioAvaliacao',
             'vlTempoMedioAvaliacao3M',
             'avgTempoResposta3M',
             'qtDiasMediaEntregaPrevista',
             'avgTempoResposta1M',
             ]

imput_0 = ['qtMediaFotos',
           'pctMensagem3M',
           'vlMedioTamanhoNome',
           'avgTempoResposta',
           'vlMedioVolume1M',
            'pctMensagem',
            'vlMedioPeso',
            'vlMedioVolume',
            'vlMedioVolume3M',
            'pctMensagem1M',
            	]

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

from sklearn import linear_model
from sklearn import tree
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

# Método de imputação para variáveis numérias (999)
imputer_999 = imputation.ArbitraryNumberImputer(arbitrary_number=999, variables=imput_999)

# Método de imputação para variáveis numérias (0)
imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0, variables=imput_0)

# Nosso algoritmo!!!!
# model = linear_model.LogisticRegression(penalty='l2',solver='liblinear', max_iter=10000)
model = tree.DecisionTreeClassifier(min_samples_leaf=50, max_depth=10)

pipeline_transform = pipeline.Pipeline( [('Imputer de Categoria', cat_imputer),
                                         ('Média de Categoria', mean_encoder),
                                         ('Média para Categorias Transformadas', mean_encode_imputer),
                                         ('OneHot de Categoria', onehot_encoder),
                                         ('Imputacao 999', imputer_999),
                                         ('Imputacao 0', imputer_0),
                                         ('Modelo', model),
                                         ])

pipeline_transform.fit(X_train, y_train)

# COMMAND ----------

from sklearn import metrics

y_train_pred = pipeline_transform.predict(X_train)

acc_train = metrics.accuracy_score(y_train, y_train_pred)

y_train_proba = pipeline_transform.predict_proba(X_train)
roc_train = metrics.roc_auc_score(y_train, y_train_proba[:,1])

y_test_pred = pipeline_transform.predict(X_test)

acc_test = metrics.accuracy_score(y_test, y_test_pred)

y_test_proba = pipeline_transform.predict_proba(X_test)
roc_test = metrics.roc_auc_score(y_test, y_test_proba[:,1])

print("Acc train:", acc_train)
print("AUC train:", roc_train)

print("Acc test:", acc_test)
print("AUC test:", roc_test)

# COMMAND ----------

import scikitplot as skplt

skplt.metrics.plot_roc_curve(y_train, y_train_proba)

# COMMAND ----------

skplt.metrics.plot_ks_statistic(y_train, y_train_proba)

# COMMAND ----------

skplt.metrics.plot_cumulative_gain(y_train, y_train_proba)

# COMMAND ----------

skplt.metrics.plot_lift_curve(y_train, y_train_proba)

# COMMAND ----------

df_lift = y_train.reset_index()
df_lift['proba'] = y_train_proba[:,1]
df_lift = df_lift.sort_values(by='proba', ascending=False)
df_lift_20 = df_lift.iloc[:int(df_lift.shape[0]*0.2)]

df_lift_20['flChurn'].mean() / df_lift['flChurn'].mean()
