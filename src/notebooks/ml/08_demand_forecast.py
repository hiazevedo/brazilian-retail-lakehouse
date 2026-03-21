# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Demand Forecast
# MAGIC
# MAGIC Treina um modelo LightGBM para prever a demanda futura por produto e loja.
# MAGIC Registra experimentos no MLflow e promove o melhor modelo para o Registry.
# MAGIC
# MAGIC **Execute após o 07_feature_engineering.**

# COMMAND ----------

# MAGIC %pip install lightgbm --quiet

# COMMAND ----------

import mlflow
import mlflow.sklearn
import lightgbm as lgb
import pandas as pd
import numpy as np
import os
from sklearn.model_selection import train_test_split
from sklearn.metrics import root_mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder

# Desabilita o autologging do MLflow — evita que ele tente configurar
# a integração Spark automaticamente (indisponível no Free Edition Serverless)
mlflow.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar features

# COMMAND ----------

# Lê a feature table gerada pelo notebook 07
df = spark.table("retail_lakehouse.ml_features.demand_features").toPandas()

print(f"✔ {len(df):,} linhas carregadas")
print(f"  Colunas: {list(df.columns)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparar dados para treino

# COMMAND ----------

# Codifica variáveis categóricas como números
# LightGBM aceita strings nativamente, mas o MLflow precisa de tipos consistentes
le_produto = LabelEncoder()
le_loja    = LabelEncoder()
le_status  = LabelEncoder()

df["produto_enc"] = le_produto.fit_transform(df["produto_id"])
df["loja_enc"]    = le_loja.fit_transform(df["loja_id"])
df["status_enc"]  = le_status.fit_transform(df["status_estoque"])

# Features usadas no modelo
FEATURES = [
    "produto_enc",
    "loja_enc",
    "media_qtd_7d",
    "media_qtd_30d",
    "media_receita_7d",
    "num_transacoes",
    "estoque_atual",
    "status_enc",
    "dia_semana",
    "mes",
]

TARGET = "target_qtd_7d"

X = df[FEATURES]
y = df[TARGET]

# Divisão treino/validação — 80% treino, 20% validação
X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"✔ Treino : {len(X_train):,} linhas")
print(f"✔ Validação: {len(X_val):,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Treinar modelo com MLflow

# COMMAND ----------

# Workaround para Databricks Free Edition Serverless:
# MLflow precisa de um UC Volume para armazenar artefatos temporários
spark.sql("CREATE VOLUME IF NOT EXISTS retail_lakehouse.ml_features.mlflow_tmp")
MLFLOW_TMP = "/Volumes/retail_lakehouse/ml_features/mlflow_tmp"
os.environ["MLFLOW_DFS_TMP"] = MLFLOW_TMP

# Experiment name dinâmico com o usuário corrente — padrão dos outros projetos
CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT_NAME = f"/Users/{CURRENT_USER}/demand_forecast"
mlflow.set_experiment(EXPERIMENT_NAME)

# Parâmetros do modelo — documentados para reprodutibilidade
PARAMS = {
    "n_estimators":   300,
    "learning_rate":  0.05,
    "max_depth":      6,
    "num_leaves":     31,
    "min_child_samples": 20,
    "subsample":      0.8,
    "colsample_bytree": 0.8,
    "random_state":   42,
    "verbose":        -1,
}

with mlflow.start_run(run_name="lgbm_demand_v1"):

    # Registra os parâmetros no MLflow
    mlflow.log_params(PARAMS)
    mlflow.log_param("feature_table", "ml_features.demand_features")
    mlflow.log_param("n_features", len(FEATURES))
    mlflow.log_param("n_train", len(X_train))

    # Treina o modelo
    modelo = lgb.LGBMRegressor(**PARAMS)
    modelo.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    # Avalia no conjunto de validação
    y_pred = modelo.predict(X_val)
    rmse   = root_mean_squared_error(y_val, y_pred)
    r2     = r2_score(y_val, y_pred)
    mae    = np.mean(np.abs(y_val - y_pred))

    # Registra as métricas no MLflow
    mlflow.log_metric("rmse",  round(rmse, 4))
    mlflow.log_metric("r2",    round(r2,   4))
    mlflow.log_metric("mae",   round(mae,  4))

    # LightGBM tem API sklearn-compatível — mlflow.sklearn.log_model funciona
    # sem depender do Model Registry (indisponível no Free Edition Serverless)
    # input_example removido — causa CONFIG_NOT_AVAILABLE no Serverless Free Edition
    mlflow.sklearn.log_model(modelo, "model")

    print("=" * 55)
    print("DEMAND FORECAST — RESULTADOS")
    print("=" * 55)
    print(f"RMSE : {rmse:.2f}  (erro médio em unidades)")
    print(f"MAE  : {mae:.2f}  (erro absoluto médio)")
    print(f"R²   : {r2:.4f} (1.0 = perfeito)")
    print()
    print(f"✔ Modelo salvo no experimento: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Importância das features

# COMMAND ----------

# Mostra quais features mais influenciaram o modelo
importancia = pd.DataFrame({
    "feature":    FEATURES,
    "importancia": modelo.feature_importances_,
}).sort_values("importancia", ascending=False)

print("\nImportância das features:")
for _, row in importancia.iterrows():
    barra = "█" * int(row["importancia"] / importancia["importancia"].max() * 30)
    print(f"  {row['feature']:<22} {barra} {row['importancia']:.0f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exemplo de previsão

# COMMAND ----------

# Demonstra o modelo fazendo uma previsão para as primeiras 5 lojas do PROD-0000
amostra = df[df["produto_id"] == "PROD-0000"].head(5)[FEATURES]
previsoes = modelo.predict(amostra)

print("\nExemplo de previsão — PROD-0000:")
print(f"{'Loja':<10} {'Real':>8} {'Previsto':>10}")
print("-" * 30)
for i, (real, prev) in enumerate(zip(df[df["produto_id"] == "PROD-0000"].head(5)[TARGET], previsoes)):
    loja = df[df["produto_id"] == "PROD-0000"].head(5)["loja_id"].iloc[i]
    print(f"{loja:<10} {real:>8.0f} {prev:>10.0f}")
