# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Fraud Detector
# MAGIC
# MAGIC Treina um modelo LightGBM para classificar transações suspeitas.
# MAGIC Registra experimentos no MLflow com métricas de classificação.
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
from sklearn.metrics import (
    f1_score, roc_auc_score, precision_score,
    recall_score, confusion_matrix, classification_report
)
from sklearn.preprocessing import LabelEncoder

# Desabilita autologging — evita lookup de spark.mlflow.modelRegistryUri
# no Databricks Free Edition Serverless
mlflow.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar features

# COMMAND ----------

df = spark.table("retail_lakehouse.ml_features.fraud_features").toPandas()

total   = len(df)
fraudes = df["label"].sum()
print(f"✔ {total:,} transações carregadas")
print(f"  Fraudes : {fraudes:,} ({fraudes/total*100:.2f}%)")
print(f"  Normais : {total - fraudes:,} ({(total-fraudes)/total*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Preparar dados para treino

# COMMAND ----------

FEATURES = [
    "valor_total",
    "ticket_medio_cliente",
    "desvio_padrao_cliente",
    "desvio_do_ticket",
    "parcelas",
    "metodo_num",
    "status_num",
    "hora_dia",
    "dia_semana",
    "total_compras_cliente",
]

TARGET = "label"

X = df[FEATURES]
y = df[TARGET]

# Divisão estratificada — garante a mesma proporção de fraudes em treino e validação
X_train, X_val, y_train, y_val = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"✔ Treino    : {len(X_train):,} | Fraudes: {y_train.sum():,} ({y_train.mean()*100:.2f}%)")
print(f"✔ Validação : {len(X_val):,} | Fraudes: {y_val.sum():,} ({y_val.mean()*100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Treinar modelo com MLflow

# COMMAND ----------

# Workaround para artefatos MLflow no Databricks Free Edition Serverless
spark.sql("CREATE VOLUME IF NOT EXISTS retail_lakehouse.ml_features.mlflow_tmp")
MLFLOW_TMP = "/Volumes/retail_lakehouse/ml_features/mlflow_tmp"
os.environ["MLFLOW_DFS_TMP"] = MLFLOW_TMP

CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
EXPERIMENT_NAME = f"/Users/{CURRENT_USER}/fraud_detector"
mlflow.set_experiment(EXPERIMENT_NAME)

# scale_pos_weight compensa o desbalanceamento de classes:
# se há 10x mais normais que fraudes, o modelo dá 10x mais peso às fraudes
scale_pos_weight = int((y_train == 0).sum() / (y_train == 1).sum())
print(f"✔ scale_pos_weight: {scale_pos_weight} (compensa desbalanceamento de classes)")

PARAMS = {
    "n_estimators":      300,
    "learning_rate":     0.05,
    "max_depth":         6,
    "num_leaves":        31,
    "min_child_samples": 20,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "scale_pos_weight":  scale_pos_weight,
    "random_state":      42,
    "verbose":           -1,
}

with mlflow.start_run(run_name="lgbm_fraud_v1"):

    mlflow.log_params(PARAMS)
    mlflow.log_param("feature_table", "ml_features.fraud_features")
    mlflow.log_param("n_features", len(FEATURES))
    mlflow.log_param("n_train", len(X_train))
    mlflow.log_param("fraude_pct_treino", round(y_train.mean() * 100, 2))

    modelo = lgb.LGBMClassifier(**PARAMS)
    modelo.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    # Probabilidades e predições
    y_prob = modelo.predict_proba(X_val)[:, 1]
    y_pred = modelo.predict(X_val)

    # Métricas de classificação
    f1        = f1_score(y_val, y_pred)
    auc       = roc_auc_score(y_val, y_prob)
    precision = precision_score(y_val, y_pred)
    recall    = recall_score(y_val, y_pred)

    mlflow.log_metric("f1",        round(f1,        4))
    mlflow.log_metric("auc_roc",   round(auc,       4))
    mlflow.log_metric("precision", round(precision, 4))
    mlflow.log_metric("recall",    round(recall,    4))

    mlflow.sklearn.log_model(
        modelo, "model",
        input_example=X_train.iloc[:5]
    )

    print("=" * 55)
    print("FRAUD DETECTOR — RESULTADOS")
    print("=" * 55)
    print(f"F1-Score  : {f1:.4f}  (equilíbrio entre precisão e recall)")
    print(f"AUC-ROC   : {auc:.4f}  (capacidade de separar classes)")
    print(f"Precision : {precision:.4f}  (dos alertados, quantos são fraude?)")
    print(f"Recall    : {recall:.4f}  (das fraudes, quantas foram detectadas?)")
    print(f"\n✔ Modelo salvo no experimento: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Matriz de Confusão

# COMMAND ----------

# Mostra onde o modelo erra — fundamental para avaliar detectores de fraude
cm = confusion_matrix(y_val, y_pred)

print("\nMatriz de Confusão:")
print(f"                  Previsto Normal  Previsto Fraude")
print(f"  Real Normal   :     {cm[0][0]:>8,}        {cm[0][1]:>8,}")
print(f"  Real Fraude   :     {cm[1][0]:>8,}        {cm[1][1]:>8,}")
print()
print(f"  Falsos Negativos (fraudes não detectadas): {cm[1][0]:,}")
print(f"  Falsos Positivos (alertas incorretos)    : {cm[0][1]:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Importância das features

# COMMAND ----------

importancia = pd.DataFrame({
    "feature":     FEATURES,
    "importancia": modelo.feature_importances_,
}).sort_values("importancia", ascending=False)

print("\nImportância das features:")
for _, row in importancia.iterrows():
    barra = "█" * int(row["importancia"] / importancia["importancia"].max() * 30)
    print(f"  {row['feature']:<25} {barra} {row['importancia']:.0f}")
