# Databricks notebook source
# MAGIC %md
# MAGIC # 15 - A/B Test de Modelos
# MAGIC
# MAGIC Compara duas versões do fraud_detector e do demand_forecast sobre o mesmo
# MAGIC conjunto de teste para decidir qual versão vai para "produção".
# MAGIC
# MAGIC **Metodologia:**
# MAGIC - v1 = modelo atual (carregado do MLflow pelo run mais recente)
# MAGIC - v2 = challenger (treinado agora com hiperparâmetros diferentes)
# MAGIC - Ambos são avaliados sobre o **mesmo X_test** — comparação justa
# MAGIC - O vencedor é registrado em `gold.ab_test_results`
# MAGIC
# MAGIC **Execute após o 09_fraud_detector e o 08_demand_forecast.**

# COMMAND ----------

# MAGIC %pip install lightgbm --quiet

# COMMAND ----------

dbutils.library.restartPython()

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
    recall_score, root_mean_squared_error, mean_absolute_error, r2_score
)
from pyspark.sql import functions as F
from datetime import date

mlflow.autolog(disable=True)

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.ab_test_results (
        test_date       DATE    COMMENT 'Data do teste',
        modelo          STRING  COMMENT 'Nome do modelo testado',
        versao          STRING  COMMENT 'v1 (champion) ou v2 (challenger)',
        run_id          STRING  COMMENT 'Run ID do MLflow',
        metrica_1_nome  STRING  COMMENT 'Nome da métrica principal (F1 ou RMSE)',
        metrica_1_valor DOUBLE  COMMENT 'Valor da métrica principal',
        metrica_2_nome  STRING  COMMENT 'Nome da métrica secundária (AUC ou MAE)',
        metrica_2_valor DOUBLE  COMMENT 'Valor da métrica secundária',
        metrica_3_nome  STRING  COMMENT 'Nome da métrica terciária (Precision ou R2)',
        metrica_3_valor DOUBLE  COMMENT 'Valor da métrica terciária',
        vencedor        BOOLEAN COMMENT 'True se esta versão venceu o teste',
        params_json     STRING  COMMENT 'Hiperparâmetros usados (JSON)'
    )
    COMMENT 'Histórico de resultados de testes A/B entre versões de modelos'
""")

print("✔ Tabela gold.ab_test_results criada")

# COMMAND ----------

spark.sql("CREATE VOLUME IF NOT EXISTS retail_lakehouse.ml_features.mlflow_tmp")
MLFLOW_TMP   = "/Volumes/retail_lakehouse/ml_features/mlflow_tmp"
os.environ["MLFLOW_DFS_TMP"] = MLFLOW_TMP

CURRENT_USER = spark.sql("SELECT current_user()").collect()[0][0]
TODAY        = date.today().isoformat()

results = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. A/B Test — Fraud Detector
# MAGIC
# MAGIC **v1** — champion: run mais recente do experimento `fraud_detector`
# MAGIC
# MAGIC **v2** — challenger: mais árvores, learning rate menor, mais regularização.
# MAGIC A ideia é verificar se um modelo mais conservador generaliza melhor.

# COMMAND ----------

FEATURES_FRAUD = [
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

df_fraud = spark.table("retail_lakehouse.ml_features.fraud_features").toPandas()

X_fraud = df_fraud[FEATURES_FRAUD]
y_fraud = df_fraud["label"]

# Mesmo split para v1 e v2 — comparação justa
X_train_f, X_test_f, y_train_f, y_test_f = train_test_split(
    X_fraud, y_fraud, test_size=0.2, random_state=42, stratify=y_fraud
)

print(f"Fraud — Treino: {len(X_train_f):,} | Teste: {len(X_test_f):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. v1 — Champion (modelo existente no MLflow)

# COMMAND ----------

EXPERIMENT_FRAUD = f"/Users/{CURRENT_USER}/fraud_detector"
mlflow.set_experiment(EXPERIMENT_FRAUD)

runs_fraud = mlflow.search_runs(
    experiment_names=[EXPERIMENT_FRAUD],
    order_by=["start_time DESC"],
)

run_id_v1 = runs_fraud.iloc[0]["run_id"]
model_v1_fraud = mlflow.sklearn.load_model(f"runs:/{run_id_v1}/model")

y_prob_v1 = model_v1_fraud.predict_proba(X_test_f)[:, 1]
y_pred_v1 = (y_prob_v1 >= 0.5).astype(int)

f1_v1        = round(f1_score(y_test_f, y_pred_v1),        4)
auc_v1       = round(roc_auc_score(y_test_f, y_prob_v1),   4)
precision_v1 = round(precision_score(y_test_f, y_pred_v1), 4)

print(f"Fraud v1 (champion) — F1={f1_v1} | AUC={auc_v1} | Precision={precision_v1}")

results.append({
    "test_date":       TODAY,
    "modelo":          "fraud_detector",
    "versao":          "v1",
    "run_id":          run_id_v1,
    "metrica_1_nome":  "f1",
    "metrica_1_valor": float(f1_v1),
    "metrica_2_nome":  "auc_roc",
    "metrica_2_valor": float(auc_v1),
    "metrica_3_nome":  "precision",
    "metrica_3_valor": float(precision_v1),
    "vencedor":        False,   # atualizado após comparação
    "params_json":     str(runs_fraud.iloc[0].get("params.n_estimators", "300")),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. v2 — Challenger (treinado agora com novos hiperparâmetros)

# COMMAND ----------

scale_pos_weight = int((y_train_f == 0).sum() / (y_train_f == 1).sum())

PARAMS_V2 = {
    "n_estimators":      500,     # mais árvores (v1=300)
    "learning_rate":     0.03,    # aprendizado mais lento (v1=0.05)
    "max_depth":         6,
    "num_leaves":        63,      # mais folhas = mais complexidade (v1=31)
    "min_child_samples": 30,      # mais dados por folha = menos overfitting (v1=20)
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "reg_lambda":        0.5,     # regularização L2 (v1 não usava)
    "scale_pos_weight":  scale_pos_weight,
    "random_state":      42,
    "verbose":           -1,
}

with mlflow.start_run(run_name="lgbm_fraud_v2_challenger") as run_v2_fraud:
    run_id_v2_fraud = run_v2_fraud.info.run_id

    mlflow.log_params(PARAMS_V2)
    mlflow.log_param("ab_test", "challenger")

    model_v2_fraud = lgb.LGBMClassifier(**PARAMS_V2)
    model_v2_fraud.fit(
        X_train_f, y_train_f,
        eval_set=[(X_test_f, y_test_f)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    y_prob_v2 = model_v2_fraud.predict_proba(X_test_f)[:, 1]
    y_pred_v2 = (y_prob_v2 >= 0.5).astype(int)

    f1_v2        = round(f1_score(y_test_f, y_pred_v2),        4)
    auc_v2       = round(roc_auc_score(y_test_f, y_prob_v2),   4)
    precision_v2 = round(precision_score(y_test_f, y_pred_v2), 4)

    mlflow.log_metric("f1",        f1_v2)
    mlflow.log_metric("auc_roc",   auc_v2)
    mlflow.log_metric("precision", precision_v2)

    mlflow.sklearn.log_model(model_v2_fraud, "model")

print(f"Fraud v2 (challenger) — F1={f1_v2} | AUC={auc_v2} | Precision={precision_v2}")

results.append({
    "test_date":       TODAY,
    "modelo":          "fraud_detector",
    "versao":          "v2",
    "run_id":          run_id_v2_fraud,
    "metrica_1_nome":  "f1",
    "metrica_1_valor": float(f1_v2),
    "metrica_2_nome":  "auc_roc",
    "metrica_2_valor": float(auc_v2),
    "metrica_3_nome":  "precision",
    "metrica_3_valor": float(precision_v2),
    "vencedor":        False,
    "params_json":     str(PARAMS_V2),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. Veredicto — Fraud Detector

# COMMAND ----------

# Critério: F1-Score como métrica principal de desempate
vencedor_fraud = "v2" if f1_v2 > f1_v1 else "v1"
delta_f1       = round(f1_v2 - f1_v1, 4)
delta_auc      = round(auc_v2 - auc_v1, 4)

# Marca o vencedor na lista de results
for r in results:
    if r["modelo"] == "fraud_detector" and r["versao"] == vencedor_fraud:
        r["vencedor"] = True

print("=" * 55)
print("VEREDICTO — FRAUD DETECTOR")
print("=" * 55)
print(f"  F1        v1={f1_v1}  |  v2={f1_v2}  |  Δ={delta_f1:+.4f}")
print(f"  AUC-ROC   v1={auc_v1}  |  v2={auc_v2}  |  Δ={delta_auc:+.4f}")
print(f"  Precision v1={precision_v1}  |  v2={precision_v2}")
print()
icone = "🏆" if vencedor_fraud == "v2" else "👑"
print(f"  {icone} VENCEDOR: {vencedor_fraud.upper()} ({'challenger promovido' if vencedor_fraud == 'v2' else 'champion mantido'})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. A/B Test — Demand Forecast
# MAGIC
# MAGIC **v1** — champion: run mais recente do experimento `demand_forecast`
# MAGIC
# MAGIC **v2** — challenger: mais árvores e regularização — verifica se reduz overfitting
# MAGIC (o R²=0.9999 da v1 pode indicar que o modelo memorizou os dados de treino)

# COMMAND ----------

df_demand = spark.table("retail_lakehouse.ml_features.demand_features").toPandas()

# Reaplica o mesmo encoding do treinamento (08_demand_forecast)
from sklearn.preprocessing import LabelEncoder
le_product = LabelEncoder()
le_store   = LabelEncoder()
le_status  = LabelEncoder()

df_demand["produto_enc"] = le_product.fit_transform(df_demand["produto_id"])
df_demand["loja_enc"]    = le_store.fit_transform(df_demand["loja_id"])
df_demand["status_enc"]  = le_status.fit_transform(df_demand["status_estoque"])

FEATURES_DEMAND = [
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

X_demand = df_demand[FEATURES_DEMAND].fillna(0)
y_demand = df_demand["target_qtd_7d"]

X_train_d, X_test_d, y_train_d, y_test_d = train_test_split(
    X_demand, y_demand, test_size=0.2, random_state=42
)

print(f"Demand — Treino: {len(X_train_d):,} | Teste: {len(X_test_d):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4a. v1 — Champion

# COMMAND ----------

EXPERIMENT_DEMAND = f"/Users/{CURRENT_USER}/demand_forecast"
mlflow.set_experiment(EXPERIMENT_DEMAND)

runs_demand = mlflow.search_runs(
    experiment_names=[EXPERIMENT_DEMAND],
    order_by=["start_time DESC"],
)

run_id_v1_demand = runs_demand.iloc[0]["run_id"]
model_v1_demand = mlflow.sklearn.load_model(f"runs:/{run_id_v1_demand}/model")

y_pred_v1_d = model_v1_demand.predict(X_test_d)

rmse_v1 = round(root_mean_squared_error(y_test_d, y_pred_v1_d), 4)
mae_v1  = round(mean_absolute_error(y_test_d, y_pred_v1_d),     4)
r2_v1   = round(r2_score(y_test_d, y_pred_v1_d),                4)

print(f"Demand v1 (champion) — RMSE={rmse_v1} | MAE={mae_v1} | R²={r2_v1}")

results.append({
    "test_date":       TODAY,
    "modelo":          "demand_forecast",
    "versao":          "v1",
    "run_id":          run_id_v1_demand,
    "metrica_1_nome":  "rmse",
    "metrica_1_valor": float(rmse_v1),
    "metrica_2_nome":  "mae",
    "metrica_2_valor": float(mae_v1),
    "metrica_3_nome":  "r2",
    "metrica_3_valor": float(r2_v1),
    "vencedor":        False,
    "params_json":     str(runs_demand.iloc[0].get("params.n_estimators", "300")),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. v2 — Challenger

# COMMAND ----------

PARAMS_V2_DEMAND = {
    "n_estimators":      500,
    "learning_rate":     0.03,
    "max_depth":         5,       # mais raso que v1 (6) — evita overfitting
    "num_leaves":        31,
    "min_child_samples": 30,
    "subsample":         0.8,
    "colsample_bytree":  0.8,
    "reg_lambda":        1.0,     # regularização L2 mais forte
    "random_state":      42,
    "verbose":           -1,
}

mlflow.set_experiment(EXPERIMENT_DEMAND)

with mlflow.start_run(run_name="lgbm_demand_v2_challenger") as run_v2_demand:
    run_id_v2_demand = run_v2_demand.info.run_id

    mlflow.log_params(PARAMS_V2_DEMAND)
    mlflow.log_param("ab_test", "challenger")

    model_v2_demand = lgb.LGBMRegressor(**PARAMS_V2_DEMAND)
    model_v2_demand.fit(
        X_train_d, y_train_d,
        eval_set=[(X_test_d, y_test_d)],
        callbacks=[lgb.early_stopping(50, verbose=False)],
    )

    y_pred_v2_d = model_v2_demand.predict(X_test_d)

    rmse_v2 = round(root_mean_squared_error(y_test_d, y_pred_v2_d), 4)
    mae_v2  = round(mean_absolute_error(y_test_d, y_pred_v2_d),     4)
    r2_v2   = round(r2_score(y_test_d, y_pred_v2_d),                4)

    mlflow.log_metric("rmse", rmse_v2)
    mlflow.log_metric("mae",  mae_v2)
    mlflow.log_metric("r2",   r2_v2)

    mlflow.sklearn.log_model(model_v2_demand, "model")

print(f"Demand v2 (challenger) — RMSE={rmse_v2} | MAE={mae_v2} | R²={r2_v2}")

results.append({
    "test_date":       TODAY,
    "modelo":          "demand_forecast",
    "versao":          "v2",
    "run_id":          run_id_v2_demand,
    "metrica_1_nome":  "rmse",
    "metrica_1_valor": float(rmse_v2),
    "metrica_2_nome":  "mae",
    "metrica_2_valor": float(mae_v2),
    "metrica_3_nome":  "r2",
    "metrica_3_valor": float(r2_v2),
    "vencedor":        False,
    "params_json":     str(PARAMS_V2_DEMAND),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. Veredicto — Demand Forecast

# COMMAND ----------

# Critério: RMSE menor = melhor para regressão
vencedor_demand = "v2" if rmse_v2 < rmse_v1 else "v1"
delta_rmse      = round(rmse_v2 - rmse_v1, 4)
delta_r2        = round(r2_v2 - r2_v1, 4)

for r in results:
    if r["modelo"] == "demand_forecast" and r["versao"] == vencedor_demand:
        r["vencedor"] = True

print("=" * 55)
print("VEREDICTO — DEMAND FORECAST")
print("=" * 55)
print(f"  RMSE  v1={rmse_v1}  |  v2={rmse_v2}  |  Δ={delta_rmse:+.4f}")
print(f"  MAE   v1={mae_v1}  |  v2={mae_v2}  |  Δ={round(mae_v2-mae_v1,4):+.4f}")
print(f"  R²    v1={r2_v1}  |  v2={r2_v2}  |  Δ={delta_r2:+.4f}")
print()
icone = "🏆" if vencedor_demand == "v2" else "👑"
print(f"  {icone} VENCEDOR: {vencedor_demand.upper()} ({'challenger promovido' if vencedor_demand == 'v2' else 'champion mantido'})")

# COMMAND ----------

df_ab = spark.createDataFrame(results) \
    .withColumn("test_date", F.to_date(F.col("test_date")))

df_ab.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retail_lakehouse.gold.ab_test_results")

print(f"\n✔ {len(results)} resultados registrados em gold.ab_test_results")

# COMMAND ----------

display(spark.sql("""
    SELECT
        modelo,
        versao,
        metrica_1_nome                              AS metrica_principal,
        metrica_1_valor                             AS valor_principal,
        metrica_2_nome                              AS metrica_secundaria,
        metrica_2_valor                             AS valor_secundario,
        metrica_3_nome                              AS metrica_terciaria,
        metrica_3_valor                             AS valor_terciario,
        CASE WHEN vencedor THEN '🏆 VENCEDOR' ELSE '—' END AS resultado,
        test_date
    FROM retail_lakehouse.gold.ab_test_results
    WHERE test_date = CURRENT_DATE
    ORDER BY modelo, versao
"""))
