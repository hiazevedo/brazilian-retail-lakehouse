# Databricks notebook source
# MAGIC %md
# MAGIC # 14 - Batch Scoring
# MAGIC
# MAGIC Aplica os modelos treinados sobre as feature tables e persiste as predições em tabelas Gold.
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC - Carrega o run mais recente de cada experimento MLflow (sem Model Registry)
# MAGIC - Aplica `fraud_detector` sobre `ml_features.fraud_features` → `gold.fraud_scores`
# MAGIC - Aplica `demand_forecast` sobre `ml_features.demand_features` → `gold.demand_scores`
# MAGIC
# MAGIC **Execute após o 07_feature_engineering, 08_demand_forecast e 09_fraud_detector.**

# COMMAND ----------

# MAGIC %pip install lightgbm --quiet

# COMMAND ----------

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar tabelas de destino

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.fraud_scores (
        scored_at        TIMESTAMP COMMENT 'Momento em que o score foi gerado',
        venda_id         STRING    COMMENT 'Identificador da venda',
        cliente_id       STRING    COMMENT 'Identificador do cliente',
        valor_total      DOUBLE    COMMENT 'Valor total da transação',
        parcelas         INT       COMMENT 'Número de parcelas',
        hora_dia         INT       COMMENT 'Hora do dia da transação',
        prob_fraude      DOUBLE    COMMENT 'Probabilidade de fraude (0 a 1)',
        predicao         STRING    COMMENT 'FRAUDE ou NORMAL',
        modelo_run_id    STRING    COMMENT 'Run ID do MLflow usado para gerar o score'
    )
    COMMENT 'Predições do fraud_detector aplicadas em batch'
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.demand_scores (
        scored_at         TIMESTAMP COMMENT 'Momento em que o score foi gerado',
        produto_id        STRING    COMMENT 'Identificador do produto',
        loja_id           STRING    COMMENT 'Identificador da loja',
        media_qtd_7d      DOUBLE    COMMENT 'Média de quantidade vendida nos últimos 7 dias',
        media_qtd_30d     DOUBLE    COMMENT 'Média de quantidade vendida nos últimos 30 dias',
        estoque_atual     DOUBLE    COMMENT 'Quantidade atual em estoque',
        previsao_demanda  DOUBLE    COMMENT 'Quantidade prevista para o próximo período',
        modelo_run_id     STRING    COMMENT 'Run ID do MLflow usado para gerar o score'
    )
    COMMENT 'Predições do demand_forecast aplicadas em batch'
""")

print("✔ Tabelas gold.fraud_scores e gold.demand_scores criadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Função auxiliar: buscar run mais recente

# COMMAND ----------

def buscar_run_mais_recente(experiment_name: str) -> tuple[str, object]:
    """
    Busca o run mais recente de um experimento MLflow e retorna
    o run_id e o modelo carregado.

    Como não temos Model Registry no Free Edition, usamos search_runs()
    ordenado por start_time para pegar sempre o modelo mais atual.
    """
    current_user = (
        spark.sql("SELECT current_user() AS u").collect()[0]["u"]
    )
    full_name = f"/Users/{current_user}/{experiment_name}"

    runs = mlflow.search_runs(
        experiment_names=[full_name],
        order_by=["start_time DESC"],
    )

    if runs.empty:
        raise ValueError(f"Nenhum run encontrado para o experimento: {full_name}")

    run_id = runs.iloc[0]["run_id"]
    modelo = mlflow.sklearn.load_model(f"runs:/{run_id}/model")

    print(f"  Experimento : {full_name}")
    print(f"  Run ID      : {run_id}")
    print(f"  Iniciado em : {runs.iloc[0]['start_time']}")

    return run_id, modelo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scoring — Fraud Detector

# COMMAND ----------

print("BATCH SCORING — FRAUD DETECTOR")
print("=" * 55)

run_id_fraud, modelo_fraud = buscar_run_mais_recente("fraud_detector")

# Carrega a feature table completa
df_fraud = spark.table("retail_lakehouse.ml_features.fraud_features").toPandas()

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

X_fraud = df_fraud[FEATURES_FRAUD].fillna(0)

# predict_proba retorna [prob_normal, prob_fraude] — pegamos a coluna 1
prob_fraude = modelo_fraud.predict_proba(X_fraud)[:, 1]

df_fraud["prob_fraude"]   = prob_fraude.round(4)
df_fraud["predicao"]      = np.where(prob_fraude >= 0.5, "FRAUDE", "NORMAL")
df_fraud["modelo_run_id"] = run_id_fraud
df_fraud["scored_at"]     = datetime.utcnow()

# Seleciona apenas as colunas da tabela de destino
colunas_fraud = [
    "scored_at", "venda_id", "cliente_id",
    "valor_total", "parcelas", "hora_dia",
    "prob_fraude", "predicao", "modelo_run_id",
]

df_scores_fraud = spark.createDataFrame(df_fraud[colunas_fraud]) \
    .withColumn("scored_at", F.to_timestamp(F.col("scored_at"))) \
    .withColumn("parcelas",  F.col("parcelas").cast("int"))

df_scores_fraud.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_lakehouse.gold.fraud_scores")

n_fraudes = int((prob_fraude >= 0.5).sum())
print(f"\n✔ {len(df_fraud):,} transações pontuadas")
print(f"  Predições FRAUDE : {n_fraudes:,} ({n_fraudes / len(df_fraud) * 100:.1f}%)")
print(f"  Predições NORMAL : {len(df_fraud) - n_fraudes:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Scoring — Demand Forecast

# COMMAND ----------

print("\nBATCH SCORING — DEMAND FORECAST")
print("=" * 55)

run_id_demand, modelo_demand = buscar_run_mais_recente("demand_forecast")

df_demand = spark.table("retail_lakehouse.ml_features.demand_features").toPandas()

# Reaplica o mesmo encoding do treinamento (08_demand_forecast)
# LabelEncoder precisa ser fit nos mesmos dados para gerar os mesmos códigos
from sklearn.preprocessing import LabelEncoder
le_produto = LabelEncoder()
le_loja    = LabelEncoder()
le_status  = LabelEncoder()

df_demand["produto_enc"] = le_produto.fit_transform(df_demand["produto_id"])
df_demand["loja_enc"]    = le_loja.fit_transform(df_demand["loja_id"])
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

previsao = modelo_demand.predict(X_demand)

df_demand["previsao_demanda"] = previsao.round(2)
df_demand["modelo_run_id"]    = run_id_demand
df_demand["scored_at"]        = datetime.utcnow()

colunas_demand = [
    "scored_at", "produto_id", "loja_id",
    "media_qtd_7d", "media_qtd_30d", "estoque_atual",
    "previsao_demanda", "modelo_run_id",
]

df_scores_demand = spark.createDataFrame(df_demand[colunas_demand]) \
    .withColumn("scored_at", F.to_timestamp(F.col("scored_at")))

df_scores_demand.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_lakehouse.gold.demand_scores")

print(f"\n✔ {len(df_demand):,} combinações produto/loja pontuadas")
print(f"  Demanda prevista média : {previsao.mean():.1f} unidades")
print(f"  Demanda prevista máx  : {previsao.max():.1f} unidades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo dos scores gerados

# COMMAND ----------

display(spark.sql("""
    SELECT
        'fraud_detector'                           AS modelo,
        COUNT(*)                                   AS transacoes_pontuadas,
        COUNT_IF(predicao = 'FRAUDE')              AS predito_fraude,
        COUNT_IF(predicao = 'NORMAL')              AS predito_normal,
        ROUND(AVG(prob_fraude), 4)                 AS prob_media,
        ROUND(MAX(prob_fraude), 4)                 AS prob_maxima,
        MAX(scored_at)                             AS ultima_execucao
    FROM retail_lakehouse.gold.fraud_scores

    UNION ALL

    SELECT
        'demand_forecast'                          AS modelo,
        COUNT(*)                                   AS transacoes_pontuadas,
        NULL                                       AS predito_fraude,
        NULL                                       AS predito_normal,
        ROUND(AVG(previsao_demanda), 2)            AS prob_media,
        ROUND(MAX(previsao_demanda), 2)            AS prob_maxima,
        MAX(scored_at)                             AS ultima_execucao
    FROM retail_lakehouse.gold.demand_scores
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Top 20 transações com maior risco de fraude

# COMMAND ----------

display(spark.sql("""
    SELECT
        venda_id,
        cliente_id,
        ROUND(valor_total, 2)  AS valor_total,
        parcelas,
        hora_dia,
        ROUND(prob_fraude, 4)  AS prob_fraude,
        predicao
    FROM retail_lakehouse.gold.fraud_scores
    ORDER BY prob_fraude DESC
    LIMIT 20
"""))
