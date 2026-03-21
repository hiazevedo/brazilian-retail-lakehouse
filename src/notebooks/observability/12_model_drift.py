# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Model Drift Monitor
# MAGIC
# MAGIC Detecta mudanças na distribuição dos dados de entrada em relação ao momento do treino.
# MAGIC Usa o PSI (Population Stability Index) para quantificar o drift por feature.
# MAGIC
# MAGIC **PSI < 0.1** → estável | **0.1–0.2** → monitorar | **> 0.2** → retreinar
# MAGIC
# MAGIC **Execute após o 09_fraud_detector e o 07_feature_engineering.**

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar tabela de histórico de drift

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.observability.model_drift (
        check_date      DATE      COMMENT 'Data da verificação',
        modelo          STRING    COMMENT 'Nome do modelo monitorado',
        feature         STRING    COMMENT 'Feature verificada',
        psi             DOUBLE    COMMENT 'Population Stability Index',
        drift_status    STRING    COMMENT 'ESTAVEL / MONITORAR / RETREINAR',
        media_treino    DOUBLE    COMMENT 'Média da feature no conjunto de treino',
        media_atual     DOUBLE    COMMENT 'Média atual da feature em produção',
        desvio_treino   DOUBLE    COMMENT 'Desvio padrão no treino',
        desvio_atual    DOUBLE    COMMENT 'Desvio padrão atual'
    )
    COMMENT 'Histórico de drift dos modelos de ML'
""")

print("✔ Tabela observability.model_drift criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Função PSI

# COMMAND ----------

def calcular_psi(base: np.ndarray, atual: np.ndarray, bins: int = 10) -> float:
    """
    Calcula o PSI entre a distribuição de treino (base) e a distribuição atual.

    O PSI compara os histogramas das duas distribuições:
    - Define os bins com base na distribuição de treino
    - Calcula a proporção de observações em cada bin para treino e atual
    - PSI = SUM((atual% - base%) * ln(atual% / base%))

    Valores próximos de 0 indicam distribuições similares.
    """
    # Define os bins usando os percentis da distribuição base (treino)
    breakpoints = np.percentile(base, np.linspace(0, 100, bins + 1))
    breakpoints = np.unique(breakpoints)  # remove duplicatas

    # Conta observações em cada bin
    base_counts,  _ = np.histogram(base,  bins=breakpoints)
    atual_counts, _ = np.histogram(atual, bins=breakpoints)

    # Converte para proporções — adiciona epsilon para evitar divisão por zero
    eps = 1e-6
    base_pct  = base_counts  / len(base)  + eps
    atual_pct = atual_counts / len(atual) + eps

    # Fórmula do PSI
    psi = np.sum((atual_pct - base_pct) * np.log(atual_pct / base_pct))
    return round(float(psi), 4)


def classificar_drift(psi: float) -> str:
    if psi < 0.1:
        return "ESTAVEL"
    elif psi < 0.2:
        return "MONITORAR"
    else:
        return "RETREINAR"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Drift do Fraud Detector

# COMMAND ----------

# Carrega a feature table completa — representa a distribuição atual
df_fraud = spark.table("retail_lakehouse.ml_features.fraud_features").toPandas()

# Simula a distribuição de treino como os primeiros 80% dos dados
# Em produção, isso seria carregado do artefato MLflow (X_train salvo no run)
n_treino = int(len(df_fraud) * 0.8)
df_treino = df_fraud.iloc[:n_treino]
df_atual  = df_fraud.iloc[n_treino:]

FEATURES_FRAUD = [
    "valor_total",
    "ticket_medio_cliente",
    "desvio_do_ticket",
    "parcelas",
    "hora_dia",
]

print("DRIFT — FRAUD DETECTOR")
print("=" * 55)

resultados_drift = []
CHECK_DATE = date.today().isoformat()

for feature in FEATURES_FRAUD:
    base  = df_treino[feature].dropna().values
    atual = df_atual[feature].dropna().values

    if len(base) < 10 or len(atual) < 10:
        continue

    psi    = calcular_psi(base, atual)
    status = classificar_drift(psi)

    media_treino  = round(float(base.mean()),  2)
    media_atual   = round(float(atual.mean()), 2)
    desvio_treino = round(float(base.std()),   2)
    desvio_atual  = round(float(atual.std()),  2)

    icone = "🟢" if status == "ESTAVEL" else "🟡" if status == "MONITORAR" else "🔴"
    print(f"  {icone} {feature:<25} PSI={psi:.4f} → {status}")
    print(f"     Treino: média={media_treino:>8.2f} | desvio={desvio_treino:.2f}")
    print(f"     Atual : média={media_atual:>8.2f} | desvio={desvio_atual:.2f}")

    resultados_drift.append({
        "check_date":    CHECK_DATE,
        "modelo":        "fraud_detector",
        "feature":       feature,
        "psi":           psi,
        "drift_status":  status,
        "media_treino":  media_treino,
        "media_atual":   media_atual,
        "desvio_treino": desvio_treino,
        "desvio_atual":  desvio_atual,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Drift do Demand Forecast

# COMMAND ----------

df_demand = spark.table("retail_lakehouse.ml_features.demand_features").toPandas()

n_treino  = int(len(df_demand) * 0.8)
df_treino = df_demand.iloc[:n_treino]
df_atual  = df_demand.iloc[n_treino:]

FEATURES_DEMAND = [
    "media_qtd_7d",
    "media_qtd_30d",
    "estoque_atual",
]

print("\nDRIFT — DEMAND FORECAST")
print("=" * 55)

for feature in FEATURES_DEMAND:
    base  = df_treino[feature].dropna().values
    atual = df_atual[feature].dropna().values

    if len(base) < 10 or len(atual) < 10:
        continue

    psi    = calcular_psi(base, atual)
    status = classificar_drift(psi)

    media_treino  = round(float(base.mean()),  2)
    media_atual   = round(float(atual.mean()), 2)
    desvio_treino = round(float(base.std()),   2)
    desvio_atual  = round(float(atual.std()),  2)

    icone = "🟢" if status == "ESTAVEL" else "🟡" if status == "MONITORAR" else "🔴"
    print(f"  {icone} {feature:<25} PSI={psi:.4f} → {status}")

    resultados_drift.append({
        "check_date":    CHECK_DATE,
        "modelo":        "demand_forecast",
        "feature":       feature,
        "psi":           psi,
        "drift_status":  status,
        "media_treino":  media_treino,
        "media_atual":   media_atual,
        "desvio_treino": desvio_treino,
        "desvio_atual":  desvio_atual,
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Persistir e exibir resultados

# COMMAND ----------

df_drift = spark.createDataFrame(resultados_drift) \
    .withColumn("check_date", F.to_date(F.col("check_date")))

df_drift.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retail_lakehouse.observability.model_drift")

print(f"\n✔ {len(resultados_drift)} verificações registradas em observability.model_drift")

# COMMAND ----------

display(spark.sql("""
    SELECT
        modelo,
        feature,
        psi,
        CASE drift_status
            WHEN 'ESTAVEL'   THEN '🟢 ESTÁVEL'
            WHEN 'MONITORAR' THEN '🟡 MONITORAR'
            ELSE                  '🔴 RETREINAR'
        END AS drift_status,
        media_treino,
        media_atual,
        ROUND(ABS(media_atual - media_treino) / NULLIF(media_treino, 0) * 100, 1) AS variacao_pct,
        check_date
    FROM retail_lakehouse.observability.model_drift
    WHERE check_date = CURRENT_DATE
    ORDER BY psi DESC
"""))
