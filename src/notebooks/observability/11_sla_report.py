# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - SLA Report
# MAGIC
# MAGIC Relatório de cumprimento de SLAs por domínio.
# MAGIC Cruza o que foi prometido nos contratos YAML com o que foi entregue nas tabelas Silver.
# MAGIC
# MAGIC **Execute após o 10_data_health.**

# COMMAND ----------

from pyspark.sql import functions as F
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SLA Prometido vs Entregue — Visão Atual

# COMMAND ----------

# Lê os contratos ativos e compara com o estado atual das tabelas Silver
display(spark.sql("""
    WITH contratos AS (
        SELECT
            contract_id,
            version,
            owner,
            CAST(GET_JSON_OBJECT(sla_json, '$.freshness_minutes') AS INT)  AS sla_freshness_min,
            CAST(GET_JSON_OBJECT(sla_json, '$.min_daily_volume')  AS INT)  AS sla_min_volume
        FROM retail_lakehouse.contracts.registry
        WHERE status = 'active'
    ),

    entregue AS (
        SELECT
            dominio                                          AS contract_id,
            volume_hoje,
            freshness_min,
            freshness_sla,
            freshness_status,
            health_score,
            check_ts
        FROM retail_lakehouse.observability.data_health
        WHERE check_date = CURRENT_DATE
    )

    SELECT
        c.contract_id,
        c.owner,
        -- Freshness
        c.sla_freshness_min                                  AS freshness_prometido_min,
        ROUND(e.freshness_min, 0)                            AS freshness_entregue_min,
        CASE
            WHEN e.freshness_min <= c.sla_freshness_min THEN '✅ DENTRO DO SLA'
            WHEN e.freshness_min <= c.sla_freshness_min * 2 THEN '⚠️ SLA LEVEMENTE VIOLADO'
            ELSE '❌ SLA GRAVEMENTE VIOLADO'
        END AS sla_freshness_status,
        -- Volume
        c.sla_min_volume                                     AS volume_prometido,
        e.volume_hoje                                        AS volume_entregue,
        CASE
            WHEN e.volume_hoje >= c.sla_min_volume THEN '✅ DENTRO DO SLA'
            WHEN e.volume_hoje >= c.sla_min_volume * 0.8 THEN '⚠️ SLA LEVEMENTE VIOLADO'
            ELSE '❌ SLA GRAVEMENTE VIOLADO'
        END AS sla_volume_status,
        e.health_score,
        e.check_ts
    FROM contratos c
    LEFT JOIN entregue e ON c.contract_id = 'retail.' || e.contract_id
    ORDER BY e.health_score ASC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Histórico de Cumprimento de SLA — Últimos 30 dias

# COMMAND ----------

# Tendência de health score por domínio — identifica degradação progressiva
display(spark.sql("""
    SELECT
        check_date,
        dominio,
        health_score,
        freshness_status,
        volume_status,
        volume_hoje,
        freshness_min
    FROM retail_lakehouse.observability.data_health
    WHERE check_date >= CURRENT_DATE - INTERVAL 30 DAY
    ORDER BY check_date DESC, dominio
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Taxa de Cumprimento de SLA por Domínio

# COMMAND ----------

# Percentual de dias em que cada domínio cumpriu o SLA de freshness
display(spark.sql("""
    SELECT
        dominio,
        COUNT(*)                                                          AS dias_verificados,
        COUNT_IF(freshness_status = 'OK')                                 AS dias_dentro_sla,
        COUNT_IF(freshness_status = 'ATRASADO')                           AS dias_violado,
        ROUND(COUNT_IF(freshness_status = 'OK') * 100.0 / COUNT(*), 1)  AS pct_cumprimento,
        ROUND(AVG(health_score), 1)                                       AS health_score_medio,
        ROUND(AVG(freshness_min), 0)                                      AS freshness_medio_min
    FROM retail_lakehouse.observability.data_health
    GROUP BY dominio
    ORDER BY pct_cumprimento ASC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Violações Registradas nos Contratos

# COMMAND ----------

# Cruza com o histórico de violações do contract_validator (notebook 05)
display(spark.sql("""
    SELECT
        v.contract_id,
        v.run_date,
        v.severity,
        COUNT(*)             AS regras_violadas,
        SUM(v.violation_count) AS registros_afetados,
        MAX(v.detected_at)   AS ultima_deteccao
    FROM retail_lakehouse.contracts.violations v
    WHERE v.run_date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY v.contract_id, v.run_date, v.severity
    ORDER BY v.run_date DESC, v.severity
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resumo Executivo de SLA

# COMMAND ----------

stats = spark.sql("""
    SELECT
        COUNT(DISTINCT dominio)                           AS dominios_monitorados,
        ROUND(AVG(health_score), 1)                       AS health_score_medio,
        COUNT_IF(freshness_status = 'ATRASADO')           AS dominios_atrasados,
        COUNT_IF(volume_status IN ('BAIXO', 'CRITICO'))   AS dominios_volume_baixo
    FROM retail_lakehouse.observability.data_health
    WHERE check_date = CURRENT_DATE
""").collect()[0]

print("=" * 55)
print("RELATÓRIO DE SLA — RESUMO EXECUTIVO")
print("=" * 55)
print(f"Domínios monitorados : {stats['dominios_monitorados']}")
print(f"Health score médio   : {stats['health_score_medio']}/100")
print(f"Domínios atrasados   : {stats['dominios_atrasados']}")
print(f"Volume abaixo do SLA : {stats['dominios_volume_baixo']}")
print()

score = stats["health_score_medio"] or 0
if score >= 80:
    print("🟢 Plataforma SAUDÁVEL — todos os SLAs cumpridos")
elif score >= 50:
    print("🟡 Plataforma em ATENÇÃO — alguns SLAs violados")
else:
    print("🔴 Plataforma em RISCO — múltiplos SLAs violados")
