# Databricks notebook source
# MAGIC %md
# MAGIC # 06 - Contract Monitor
# MAGIC
# MAGIC Dashboard analítico de conformidade de data contracts.
# MAGIC Responde às principais perguntas de saúde dos dados:
# MAGIC
# MAGIC - Contratos em conformidade hoje
# MAGIC - Freshness por domínio (dados estão chegando dentro do SLA?)
# MAGIC - Tendência de violações nos últimos 30 dias
# MAGIC - Regras mais violadas
# MAGIC - Contratos em risco de SLA

# COMMAND ----------

from pyspark.sql import functions as F
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Status de Conformidade por Contrato

# COMMAND ----------

# Para cada contrato ativo, verifica se houve violações CRITICAL hoje.
# Um contrato está "em conformidade" se não teve CRITICAL nas últimas 24h.

display(spark.sql("""
    WITH violacoes_hoje AS (
        SELECT
            contract_id,
            severity,
            SUM(violation_count)  AS total_violacoes,
            COUNT(*)              AS total_regras_violadas,
            MAX(detected_at)      AS ultima_violacao
        FROM retail_lakehouse.contracts.violations
        WHERE run_date >= CURRENT_DATE - INTERVAL 1 DAY
        GROUP BY contract_id, severity
    )
    SELECT
        r.contract_id,
        r.version,
        r.owner,
        COALESCE(c.total_violacoes, 0)       AS violacoes_critical,
        COALESCE(w.total_violacoes, 0)       AS violacoes_warning,
        CASE
            WHEN c.total_violacoes > 0 THEN '🔴 CRÍTICO'
            WHEN w.total_violacoes > 0 THEN '🟡 ATENÇÃO'
            ELSE                            '🟢 CONFORME'
        END AS status_conformidade,
        COALESCE(c.ultima_violacao, w.ultima_violacao) AS ultima_violacao
    FROM retail_lakehouse.contracts.registry r
    LEFT JOIN violacoes_hoje c
        ON r.contract_id = c.contract_id AND c.severity = 'CRITICAL'
    LEFT JOIN violacoes_hoje w
        ON r.contract_id = w.contract_id AND w.severity = 'WARNING'
    WHERE r.status = 'active'
    ORDER BY r.contract_id
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Freshness Check — Dados chegando dentro do SLA?

# COMMAND ----------

# Verifica se o dado mais recente de cada tabela Silver
# chegou dentro do prazo definido no contrato.
#
# freshness_minutes é o tempo máximo aceitável desde o último evento.
# Se o último evento foi há mais tempo que o SLA, o contrato está em risco.

SILVER_TABLES = {
    "retail.vendas":     "retail_lakehouse.silver.vendas",
    "retail.estoque":    "retail_lakehouse.silver.estoque",
    "retail.clientes":   "retail_lakehouse.silver.clientes",
    "retail.pagamentos": "retail_lakehouse.silver.pagamentos",
}

freshness_rows = []

active_contracts = spark.sql("""
    SELECT contract_id, version, sla_json
    FROM retail_lakehouse.contracts.registry
    WHERE status = 'active'
""").collect()

for contract in active_contracts:
    tabela = SILVER_TABLES.get(contract.contract_id)
    if not tabela:
        continue

    sla = json.loads(contract.sla_json)
    freshness_sla = sla.get("freshness_minutes", 60)
    min_volume    = sla.get("min_daily_volume", 0)

    # Busca o timestamp do evento mais recente na tabela Silver
    resultado = spark.sql(f"""
        SELECT
            MAX(timestamp)                                     AS ultimo_evento,
            ROUND(
                (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(MAX(timestamp))) / 60,
                1
            )                                                  AS minutos_desde_ultimo,
            COUNT(*)                                           AS volume_hoje
        FROM {tabela}
        WHERE CAST(timestamp AS DATE) >= CURRENT_DATE - INTERVAL 1 DAY
    """).collect()[0]

    minutos_atrasado = resultado["minutos_desde_ultimo"] or 9999
    volume_hoje      = resultado["volume_hoje"] or 0
    ultimo_evento    = resultado["ultimo_evento"]

    # Avalia status de freshness
    if minutos_atrasado > freshness_sla:
        status_freshness = f"🔴 ATRASADO ({minutos_atrasado:.0f}min > SLA {freshness_sla}min)"
    else:
        status_freshness = f"🟢 OK ({minutos_atrasado:.0f}min < SLA {freshness_sla}min)"

    # Avalia volume mínimo diário
    if volume_hoje < min_volume:
        status_volume = f"🔴 BAIXO ({volume_hoje:,} < mínimo {min_volume:,})"
    else:
        status_volume = f"🟢 OK ({volume_hoje:,} eventos)"

    freshness_rows.append({
        "contract_id":      contract.contract_id,
        "ultimo_evento":    str(ultimo_evento),
        "minutos_atraso":   round(float(minutos_atrasado), 1),
        "freshness_sla":    freshness_sla,
        "status_freshness": status_freshness,
        "volume_hoje":      volume_hoje,
        "min_volume_sla":   min_volume,
        "status_volume":    status_volume,
    })

df_freshness = spark.createDataFrame(freshness_rows)
display(df_freshness)

# COMMAND ----------

# Agrega violações por dia e severidade para visualizar tendências.
# Uma curva crescente de violations é sinal de degradação na qualidade dos dados.

display(spark.sql("""
    SELECT
        run_date,
        contract_id,
        severity,
        COUNT(*)             AS regras_violadas,
        SUM(violation_count) AS registros_afetados
    FROM retail_lakehouse.contracts.violations
    WHERE run_date >= CURRENT_DATE - INTERVAL 30 DAY
    GROUP BY run_date, contract_id, severity
    ORDER BY run_date DESC, contract_id, severity
"""))

# COMMAND ----------

# Identifica quais regras de qualidade são violadas com mais frequência.
# Regras que aparecem repetidamente são candidatas a revisão no contrato
# ou indicam problemas estruturais no produtor de dados.

display(spark.sql("""
    SELECT
        contract_id,
        rule_name,
        severity,
        COUNT(DISTINCT run_date)  AS dias_com_violacao,
        SUM(violation_count)      AS total_registros_afetados,
        MAX(detected_at)          AS ultima_ocorrencia
    FROM retail_lakehouse.contracts.violations
    GROUP BY contract_id, rule_name, severity
    ORDER BY total_registros_afetados DESC
    LIMIT 20
"""))

# COMMAND ----------

# Visão consolidada do estado atual da plataforma
total_contratos = spark.sql("""
    SELECT COUNT(*) FROM retail_lakehouse.contracts.registry WHERE status = 'active'
""").collect()[0][0]

total_violacoes_hoje = spark.sql("""
    SELECT COALESCE(SUM(violation_count), 0)
    FROM retail_lakehouse.contracts.violations
    WHERE run_date = CURRENT_DATE
""").collect()[0][0]

criticos_hoje = spark.sql("""
    SELECT COALESCE(SUM(violation_count), 0)
    FROM retail_lakehouse.contracts.violations
    WHERE run_date = CURRENT_DATE AND severity = 'CRITICAL'
""").collect()[0][0]

print("=" * 55)
print("RESUMO EXECUTIVO — DATA CONTRACTS")
print("=" * 55)
print(f"Contratos ativos        : {total_contratos}")
print(f"Violações hoje          : {total_violacoes_hoje:,}")
print(f"Violações CRITICAL hoje : {criticos_hoje:,}")
print()

if criticos_hoje > 0:
    print("🔴 ATENÇÃO: Há violações CRITICAL hoje — verifique os dados Silver")
elif total_violacoes_hoje > 0:
    print("🟡 ATENÇÃO: Há violações WARNING hoje — monitorar tendência")
else:
    print("🟢 Todos os contratos estão em conformidade hoje")
