# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Data Health Monitor
# MAGIC
# MAGIC Monitora a saúde das tabelas Silver em três dimensões:
# MAGIC
# MAGIC - **Volume** — o número de registros hoje está dentro do esperado?
# MAGIC - **Freshness** — o dado mais recente chegou dentro do SLA?
# MAGIC - **Qualidade** — a proporção de nulos aumentou?
# MAGIC
# MAGIC Persiste os resultados em `observability.data_health` para análise histórica.

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, date

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.observability.data_health (
        check_date       DATE      COMMENT 'Data da verificação',
        check_ts         TIMESTAMP COMMENT 'Timestamp exato da verificação',
        tabela           STRING    COMMENT 'Nome completo da tabela verificada',
        dominio          STRING    COMMENT 'Domínio de dados (vendas, estoque, etc.)',
        volume_hoje      BIGINT    COMMENT 'Número de registros nas últimas 24h',
        volume_media_7d  DOUBLE    COMMENT 'Média diária dos últimos 7 dias',
        volume_status    STRING    COMMENT 'OK / BAIXO / CRITICO',
        freshness_min    DOUBLE    COMMENT 'Minutos desde o último evento',
        freshness_sla    INT       COMMENT 'SLA de freshness em minutos (do contrato)',
        freshness_status STRING    COMMENT 'OK / ATRASADO',
        nulos_pct        DOUBLE    COMMENT 'Percentual de nulos nas colunas-chave',
        nulos_status     STRING    COMMENT 'OK / ALTO',
        health_score     INT       COMMENT 'Score geral: 100=saudável, decresce por problema'
    )
    COMMENT 'Histórico de saúde das tabelas Silver'
""")

print("✔ Tabela observability.data_health criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuração das verificações

# COMMAND ----------

# Definição de cada domínio: tabela Silver, colunas-chave e SLA de freshness
DOMAINS = {
    "vendas": {
        "tabela":         "retail_lakehouse.silver.vendas",
        "col_id":         "venda_id",
        "col_ts":         "timestamp",
        "freshness_sla":  30,
        "min_volume_dia": 5000,
    },
    "estoque": {
        "tabela":         "retail_lakehouse.silver.estoque",
        "col_id":         "evento_id",
        "col_ts":         "timestamp",
        "freshness_sla":  60,
        "min_volume_dia": 500,
    },
    "clientes": {
        "tabela":         "retail_lakehouse.silver.clientes",
        "col_id":         "cliente_id",
        "col_ts":         "timestamp",
        "freshness_sla":  120,
        "min_volume_dia": 100,
    },
    "pagamentos": {
        "tabela":         "retail_lakehouse.silver.pagamentos",
        "col_id":         "pagamento_id",
        "col_ts":         "timestamp",
        "freshness_sla":  30,
        "min_volume_dia": 1000,
    },
}

CHECK_TS   = datetime.utcnow()
CHECK_DATE = date.today().isoformat()

# COMMAND ----------

results = []

for domain, cfg in DOMAINS.items():
    tabela        = cfg["tabela"]
    col_id        = cfg["col_id"]
    col_ts        = cfg["col_ts"]
    freshness_sla = cfg["freshness_sla"]
    min_volume    = cfg["min_volume_dia"]

    print(f"\n{'='*50}")
    print(f"Verificando: {domain}")
    print(f"{'='*50}")

    # --- Volume ---
    # Conta registros das últimas 24h e compara com média dos últimos 7 dias
    stats = spark.sql(f"""
        SELECT
            COUNT_IF(CAST({col_ts} AS DATE) >= CURRENT_DATE - INTERVAL 1 DAY)  AS volume_hoje,
            COUNT(*) / 7.0 AS volume_media_7d
        FROM {tabela}
        WHERE CAST({col_ts} AS DATE) >= CURRENT_DATE - INTERVAL 7 DAY
    """).collect()[0]

    volume_hoje    = stats["volume_hoje"] or 0
    volume_media   = round(float(stats["volume_media_7d"] or 0), 1)

    if volume_hoje < min_volume * 0.5:
        volume_status = "CRITICO"
    elif volume_hoje < min_volume:
        volume_status = "BAIXO"
    else:
        volume_status = "OK"

    print(f"  Volume hoje : {volume_hoje:,} | Média 7d: {volume_media:,.0f} | Status: {volume_status}")

    # --- Freshness ---
    freshness = spark.sql(f"""
        SELECT ROUND(
            (UNIX_TIMESTAMP(CURRENT_TIMESTAMP) - UNIX_TIMESTAMP(MAX({col_ts}))) / 60,
        1) AS minutos_desde_ultimo
        FROM {tabela}
    """).collect()[0]

    freshness_min = float(freshness["minutos_desde_ultimo"] or 9999)
    freshness_status = "ATRASADO" if freshness_min > freshness_sla else "OK"

    print(f"  Freshness   : {freshness_min:.0f}min | SLA: {freshness_sla}min | Status: {freshness_status}")

    # --- Qualidade (nulos nas colunas-chave) ---
    total = spark.sql(f"SELECT COUNT(*) AS n FROM {tabela}").collect()[0]["n"]
    nulos = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {tabela}
        WHERE {col_id} IS NULL OR {col_ts} IS NULL
    """).collect()[0]["n"]

    nulos_pct    = round((nulos / total * 100) if total > 0 else 0, 2)
    nulos_status = "ALTO" if nulos_pct > 1.0 else "OK"

    print(f"  Nulos chave : {nulos_pct:.2f}% | Status: {nulos_status}")

    # --- Health Score ---
    # Começa em 100 e desconta por problema encontrado
    score = 100
    if volume_status == "CRITICO":    score -= 40
    elif volume_status == "BAIXO":    score -= 20
    if freshness_status == "ATRASADO": score -= 30
    if nulos_status == "ALTO":         score -= 30

    print(f"  Health Score: {score}/100")

    results.append({
        "check_date":       CHECK_DATE,
        "check_ts":         str(CHECK_TS),
        "tabela":           tabela,
        "dominio":          domain,
        "volume_hoje":      volume_hoje,
        "volume_media_7d":  volume_media,
        "volume_status":    volume_status,
        "freshness_min":    freshness_min,
        "freshness_sla":    freshness_sla,
        "freshness_status": freshness_status,
        "nulos_pct":        nulos_pct,
        "nulos_status":     nulos_status,
        "health_score":     score,
    })

# COMMAND ----------

df_health = spark.createDataFrame(results).withColumns({
    "check_ts":    F.to_timestamp(F.col("check_ts")),
    "check_date":  F.to_date(F.col("check_date")),
    "freshness_sla": F.col("freshness_sla").cast("int"),
    "health_score":  F.col("health_score").cast("int"),
    "volume_hoje":   F.col("volume_hoje").cast("bigint"),
})

df_health.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retail_lakehouse.observability.data_health")

print(f"\n✔ {len(results)} verificações registradas em observability.data_health")

# COMMAND ----------

display(spark.sql("""
    SELECT
        dominio,
        health_score,
        CASE
            WHEN health_score >= 80 THEN '🟢 SAUDÁVEL'
            WHEN health_score >= 50 THEN '🟡 ATENÇÃO'
            ELSE                        '🔴 CRÍTICO'
        END AS status_geral,
        volume_hoje,
        CONCAT(freshness_min, 'min / SLA ', freshness_sla, 'min') AS freshness,
        freshness_status,
        CONCAT(nulos_pct, '%') AS nulos_chave,
        check_ts
    FROM retail_lakehouse.observability.data_health
    WHERE check_date = CURRENT_DATE
    ORDER BY health_score ASC
"""))
