# Databricks notebook source
# MAGIC %md
# MAGIC # 13 - Feature Store
# MAGIC
# MAGIC Documenta e governa as feature tables do projeto no Unity Catalog.
# MAGIC
# MAGIC **O que este notebook faz:**
# MAGIC - Adiciona metadados (tags, owner, cadência) nas feature tables existentes via `ALTER TABLE`
# MAGIC - Cria a tabela `ml_features.feature_catalog` com o dicionário de todas as features
# MAGIC - Exibe relatórios de cobertura: quais modelos consomem quais features
# MAGIC
# MAGIC **Execute após o 07_feature_engineering.**

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import date

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anotar metadados nas feature tables existentes
# MAGIC
# MAGIC O Unity Catalog permite adicionar propriedades customizadas a qualquer tabela via `TBLPROPERTIES`.
# MAGIC Isso não altera os dados — apenas enriquece o metastore com informações de governança.

# COMMAND ----------

# --- demand_features ---
spark.sql("""
    ALTER TABLE retail_lakehouse.ml_features.demand_features
    SET TBLPROPERTIES (
        'feature.owner'            = 'engenharia-de-dados',
        'feature.domain'           = 'demand',
        'feature.refresh_cadence'  = 'daily',
        'feature.consumed_by'      = 'demand_forecast',
        'feature.source_tables'    = 'gold.mrt_estoque_atual, silver.vendas',
        'feature.description'      = 'Features de previsão de demanda: médias móveis de quantidade e posição de estoque por produto e loja'
    )
""")

print("✔ Metadados aplicados em ml_features.demand_features")

# COMMAND ----------

# --- fraud_features ---
spark.sql("""
    ALTER TABLE retail_lakehouse.ml_features.fraud_features
    SET TBLPROPERTIES (
        'feature.owner'            = 'engenharia-de-dados',
        'feature.domain'           = 'fraud',
        'feature.refresh_cadence'  = 'daily',
        'feature.consumed_by'      = 'fraud_detector',
        'feature.source_tables'    = 'gold.mrt_risco_fraude, silver.vendas',
        'feature.description'      = 'Features de detecção de fraude: perfil comportamental por cliente (ticket médio, desvio, parcelas, hora)'
    )
""")

print("✔ Metadados aplicados em ml_features.fraud_features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar tabela feature_catalog
# MAGIC
# MAGIC O `feature_catalog` é o dicionário central de features.
# MAGIC Cada linha representa uma feature individual, com sua origem, tipo semântico,
# MAGIC descrição de negócio e quais modelos a consomem.
# MAGIC
# MAGIC **Tipos semânticos usados:**
# MAGIC - `TEMPORAL`    — agregações baseadas em janela de tempo (médias móveis)
# MAGIC - `BEHAVIORAL`  — comportamento histórico do cliente ou entidade
# MAGIC - `CONTEXTUAL`  — atributos do contexto da transação (hora, canal)
# MAGIC - `INVENTORY`   — posição atual de estoque

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.ml_features.feature_catalog (
        feature_name      STRING  COMMENT 'Nome da feature (igual ao nome da coluna)',
        source_table      STRING  COMMENT 'Tabela Delta onde a feature está armazenada',
        feature_type      STRING  COMMENT 'Tipo semântico: TEMPORAL, BEHAVIORAL, CONTEXTUAL, INVENTORY',
        consumed_by       STRING  COMMENT 'Modelo(s) que consomem esta feature',
        description       STRING  COMMENT 'Descrição de negócio da feature',
        registered_at     DATE    COMMENT 'Data de registro no catálogo'
    )
    COMMENT 'Dicionário central de features — governança da plataforma de ML'
""")

print("✔ Tabela ml_features.feature_catalog criada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registrar as features

# COMMAND ----------

TODAY = date.today().isoformat()

features = [
    # --- demand_forecast ---
    {
        "feature_name":  "media_qtd_7d",
        "source_table":  "retail_lakehouse.ml_features.demand_features",
        "feature_type":  "TEMPORAL",
        "consumed_by":   "demand_forecast",
        "description":   "Média de quantidade vendida nos últimos 7 dias por produto e loja",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "media_qtd_30d",
        "source_table":  "retail_lakehouse.ml_features.demand_features",
        "feature_type":  "TEMPORAL",
        "consumed_by":   "demand_forecast",
        "description":   "Média de quantidade vendida nos últimos 30 dias por produto e loja",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "estoque_atual",
        "source_table":  "retail_lakehouse.ml_features.demand_features",
        "feature_type":  "INVENTORY",
        "consumed_by":   "demand_forecast",
        "description":   "Quantidade atual em estoque por produto e loja",
        "registered_at": TODAY,
    },
    # --- fraud_detector ---
    {
        "feature_name":  "valor_total",
        "source_table":  "retail_lakehouse.ml_features.fraud_features",
        "feature_type":  "BEHAVIORAL",
        "consumed_by":   "fraud_detector",
        "description":   "Valor total da transação (preço unitário × quantidade)",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "ticket_medio_cliente",
        "source_table":  "retail_lakehouse.ml_features.fraud_features",
        "feature_type":  "BEHAVIORAL",
        "consumed_by":   "fraud_detector",
        "description":   "Ticket médio histórico do cliente em todas as suas transações",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "desvio_do_ticket",
        "source_table":  "retail_lakehouse.ml_features.fraud_features",
        "feature_type":  "BEHAVIORAL",
        "consumed_by":   "fraud_detector",
        "description":   "Distância em desvios padrão entre o valor_total e o ticket_medio_cliente",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "parcelas",
        "source_table":  "retail_lakehouse.ml_features.fraud_features",
        "feature_type":  "CONTEXTUAL",
        "consumed_by":   "fraud_detector",
        "description":   "Número de parcelas do pagamento associado à transação",
        "registered_at": TODAY,
    },
    {
        "feature_name":  "hora_dia",
        "source_table":  "retail_lakehouse.ml_features.fraud_features",
        "feature_type":  "CONTEXTUAL",
        "consumed_by":   "fraud_detector",
        "description":   "Hora do dia em que a transação ocorreu (0–23)",
        "registered_at": TODAY,
    },
]

df_catalog = spark.createDataFrame(features) \
    .withColumn("registered_at", F.to_date(F.col("registered_at")))

# Substitui completamente o catálogo a cada execução (idempotente)
df_catalog.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_lakehouse.ml_features.feature_catalog")

print(f"✔ {len(features)} features registradas em ml_features.feature_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Relatório: cobertura por modelo

# COMMAND ----------

display(spark.sql("""
    SELECT
        consumed_by                         AS modelo,
        COUNT(*)                            AS total_features,
        COUNT_IF(feature_type = 'TEMPORAL')    AS temporais,
        COUNT_IF(feature_type = 'BEHAVIORAL')  AS comportamentais,
        COUNT_IF(feature_type = 'CONTEXTUAL')  AS contextuais,
        COUNT_IF(feature_type = 'INVENTORY')   AS inventario
    FROM retail_lakehouse.ml_features.feature_catalog
    GROUP BY consumed_by
    ORDER BY consumed_by
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Relatório: dicionário completo de features

# COMMAND ----------

display(spark.sql("""
    SELECT
        feature_name,
        feature_type,
        consumed_by,
        description,
        source_table,
        registered_at
    FROM retail_lakehouse.ml_features.feature_catalog
    ORDER BY consumed_by, feature_type, feature_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificar metadados aplicados nas tabelas

# COMMAND ----------

# Exibe as propriedades registradas no Unity Catalog para cada feature table
for tabela in [
    "retail_lakehouse.ml_features.demand_features",
    "retail_lakehouse.ml_features.fraud_features",
]:
    props = spark.sql(f"SHOW TBLPROPERTIES {tabela}").collect()
    feature_props = [r for r in props if r["key"].startswith("feature.")]
    print(f"\n{tabela}")
    print("=" * 60)
    for p in feature_props:
        print(f"  {p['key']:<35} {p['value']}")
