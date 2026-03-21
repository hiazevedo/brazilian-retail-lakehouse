# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - DLT Silver: Bronze → Parsed + Validated
# MAGIC
# MAGIC Pipeline Delta Live Tables que lê as tabelas Bronze, faz parse do JSON,
# MAGIC aplica tipagem correta, valida qualidade e enriquece com colunas derivadas.
# MAGIC
# MAGIC **Importante:** Este notebook deve ser adicionado ao mesmo DLT Pipeline
# MAGIC do notebook 01_dlt_bronze — o DLT resolve automaticamente a dependência
# MAGIC entre Bronze e Silver.
# MAGIC
# MAGIC ## Tabelas criadas:
# MAGIC - `retail_lakehouse.silver.vendas`
# MAGIC - `retail_lakehouse.silver.estoque`
# MAGIC - `retail_lakehouse.silver.clientes`
# MAGIC - `retail_lakehouse.silver.pagamentos`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, TimestampType
)

# COMMAND ----------

# Schema explícito da tabela de vendas.
# Definir o schema manualmente (em vez de inferir) é uma boa prática:
# - Mais rápido: não precisa fazer scan dos dados para inferir tipos
# - Mais seguro: campos inesperados são ignorados, não causam falha
# - Mais claro: documenta o contrato esperado dos dados

SCHEMA_SALES = StructType([
    StructField("venda_id",       StringType(),    nullable=False),
    StructField("timestamp",      StringType(),    nullable=True),   # chega como string ISO
    StructField("loja_id",        StringType(),    nullable=False),
    StructField("produto_id",     IntegerType(),   nullable=True),   # número sequencial 0-499
    StructField("cliente_id",     StringType(),    nullable=True),
    StructField("quantidade",     DoubleType(),    nullable=True),   # double por causa do decimals:0
    StructField("preco_unitario", DoubleType(),    nullable=True),
    StructField("desconto_pct",   DoubleType(),    nullable=True),
    StructField("canal",          StringType(),    nullable=True),
    StructField("uf",             StringType(),    nullable=True),
    StructField("schema_version", StringType(),    nullable=True),
])

@dlt.table(
    name="retail_lakehouse.silver.vendas",
    comment="Vendas parseadas, tipadas e validadas — granularidade por transação",
    table_properties={"quality": "silver"},
    partition_cols=["ano_mes"]
)
# expect_or_drop: registros que violam essas regras são DESCARTADOS
# A violação fica registrada nas métricas do DLT para monitoramento
@dlt.expect_or_drop("venda_id_nao_nulo",   "venda_id IS NOT NULL")
@dlt.expect_or_drop("loja_valida",         "loja_id IS NOT NULL AND loja_id != ''")
@dlt.expect_or_drop("preco_positivo",      "preco_unitario > 0")
@dlt.expect_or_drop("quantidade_positiva", "quantidade > 0")
@dlt.expect_or_drop("desconto_valido",     "desconto_pct >= 0 AND desconto_pct <= 1")
@dlt.expect("canal_valido",               "canal IN ('fisico', 'online')")  # só monitora, não descarta
def vendas():
    """Read raw sales events from Bronze, parse JSON, validate and enrich with derived columns.

    Applies expect_or_drop rules — records that violate them are silently discarded
    and counted in the DLT pipeline metrics for monitoring.

    Returns:
        DLT streaming DataFrame with cleaned and enriched sales records.
    """
    return (
        # dlt.read_stream lê da tabela Bronze em modo streaming
        dlt.read_stream("raw_vendas")
        .select(
            # from_json: extrai os campos do JSON usando o schema definido acima
            F.from_json(F.col("payload"), SCHEMA_SALES).alias("data"),
            "ingested_at",
            "kafka_timestamp",
        )
        # Expande o struct "data" em colunas individuais
        .select("data.*", "ingested_at", "kafka_timestamp")

        # Converte timestamp de string ISO para tipo TIMESTAMP do Spark
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

        # Formata produto_id como "PROD-0007" (lpad preenche com zeros à esquerda)
        .withColumn("produto_id",
            F.concat(
                F.lit("PROD-"),
                F.lpad(F.col("produto_id").cast("string"), 4, "0")
            )
        )

        # Calcula valor_total a partir dos campos brutos
        .withColumn("valor_total",
            F.round(
                F.col("preco_unitario") * F.col("quantidade") * (1 - F.col("desconto_pct")),
                2
            )
        )

        # Colunas derivadas de tempo — úteis para análises e agregações Gold
        .withColumn("ano_mes",    F.date_format(F.col("timestamp"), "yyyy-MM"))
        .withColumn("hora_dia",   F.hour(F.col("timestamp")))
        .withColumn("dia_semana", F.dayofweek(F.col("timestamp")))  # 1=Dom, 7=Sab

        # Garante que quantidade seja inteiro na Silver
        .withColumn("quantidade", F.col("quantidade").cast("integer"))
    )

# COMMAND ----------

SCHEMA_INVENTORY = StructType([
    StructField("evento_id",      StringType(),  nullable=False),
    StructField("timestamp",      StringType(),  nullable=True),
    StructField("loja_id",        StringType(),  nullable=False),
    StructField("produto_id",     IntegerType(), nullable=True),
    StructField("tipo_movimento", StringType(),  nullable=True),
    StructField("quantidade",     DoubleType(),  nullable=True),
    StructField("estoque_atual",  DoubleType(),  nullable=True),
    StructField("schema_version", StringType(),  nullable=True),
])

@dlt.table(
    name="retail_lakehouse.silver.estoque",
    comment="Movimentações de estoque parseadas e validadas",
    table_properties={"quality": "silver"},
    partition_cols=["ano_mes"]
)
@dlt.expect_or_drop("evento_id_nao_nulo",  "evento_id IS NOT NULL")
@dlt.expect_or_drop("loja_valida",         "loja_id IS NOT NULL AND loja_id != ''")
@dlt.expect_or_drop("quantidade_positiva", "quantidade > 0")
@dlt.expect_or_drop("estoque_nao_negativo","estoque_atual >= 0")
@dlt.expect("movimento_valido",            "tipo_movimento IN ('saida', 'entrada', 'ajuste', 'transferencia')")
def estoque():
    """Read raw inventory movement events from Bronze, parse JSON and validate quality rules.

    Returns:
        DLT streaming DataFrame with cleaned inventory movement records.
    """
    return (
        dlt.read_stream("raw_estoque")
        .select(
            F.from_json(F.col("payload"), SCHEMA_INVENTORY).alias("data"),
            "ingested_at",
        )
        .select("data.*", "ingested_at")
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("produto_id",
            F.concat(
                F.lit("PROD-"),
                F.lpad(F.col("produto_id").cast("string"), 4, "0")
            )
        )
        .withColumn("quantidade",    F.col("quantidade").cast("integer"))
        .withColumn("estoque_atual", F.col("estoque_atual").cast("integer"))
        .withColumn("ano_mes",       F.date_format(F.col("timestamp"), "yyyy-MM"))
        .withColumn("hora_dia",      F.hour(F.col("timestamp")))
    )

# COMMAND ----------

SCHEMA_CUSTOMERS = StructType([
    StructField("cliente_id",      StringType(), nullable=False),
    StructField("timestamp",       StringType(), nullable=True),
    StructField("tipo_evento",     StringType(), nullable=True),
    StructField("cidade",          StringType(), nullable=True),
    StructField("uf",              StringType(), nullable=True),
    StructField("faixa_renda",     StringType(), nullable=True),
    StructField("canal_preferido", StringType(), nullable=True),
    StructField("schema_version",  StringType(), nullable=True),
])

@dlt.table(
    name="retail_lakehouse.silver.clientes",
    comment="Cadastro e atualizações de clientes parseados e validados",
    table_properties={"quality": "silver"},
    partition_cols=["ano_mes"]
)
@dlt.expect_or_drop("cliente_id_nao_nulo", "cliente_id IS NOT NULL")
@dlt.expect_or_drop("tipo_evento_valido",  "tipo_evento IN ('cadastro', 'atualizacao', 'inativacao')")
@dlt.expect("uf_valida",                   "uf IN ('SP','RJ','MG','RS','PR','BA','CE','GO','SC','PE','AM','PA','MT','MS','ES','RN','PB','AL','PI','MA','RO','AC','AP','RR','TO','SE','DF')")
def clientes():
    """Read raw customer events from Bronze, parse JSON and validate quality rules.

    Returns:
        DLT streaming DataFrame with cleaned customer profile records.
    """
    return (
        dlt.read_stream("raw_clientes")
        .select(
            F.from_json(F.col("payload"), SCHEMA_CUSTOMERS).alias("data"),
            "ingested_at",
        )
        .select("data.*", "ingested_at")
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("ano_mes",   F.date_format(F.col("timestamp"), "yyyy-MM"))
    )

# COMMAND ----------

SCHEMA_PAYMENTS = StructType([
    StructField("pagamento_id", StringType(),  nullable=False),
    StructField("venda_id",     StringType(),  nullable=False),
    StructField("timestamp",    StringType(),  nullable=True),
    StructField("metodo",       StringType(),  nullable=True),
    StructField("status",       StringType(),  nullable=True),
    StructField("parcelas",     IntegerType(), nullable=True),
    StructField("schema_version", StringType(), nullable=True),
])

@dlt.table(
    name="retail_lakehouse.silver.pagamentos",
    comment="Eventos de pagamento parseados e validados",
    table_properties={"quality": "silver"},
    partition_cols=["ano_mes"]
)
@dlt.expect_or_drop("pagamento_id_nao_nulo", "pagamento_id IS NOT NULL")
@dlt.expect_or_drop("venda_id_nao_nulo",     "venda_id IS NOT NULL")
@dlt.expect_or_drop("status_valido",         "status IN ('aprovado', 'recusado', 'estorno')")
@dlt.expect("metodo_valido",                 "metodo IN ('pix', 'debito', 'credito', 'dinheiro')")
def pagamentos():
    """Read raw payment events from Bronze, parse JSON and validate quality rules.

    Returns:
        DLT streaming DataFrame with cleaned payment records.
    """
    return (
        dlt.read_stream("raw_pagamentos")
        .select(
            F.from_json(F.col("payload"), SCHEMA_PAYMENTS).alias("data"),
            "ingested_at",
        )
        .select("data.*", "ingested_at")
        .withColumn("timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("ano_mes",   F.date_format(F.col("timestamp"), "yyyy-MM"))
    )
