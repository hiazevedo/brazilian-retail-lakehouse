# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - DLT Bronze: Kafka → Delta
# MAGIC
# MAGIC Pipeline Delta Live Tables que consome os 4 tópicos do Confluent Cloud
# MAGIC e persiste os eventos brutos em tabelas Delta na camada Bronze.
# MAGIC
# MAGIC **Importante:** Este notebook não é executado diretamente.
# MAGIC Deve ser referenciado em um DLT Pipeline no Databricks.
# MAGIC
# MAGIC ## Tabelas criadas:
# MAGIC - `retail_lakehouse.bronze.raw_vendas`
# MAGIC - `retail_lakehouse.bronze.raw_estoque`
# MAGIC - `retail_lakehouse.bronze.raw_clientes`
# MAGIC - `retail_lakehouse.bronze.raw_pagamentos`

# COMMAND ----------

import dlt
from pyspark.sql import functions as F

# COMMAND ----------

# Configuração da conexão com o Confluent Cloud.
# As credenciais são lidas via Databricks Secrets — nunca ficam no código.
#
# O padrão SASL_SSL + PLAIN é o protocolo de autenticação padrão do Confluent.
# O sasl.jaas.config monta a string de autenticação com key e secret.

def get_kafka_options(topic: str) -> dict:
    """
    Retorna as opções de conexão Kafka para um tópico específico.
    Centraliza a configuração para evitar repetição nos 4 generators.
    """
    api_key    = dbutils.secrets.get("retail-lakehouse", "confluent_api_key")
    api_secret = dbutils.secrets.get("retail-lakehouse", "confluent_api_secret")
    bootstrap  = dbutils.secrets.get("retail-lakehouse", "confluent_bootstrap_servers")

    jaas_config = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
        f'required username="{api_key}" password="{api_secret}";'
    )

    return {
        "kafka.bootstrap.servers":  bootstrap,
        "kafka.security.protocol":  "SASL_SSL",
        "kafka.sasl.mechanism":     "PLAIN",
        "kafka.sasl.jaas.config":   jaas_config,
        "subscribe":                topic,
        # earliest: garante que vamos ler todos os 500K eventos do bootstrap
        "startingOffsets":          "earliest",
        # Configurações de performance para Serverless
        "kafka.session.timeout.ms": "45000",
        "kafka.request.timeout.ms": "60000",
        "failOnDataLoss":           "false",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_vendas

# COMMAND ----------

@dlt.table(
    name="raw_vendas",
    comment="Eventos brutos de vendas consumidos do Kafka - sem transformação",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
# expect: registra violação mas não descarta o registro (monitoramento)
# Na Bronze queremos guardar TUDO, inclusive dados inválidos
@dlt.expect("payload_nao_nulo", "payload IS NOT NULL")
@dlt.expect("timestamp_kafka_nao_nulo", "kafka_timestamp IS NOT NULL")
def raw_vendas():
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options("retail.vendas"))
        .load()
        .select(
            # O valor do evento chega como bytes — convertemos para string JSON
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            # Metadados de ingestão — úteis para debugging e auditoria
            F.current_timestamp().alias("ingested_at"),
            F.to_date(F.col("timestamp")).alias("ingestion_date"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_estoque

# COMMAND ----------

@dlt.table(
    name="raw_estoque",
    comment="Eventos brutos de movimentações de estoque consumidos do Kafka",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
@dlt.expect("payload_nao_nulo", "payload IS NOT NULL")
@dlt.expect("timestamp_kafka_nao_nulo", "kafka_timestamp IS NOT NULL")
def raw_estoque():
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options("retail.estoque"))
        .load()
        .select(
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.current_timestamp().alias("ingested_at"),
            F.to_date(F.col("timestamp")).alias("ingestion_date"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_clientes

# COMMAND ----------

@dlt.table(
    name="raw_clientes",
    comment="Eventos brutos de cadastro e atualização de clientes consumidos do Kafka",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
@dlt.expect("payload_nao_nulo", "payload IS NOT NULL")
@dlt.expect("timestamp_kafka_nao_nulo", "kafka_timestamp IS NOT NULL")
def raw_clientes():
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options("retail.clientes"))
        .load()
        .select(
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.current_timestamp().alias("ingested_at"),
            F.to_date(F.col("timestamp")).alias("ingestion_date"),
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## raw_pagamentos

# COMMAND ----------

@dlt.table(
    name="raw_pagamentos",
    comment="Eventos brutos de pagamentos consumidos do Kafka",
    table_properties={"quality": "bronze"},
    partition_cols=["ingestion_date"]
)
@dlt.expect("payload_nao_nulo", "payload IS NOT NULL")
@dlt.expect("timestamp_kafka_nao_nulo", "kafka_timestamp IS NOT NULL")
def raw_pagamentos():
    return (
        spark.readStream
        .format("kafka")
        .options(**get_kafka_options("retail.pagamentos"))
        .load()
        .select(
            F.col("value").cast("string").alias("payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("offset").alias("kafka_offset"),
            F.col("partition").alias("kafka_partition"),
            F.current_timestamp().alias("ingested_at"),
            F.to_date(F.col("timestamp")).alias("ingestion_date"),
        )
    )
