# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Setup: Infraestrutura Base
# MAGIC
# MAGIC Este notebook configura toda a infraestrutura do Unity Catalog necessária
# MAGIC para o projeto Brazilian Retail Lakehouse.
# MAGIC
# MAGIC **Execute este notebook uma única vez** antes de rodar qualquer pipeline.
# MAGIC
# MAGIC ## O que será criado:
# MAGIC - Catalog: `retail_lakehouse`
# MAGIC - Schemas: `bronze`, `silver`, `gold`, `ml_features`, `contracts`, `observability`
# MAGIC - Volumes: `bronze/raw_events`, `contracts/schemas`
# MAGIC - Tabela de configuração: `bronze.pipeline_config`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog

# COMMAND ----------

# Cria o catalog principal do projeto.
# No Databricks Free Edition, o catalog padrão já existe.
# Este comando cria um novo catalog dedicado ao projeto.
spark.sql("""
    CREATE CATALOG IF NOT EXISTS retail_lakehouse
    COMMENT 'Catalog principal do Brazilian Retail Lakehouse'
""")

print("✔ Catalog retail_lakehouse criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schemas

# COMMAND ----------

# Cada schema representa uma camada da arquitetura Medallion ou um domínio funcional.
# bronze      → dados brutos do Kafka (sem transformação)
# silver      → dados limpos, tipados e validados
# gold        → modelos dimensionais e agregações analíticas (dbt na Phase 3)
# ml_features → feature store para modelos de ML (Phase 4)
# contracts   → registro de data contracts e violações (Phase 2)
# observability → métricas de qualidade e SLA (Phase 5)

schemas = [
    ("bronze",        "Dados brutos ingeridos do Kafka via DLT"),
    ("silver",        "Dados limpos, tipados e validados via DLT"),
    ("gold",          "Modelos dimensionais e agregações analíticas"),
    ("ml_features",   "Feature store para modelos de ML"),
    ("contracts",     "Registro de data contracts e violações"),
    ("observability", "Métricas de qualidade, SLA e monitoramento"),
]

for schema, comment in schemas:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS retail_lakehouse.{schema}
        COMMENT '{comment}'
    """)
    print(f"✔ Schema retail_lakehouse.{schema} criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Volumes

# COMMAND ----------

# Volumes são pastas gerenciadas pelo Unity Catalog para armazenar arquivos.
# Diferente de tabelas Delta, volumes guardam arquivos brutos (JSON, YAML, CSV, etc.)
#
# raw_events  → arquivos brutos de eventos (usado como fallback de ingestão)
# schemas     → arquivos YAML de data contracts (Phase 2)

volumes = [
    ("bronze",    "raw_events", "Arquivos brutos de eventos Kafka"),
    ("contracts", "schemas",    "Arquivos YAML de data contracts"),
]

for schema, volume, comment in volumes:
    spark.sql(f"""
        CREATE VOLUME IF NOT EXISTS retail_lakehouse.{schema}.{volume}
        COMMENT '{comment}'
    """)
    print(f"✔ Volume /Volumes/retail_lakehouse/{schema}/{volume} criado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabela de Configuração do Pipeline

# COMMAND ----------

# Esta tabela centraliza todas as configurações do pipeline Kafka → Databricks.
# Vantagem: mudar configurações sem alterar código dos notebooks DLT.
#
# Os valores sensíveis (API key, secret) NÃO ficam aqui — ficam nos Secrets.
# Aqui ficam apenas configurações não sensíveis.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.bronze.pipeline_config (
        config_key   STRING  COMMENT 'Nome da configuração',
        config_value STRING  COMMENT 'Valor da configuração',
        description  STRING  COMMENT 'Descrição do parâmetro',
        updated_at   TIMESTAMP COMMENT 'Data da última atualização'
    )
    COMMENT 'Configurações centralizadas dos pipelines DLT'
""")

# Insere as configurações iniciais.
# MERGE garante idempotência: rodar o notebook duas vezes não duplica registros.
spark.sql("""
    MERGE INTO retail_lakehouse.bronze.pipeline_config AS target
    USING (
        SELECT * FROM VALUES
            ('kafka_bootstrap',    'pkc-oxqxx9.us-east-1.aws.confluent.cloud:9092', 'Confluent Cloud bootstrap server',          current_timestamp()),
            ('kafka_consumer_group','retail-lakehouse-dlt',                          'Consumer group dos pipelines DLT',          current_timestamp()),
            ('topic_vendas',       'retail.vendas',                                  'Tópico Kafka de eventos de venda',          current_timestamp()),
            ('topic_estoque',      'retail.estoque',                                 'Tópico Kafka de movimentações de estoque',  current_timestamp()),
            ('topic_clientes',     'retail.clientes',                                'Tópico Kafka de cadastro de clientes',      current_timestamp()),
            ('topic_pagamentos',   'retail.pagamentos',                              'Tópico Kafka de eventos de pagamento',      current_timestamp()),
            ('starting_offsets',   'earliest',                                       'Offset inicial de leitura do Kafka',        current_timestamp()),
            ('secrets_scope',      'retail-lakehouse',                               'Scope do Databricks Secrets',               current_timestamp())
        AS source(config_key, config_value, description, updated_at)
    ) ON target.config_key = source.config_key
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

print("✔ Tabela bronze.pipeline_config criada e populada")

# Exibe as configurações inseridas
display(spark.table("retail_lakehouse.bronze.pipeline_config"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configuração dos Secrets (execute via Databricks CLI)
# MAGIC
# MAGIC Os secrets **não podem ser criados via notebook** — precisam ser configurados
# MAGIC via Databricks CLI na sua máquina local. Execute os comandos abaixo no terminal:
# MAGIC
# MAGIC ```bash
# MAGIC # 1. Instalar a Databricks CLI (se ainda não tiver)
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # 2. Autenticar (gera token em: User Settings → Developer → Access Tokens)
# MAGIC databricks configure --token
# MAGIC
# MAGIC # 3. Criar o scope de secrets do projeto
# MAGIC databricks secrets create-scope retail-lakehouse
# MAGIC
# MAGIC # 4. Adicionar as credenciais do Confluent Cloud
# MAGIC databricks secrets put --scope retail-lakehouse --key confluent_bootstrap_servers
# MAGIC databricks secrets put --scope retail-lakehouse --key confluent_api_key
# MAGIC databricks secrets put --scope retail-lakehouse --key confluent_api_secret
# MAGIC ```
# MAGIC
# MAGIC Após configurar, valide com:
# MAGIC ```bash
# MAGIC databricks secrets list --scope retail-lakehouse
# MAGIC ```
# MAGIC
# MAGIC > **Nunca coloque credenciais diretamente no código.**
# MAGIC > No Databricks, acesse com: `dbutils.secrets.get("retail-lakehouse", "confluent_api_key")`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação Final

# COMMAND ----------

# Valida que tudo foi criado corretamente
print("=" * 50)
print("VALIDAÇÃO DA INFRAESTRUTURA")
print("=" * 50)

# Verifica schemas
schemas_criados = [row.databaseName for row in spark.sql("SHOW SCHEMAS IN retail_lakehouse").collect()]
for schema, _ in schemas:
    status = "✔" if schema in schemas_criados else "✘"
    print(f"{status} Schema: retail_lakehouse.{schema}")

# Verifica volumes
print()
for schema, volume, _ in volumes:
    try:
        dbutils.fs.ls(f"/Volumes/retail_lakehouse/{schema}/{volume}")
        print(f"✔ Volume: /Volumes/retail_lakehouse/{schema}/{volume}")
    except Exception:
        print(f"✘ Volume: /Volumes/retail_lakehouse/{schema}/{volume}")

# Verifica tabela de configuração
count = spark.table("retail_lakehouse.bronze.pipeline_config").count()
print(f"\n✔ pipeline_config: {count} configurações registradas")
print("\n✔ Setup concluído — pronto para os pipelines DLT")
