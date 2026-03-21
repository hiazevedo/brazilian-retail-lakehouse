# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Contract Registry
# MAGIC
# MAGIC Notebook que lê os arquivos YAML de contratos de dados,
# MAGIC faz o parse e popula as tabelas de registro no Unity Catalog.
# MAGIC
# MAGIC **Execute este notebook sempre que um contrato for criado ou atualizado.**
# MAGIC
# MAGIC ## Tabelas criadas/atualizadas:
# MAGIC - `retail_lakehouse.contracts.registry`   → contratos ativos e suas versões
# MAGIC - `retail_lakehouse.contracts.violations` → violações detectadas (criada vazia)

# COMMAND ----------

import json
import yaml
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Copiar YAMLs para o Volume UC

# COMMAND ----------

# Os arquivos YAML estão no repositório Git (pasta contracts/).
# Precisamos copiá-los para o Volume do Unity Catalog para que
# outros notebooks possam lê-los sem depender do bundle.
#
# O Volume é o local permanente dos contratos no ambiente Databricks.
# O repositório Git é a fonte de verdade — o Volume é o espelho.

VOLUME_PATH = "/Volumes/retail_lakehouse/contracts/schemas"
BUNDLE_PATH = "/Workspace/Users/higor_com@hotmail.com/.bundle/brazilian-retail-lakehouse/dev/files/contracts"

contratos = ["vendas_v1", "estoque_v1", "clientes_v1", "pagamentos_v1"]

for contrato in contratos:
    src  = f"{BUNDLE_PATH}/{contrato}.yaml"
    dest = f"{VOLUME_PATH}/{contrato}.yaml"
    dbutils.fs.cp(src, dest, recurse=False)
    print(f"✔ Copiado: {contrato}.yaml → Volume UC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar tabelas do Registry

# COMMAND ----------

# Tabela principal de contratos.
# schema_json e quality_rules_json armazenam os campos serializados como JSON —
# isso permite consultas flexíveis com from_json() em outros notebooks.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.contracts.registry (
        contract_id        STRING    COMMENT 'Identificador único do contrato (igual ao tópico Kafka)',
        version            STRING    COMMENT 'Versão semântica do contrato (ex: 1.0)',
        owner              STRING    COMMENT 'Time responsável pelo domínio',
        description        STRING    COMMENT 'Descrição do contrato',
        status             STRING    COMMENT 'Status do contrato: active, deprecated',
        sla_json           STRING    COMMENT 'Definições de SLA serializadas em JSON',
        schema_json        STRING    COMMENT 'Campos e tipos do schema serializados em JSON',
        quality_rules_json STRING    COMMENT 'Regras de qualidade serializadas em JSON',
        created_at         TIMESTAMP COMMENT 'Data de criação do contrato',
        updated_at         TIMESTAMP COMMENT 'Data da última atualização'
    )
    COMMENT 'Registro central de data contracts do projeto'
""")

# Tabela de violações — populada pelo validator (notebook 05).
# Cada linha representa uma violação detectada em uma execução do DLT.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.contracts.violations (
        detected_at        TIMESTAMP COMMENT 'Momento em que a violação foi detectada',
        contract_id        STRING    COMMENT 'Contrato violado',
        version            STRING    COMMENT 'Versão do contrato no momento da violação',
        table_full_name    STRING    COMMENT 'Tabela onde a violação foi detectada',
        rule_name          STRING    COMMENT 'Nome da regra violada',
        severity           STRING    COMMENT 'Severidade: CRITICAL ou WARNING',
        violation_count    BIGINT    COMMENT 'Quantidade de registros que violaram a regra',
        sample_values      STRING    COMMENT 'Amostra de valores que causaram a violação',
        run_id             STRING    COMMENT 'ID da execução do pipeline',
        run_date           DATE      COMMENT 'Data da execução'
    )
    COMMENT 'Registro histórico de violações de data contracts'
""")

print("✔ Tabelas contracts.registry e contracts.violations criadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse dos YAMLs e carga no Registry

# COMMAND ----------

def parse_and_register_contract(contract_name: str):
    """
    Lê um arquivo YAML do Volume UC, faz o parse e registra
    na tabela contracts.registry via MERGE INTO.
    """
    yaml_path = f"{VOLUME_PATH}/{contract_name}.yaml"

    # Lê o arquivo YAML do Volume como texto
    content = dbutils.fs.head(yaml_path, max_bytes=65536)

    # Faz o parse do YAML para dicionário Python
    contract = yaml.safe_load(content)

    contract_id = contract["contract_id"]
    version     = contract["version"]
    owner       = contract["owner"]
    description = contract["description"]
    now         = datetime.utcnow().isoformat()

    # Serializa as seções estruturadas como JSON para armazenar no Delta
    sla_json           = json.dumps(contract.get("sla", {}),           ensure_ascii=False)
    schema_json        = json.dumps(contract.get("schema", []),        ensure_ascii=False)
    quality_rules_json = json.dumps(contract.get("quality_rules", []), ensure_ascii=False)

    # Escapa aspas simples para não quebrar o SQL
    description_safe        = description.replace("'", "\\'")
    sla_json_safe           = sla_json.replace("'", "\\'")
    schema_json_safe        = schema_json.replace("'", "\\'")
    quality_rules_json_safe = quality_rules_json.replace("'", "\\'")

    # MERGE INTO: atualiza se já existe, insere se é novo
    # A chave é contract_id + version — cada versão é um registro separado
    spark.sql(f"""
        MERGE INTO retail_lakehouse.contracts.registry AS target
        USING (
            SELECT
                '{contract_id}'        AS contract_id,
                '{version}'            AS version,
                '{owner}'              AS owner,
                '{description_safe}'   AS description,
                'active'               AS status,
                '{sla_json_safe}'      AS sla_json,
                '{schema_json_safe}'   AS schema_json,
                '{quality_rules_json_safe}' AS quality_rules_json,
                CAST('{now}' AS TIMESTAMP) AS created_at,
                CAST('{now}' AS TIMESTAMP) AS updated_at
        ) AS source
        ON target.contract_id = source.contract_id
        AND target.version    = source.version
        WHEN MATCHED THEN UPDATE SET
            owner              = source.owner,
            description        = source.description,
            status             = source.status,
            sla_json           = source.sla_json,
            schema_json        = source.schema_json,
            quality_rules_json = source.quality_rules_json,
            updated_at         = source.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✔ Contrato registrado: {contract_id} v{version}")

# Processa os 4 contratos
for contrato in contratos:
    parse_and_register_contract(contrato)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validação

# COMMAND ----------

print("=" * 55)
print("CONTRACTS REGISTRY — RESUMO")
print("=" * 55)

df = spark.sql("""
    SELECT
        contract_id,
        version,
        owner,
        status,
        updated_at
    FROM retail_lakehouse.contracts.registry
    ORDER BY contract_id
""")

display(df)

# Exibe os SLAs de cada contrato para conferência
print("\nSLAs registrados:")
for row in spark.sql("""
    SELECT contract_id, sla_json
    FROM retail_lakehouse.contracts.registry
    ORDER BY contract_id
""").collect():
    sla = json.loads(row.sla_json)
    print(f"  {row.contract_id}: freshness={sla.get('freshness_minutes')}min, "
          f"min_volume={sla.get('min_daily_volume')}/dia")
