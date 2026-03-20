# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Contract Validator
# MAGIC
# MAGIC Notebook que lê os contratos ativos do registry, executa as regras
# MAGIC de qualidade contra as tabelas Silver e registra violações.
# MAGIC
# MAGIC **Execute após cada execução do DLT Pipeline.**
# MAGIC
# MAGIC ## Comportamento:
# MAGIC - Violações CRITICAL → registra em contracts.violations e levanta erro
# MAGIC - Violações WARNING  → registra em contracts.violations e continua
# MAGIC - Sem violações      → registra conformidade e segue normalmente

# COMMAND ----------

import json
from datetime import date, datetime
from pyspark.sql import functions as F

# COMMAND ----------

# Mapeamento entre contract_id e tabela Silver correspondente.
# Usado para saber em qual tabela aplicar cada conjunto de regras.

TABELA_POR_CONTRATO = {
    "retail.vendas":     "retail_lakehouse.silver.vendas",
    "retail.estoque":    "retail_lakehouse.silver.estoque",
    "retail.clientes":   "retail_lakehouse.silver.clientes",
    "retail.pagamentos": "retail_lakehouse.silver.pagamentos",
}

# ID único desta execução — usado para agrupar violações do mesmo run
RUN_ID   = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
RUN_DATE = date.today().isoformat()

print(f"Run ID: {RUN_ID}")
print(f"Run Date: {RUN_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar contratos ativos

# COMMAND ----------

# Busca todos os contratos com status 'active' do registry.
# Apenas contratos ativos são validados — contratos deprecated são ignorados.

contratos_ativos = spark.sql("""
    SELECT
        contract_id,
        version,
        quality_rules_json
    FROM retail_lakehouse.contracts.registry
    WHERE status = 'active'
    ORDER BY contract_id
""").collect()

print(f"✔ {len(contratos_ativos)} contratos ativos encontrados")
for c in contratos_ativos:
    print(f"  → {c.contract_id} v{c.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Executar validações

# COMMAND ----------

def validar_contrato(contract_id: str, version: str, quality_rules_json: str) -> list:
    """
    Executa todas as regras de qualidade de um contrato contra a tabela Silver.
    Retorna lista de violações encontradas.
    """
    tabela = TABELA_POR_CONTRATO.get(contract_id)
    if not tabela:
        print(f"  ⚠ Tabela não mapeada para contrato: {contract_id} — pulando")
        return []

    regras = json.loads(quality_rules_json)
    violacoes = []

    for regra in regras:
        rule_sql    = regra["rule"]
        severity    = regra["severity"]
        description = regra.get("description", rule_sql)

        # Conta quantos registros violam a regra
        # A regra é uma expressão SQL válida — negamos para encontrar violações
        violation_count = spark.sql(f"""
            SELECT COUNT(*) AS violation_count
            FROM {tabela}
            WHERE NOT ({rule_sql})
        """).collect()[0]["violation_count"]

        if violation_count > 0:
            # Coleta até 2 linhas de exemplo para facilitar diagnóstico
            # Serializa a linha inteira como JSON para não depender de colunas específicas
            samples = spark.sql(f"""
                SELECT TO_JSON(STRUCT(*)) AS row_json
                FROM {tabela}
                WHERE NOT ({rule_sql})
                LIMIT 2
            """).collect()
            sample_str = json.dumps([r["row_json"] for r in samples], ensure_ascii=False)

            violacoes.append({
                "contract_id":     contract_id,
                "version":         version,
                "table_full_name": tabela,
                "rule_name":       description,
                "severity":        severity,
                "violation_count": violation_count,
                "sample_values":   sample_str,
            })

            icon = "✘" if severity == "CRITICAL" else "⚠"
            print(f"  {icon} [{severity}] {description}: {violation_count:,} registros")
        else:
            print(f"  ✔ {description}: OK")

    return violacoes

# Executa validações para todos os contratos ativos
todas_violacoes = []

for contrato in contratos_ativos:
    print(f"\n{'='*55}")
    print(f"Validando: {contrato.contract_id} v{contrato.version}")
    print(f"{'='*55}")

    violacoes = validar_contrato(
        contrato.contract_id,
        contrato.version,
        contrato.quality_rules_json,
    )
    todas_violacoes.extend(violacoes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registrar violações

# COMMAND ----------

if todas_violacoes:
    # Converte a lista de violações para DataFrame e insere na tabela
    df_violacoes = spark.createDataFrame(todas_violacoes).withColumns({
        "detected_at": F.current_timestamp(),
        "run_id":      F.lit(RUN_ID),
        "run_date":    F.lit(RUN_DATE).cast("date"),
    })

    df_violacoes.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("retail_lakehouse.contracts.violations")

    print(f"\n✔ {len(todas_violacoes)} violações registradas em contracts.violations")
else:
    print("\n✔ Nenhuma violação encontrada — todos os contratos estão em conformidade")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Relatório de conformidade

# COMMAND ----------

# Exibe resumo de conformidade por contrato e severidade
display(spark.sql("""
    SELECT
        contract_id,
        severity,
        COUNT(*)              AS total_regras_violadas,
        SUM(violation_count)  AS total_registros_afetados,
        MAX(detected_at)      AS ultima_deteccao
    FROM retail_lakehouse.contracts.violations
    WHERE run_id = '{}'
    GROUP BY contract_id, severity
    ORDER BY contract_id, severity
""".format(RUN_ID)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificação final — falha em CRITICAL

# COMMAND ----------

# Após registrar tudo, verifica se há violações CRITICAL.
# Se houver, levanta um erro para sinalizar ao Databricks Workflow
# que este job falhou — isso aciona alertas e bloqueia jobs dependentes.

violacoes_criticas = [v for v in todas_violacoes if v["severity"] == "CRITICAL"]

if violacoes_criticas:
    resumo = "\n".join([
        f"  - {v['contract_id']}: {v['rule_name']} ({v['violation_count']:,} registros)"
        for v in violacoes_criticas
    ])
    raise Exception(
        f"❌ {len(violacoes_criticas)} violação(ões) CRITICAL detectada(s):\n{resumo}\n"
        f"Verifique contracts.violations para detalhes (run_id={RUN_ID})"
    )

print("✔ Validação concluída — nenhuma violação CRITICAL encontrada")
