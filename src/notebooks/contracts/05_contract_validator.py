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

TABLE_BY_CONTRACT = {
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

# Busca todos os contratos com status 'active' do registry.
# Apenas contratos ativos são validados — contratos deprecated são ignorados.

active_contracts = spark.sql("""
    SELECT
        contract_id,
        version,
        quality_rules_json
    FROM retail_lakehouse.contracts.registry
    WHERE status = 'active'
    ORDER BY contract_id
""").collect()

print(f"✔ {len(active_contracts)} contratos ativos encontrados")
for c in active_contracts:
    print(f"  → {c.contract_id} v{c.version}")

# COMMAND ----------

def validar_contrato(contract_id: str, version: str, quality_rules_json: str) -> list:
    """Run all quality rules of a contract against the corresponding Silver table.

    Args:
        contract_id: Contract identifier (e.g. 'retail.vendas').
        version: Contract version string.
        quality_rules_json: JSON string with the list of quality rules.

    Returns:
        List of violation dicts found during validation.
    """
    tabela = TABLE_BY_CONTRACT.get(contract_id)
    if not tabela:
        print(f"  ⚠ Tabela não mapeada para contrato: {contract_id} — pulando")
        return []

    rules = json.loads(quality_rules_json)
    violations = []

    for rule in rules:
        rule_sql    = rule["rule"]
        severity    = rule["severity"]
        description = rule.get("description", rule_sql)

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

            violations.append({
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

    return violations

# Executa validações para todos os contratos ativos
all_violations = []

for contract in active_contracts:
    print(f"\n{'='*55}")
    print(f"Validando: {contract.contract_id} v{contract.version}")
    print(f"{'='*55}")

    violations = validar_contrato(
        contract.contract_id,
        contract.version,
        contract.quality_rules_json,
    )
    all_violations.extend(violations)

# COMMAND ----------

if all_violations:
    # Converte a lista de violações para DataFrame e insere na tabela
    df_violations = spark.createDataFrame(all_violations).withColumns({
        "detected_at": F.current_timestamp(),
        "run_id":      F.lit(RUN_ID),
        "run_date":    F.lit(RUN_DATE).cast("date"),
    })

    df_violations.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("retail_lakehouse.contracts.violations")

    print(f"\n✔ {len(all_violations)} violações registradas em contracts.violations")
else:
    print("\n✔ Nenhuma violação encontrada — todos os contratos estão em conformidade")

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

critical_violations = [v for v in all_violations if v["severity"] == "CRITICAL"]

if critical_violations:
    resumo = "\n".join([
        f"  - {v['contract_id']}: {v['rule_name']} ({v['violation_count']:,} registros)"
        for v in critical_violations
    ])
    raise Exception(
        f"❌ {len(critical_violations)} violação(ões) CRITICAL detectada(s):\n{resumo}\n"
        f"Verifique contracts.violations para detalhes (run_id={RUN_ID})"
    )

print("✔ Validação concluída — nenhuma violação CRITICAL encontrada")
