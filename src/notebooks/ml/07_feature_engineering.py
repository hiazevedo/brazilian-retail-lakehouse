# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Feature Engineering
# MAGIC
# MAGIC Prepara as feature tables para os dois modelos de ML:
# MAGIC
# MAGIC - `ml_features.demand_features` → previsão de demanda por produto/loja
# MAGIC - `ml_features.fraud_features`  → detecção de transações suspeitas
# MAGIC
# MAGIC **Execute após o dbt run** — as features são derivadas dos marts Gold.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Features de Demanda

# COMMAND ----------

# Lê a tabela fato e o mart de estoque gerados pelo dbt
fct_vendas      = spark.table("retail_lakehouse.gold.fct_vendas")
estoque_atual   = spark.table("retail_lakehouse.gold.mrt_estoque_atual")

# Janelas de tempo para calcular médias históricas por produto e loja
w_produto_loja = Window.partitionBy("produto_id", "loja_id").orderBy("vendido_em")

# Agrega vendas por produto, loja e dia — base para as médias móveis
vendas_dia = (
    fct_vendas
    .groupBy("produto_id", "loja_id", F.to_date("vendido_em").alias("data"))
    .agg(
        F.sum("quantidade").alias("qtd_vendida"),
        F.sum("valor_total").alias("receita_dia"),
        F.countDistinct("venda_id").alias("num_transacoes"),
    )
)

# Converte data para segundos (Unix timestamp) para usar rangeBetween com dias
vendas_dia = vendas_dia.withColumn("data_ts", F.unix_timestamp(F.col("data").cast("timestamp")))

SEGUNDOS_POR_DIA = 86400

# Janelas de tempo baseadas em segundos — garante 7 e 30 dias reais do calendário
# independente de gaps (dias sem venda são ignorados corretamente)
w_7d  = Window.partitionBy("produto_id", "loja_id").orderBy("data_ts").rangeBetween(-6  * SEGUNDOS_POR_DIA, 0)
w_30d = Window.partitionBy("produto_id", "loja_id").orderBy("data_ts").rangeBetween(-29 * SEGUNDOS_POR_DIA, 0)

vendas_com_medias = (
    vendas_dia
    .withColumn("media_qtd_7d",    F.avg("qtd_vendida").over(w_7d))
    .withColumn("media_qtd_30d",   F.avg("qtd_vendida").over(w_30d))
    .withColumn("media_receita_7d", F.avg("receita_dia").over(w_7d))
    # Target: média dos últimos 7 dias como proxy de demanda futura
    # Em produção com dados reais, seria substituído pela soma dos próximos 7 dias
    .withColumn("target_qtd_7d", F.round(F.avg("qtd_vendida").over(w_7d), 0))
)

# Junta com o estoque atual para adicionar contexto de disponibilidade
demand_features = (
    vendas_com_medias
    .join(
        estoque_atual.select("produto_id", "loja_id", "estoque_atual", "status_estoque"),
        on=["produto_id", "loja_id"],
        how="left"
    )
    .withColumn("dia_semana", F.dayofweek("data"))
    .withColumn("mes",        F.month("data"))
    .select(
        "produto_id",
        "loja_id",
        "data",
        "media_qtd_7d",
        "media_qtd_30d",
        "media_receita_7d",
        "num_transacoes",
        F.coalesce(F.col("estoque_atual"), F.lit(0)).alias("estoque_atual"),
        F.coalesce(F.col("status_estoque"), F.lit("normal")).alias("status_estoque"),
        "dia_semana",
        "mes",
        "target_qtd_7d",
    )
    .filter(F.col("media_qtd_7d").isNotNull())
)

# Salva como tabela Delta no schema ml_features
demand_features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_lakehouse.ml_features.demand_features")

print(f"✔ demand_features: {demand_features.count():,} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features de Fraude

# COMMAND ----------

# Calcula o perfil histórico de cada cliente — ticket médio e desvio padrão
perfil_cliente = (
    fct_vendas
    .groupBy("cliente_id")
    .agg(
        F.avg("valor_total").alias("ticket_medio_cliente"),
        F.stddev("valor_total").alias("desvio_padrao_cliente"),
        F.count("venda_id").alias("total_compras_cliente"),
    )
)

# Junta as vendas com o perfil do cliente para calcular o desvio individual
fraud_features = (
    fct_vendas
    .join(perfil_cliente, on="cliente_id", how="left")
    .withColumn(
        "desvio_do_ticket",
        F.when(
            F.col("desvio_padrao_cliente") > 0,
            (F.col("valor_total") - F.col("ticket_medio_cliente"))
            / F.col("desvio_padrao_cliente")
        ).otherwise(F.lit(0.0))
    )
    # Converte metodo_pagamento para numérico — modelos ML precisam de números
    .withColumn(
        "metodo_num",
        F.when(F.col("metodo_pagamento") == "pix",      F.lit(0))
         .when(F.col("metodo_pagamento") == "debito",   F.lit(1))
         .when(F.col("metodo_pagamento") == "credito",  F.lit(2))
         .otherwise(F.lit(3))  # dinheiro
    )
    # Status do pagamento para numérico
    .withColumn(
        "status_num",
        F.when(F.col("status_pagamento") == "aprovado", F.lit(0))
         .when(F.col("status_pagamento") == "recusado", F.lit(1))
         .otherwise(F.lit(2))  # estorno
    )
    # Target: 1 se a venda aparece no mart de risco, 0 caso contrário
    # Simulamos o label com base nas regras do mrt_risco_fraude
    .withColumn(
        "label",
        F.when(
            F.col("desvio_do_ticket") > 3,
            F.lit(1)
        ).when(
            (F.col("status_pagamento") == "recusado") & (F.col("parcelas") > 6),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .select(
        "venda_id",
        "cliente_id",
        "valor_total",
        "ticket_medio_cliente",
        F.coalesce(F.col("desvio_padrao_cliente"), F.lit(0.0)).alias("desvio_padrao_cliente"),
        "desvio_do_ticket",
        F.coalesce(F.col("parcelas"), F.lit(1)).alias("parcelas"),
        "metodo_num",
        "status_num",
        "hora_dia",
        "dia_semana",
        F.coalesce(F.col("total_compras_cliente"), F.lit(1)).alias("total_compras_cliente"),
        "label",
    )
)

# Salva como tabela Delta no schema ml_features
fraud_features.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("retail_lakehouse.ml_features.fraud_features")

total    = fraud_features.count()
fraudes  = fraud_features.filter(F.col("label") == 1).count()
print(f"✔ fraud_features: {total:,} linhas | {fraudes:,} fraudes ({fraudes/total*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação

# COMMAND ----------

print("=" * 55)
print("FEATURE TABLES — RESUMO")
print("=" * 55)

for tabela in ["demand_features", "fraud_features"]:
    df = spark.table(f"retail_lakehouse.ml_features.{tabela}")
    nulos = {col: df.filter(F.col(col).isNull()).count() for col in df.columns}
    nulos_total = sum(nulos.values())
    print(f"\n{tabela}:")
    print(f"  Linhas  : {df.count():,}")
    print(f"  Colunas : {len(df.columns)}")
    print(f"  Nulos   : {nulos_total}")

display(spark.table("retail_lakehouse.ml_features.demand_features").limit(5))
display(spark.table("retail_lakehouse.ml_features.fraud_features").limit(5))
