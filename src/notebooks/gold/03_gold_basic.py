# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Basic: Agregações Analíticas
# MAGIC
# MAGIC Notebook Spark que lê as tabelas Silver e gera as primeiras
# MAGIC agregações analíticas na camada Gold.
# MAGIC
# MAGIC **Na Phase 3 este notebook será substituído por modelos dbt.**
# MAGIC Por agora, valida que o pipeline funciona end-to-end.
# MAGIC
# MAGIC ## Tabelas criadas/atualizadas:
# MAGIC - `retail_lakehouse.gold.vendas_por_loja_dia`
# MAGIC - `retail_lakehouse.gold.top_produtos`
# MAGIC - `retail_lakehouse.gold.status_pagamentos`
# MAGIC - `retail_lakehouse.gold.estoque_atual`

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vendas por Loja e Dia

# COMMAND ----------

# Agrega as vendas da Silver por loja e dia.
# Esta é a tabela base para dashboards de performance de loja.
#
# MERGE INTO garante idempotência:
# - Se já existe o registro para loja_id + data: atualiza os valores
# - Se não existe: insere novo registro
# Isso permite rodar este notebook múltiplas vezes sem duplicar dados.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.vendas_por_loja_dia (
        loja_id          STRING  COMMENT 'Identificador da loja',
        uf               STRING  COMMENT 'Estado da loja',
        data             DATE    COMMENT 'Data das vendas',
        ano_mes          STRING  COMMENT 'Ano-mês para filtros mensais',
        total_vendas     BIGINT  COMMENT 'Quantidade de transações no dia',
        receita_bruta    DOUBLE  COMMENT 'Soma do valor_total sem descontos adicionais',
        ticket_medio     DOUBLE  COMMENT 'Receita bruta / total de vendas',
        qtd_itens        BIGINT  COMMENT 'Total de itens vendidos',
        vendas_online    BIGINT  COMMENT 'Transações pelo canal online',
        vendas_fisico    BIGINT  COMMENT 'Transações pelo canal físico',
        updated_at       TIMESTAMP COMMENT 'Última atualização do registro'
    )
    COMMENT 'Vendas agregadas por loja e dia — base para dashboard de performance'

""")

spark.sql("""
    MERGE INTO retail_lakehouse.gold.vendas_por_loja_dia AS target
    USING (
        SELECT
            loja_id,
            uf,
            CAST(timestamp AS DATE)                         AS data,
            ano_mes,
            COUNT(*)                                        AS total_vendas,
            ROUND(SUM(valor_total), 2)                      AS receita_bruta,
            ROUND(SUM(valor_total) / COUNT(*), 2)           AS ticket_medio,
            SUM(quantidade)                                 AS qtd_itens,
            COUNT(CASE WHEN canal = 'online'  THEN 1 END)  AS vendas_online,
            COUNT(CASE WHEN canal = 'fisico'  THEN 1 END)  AS vendas_fisico,
            current_timestamp()                             AS updated_at
        FROM retail_lakehouse.silver.vendas
        GROUP BY loja_id, uf, CAST(timestamp AS DATE), ano_mes
    ) AS source
    ON target.loja_id = source.loja_id
    AND target.data   = source.data
    AND target.uf     = source.uf
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table("retail_lakehouse.gold.vendas_por_loja_dia").count()
print(f"✔ vendas_por_loja_dia: {count:,} registros")
display(spark.table("retail_lakehouse.gold.vendas_por_loja_dia").orderBy("data", "loja_id").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top Produtos

# COMMAND ----------

# Ranking de produtos por volume total vendido.
# Útil para gestão de estoque e decisões de compra.
#
# Nota: produto_id foi formatado como "PROD-0001" no Silver.
# Aqui apenas agregamos — sem transformações adicionais.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.top_produtos (
        produto_id       STRING  COMMENT 'Identificador do produto (PROD-XXXX)',
        total_vendas     BIGINT  COMMENT 'Número total de transações com este produto',
        qtd_itens        BIGINT  COMMENT 'Total de unidades vendidas',
        receita_total    DOUBLE  COMMENT 'Receita total gerada pelo produto',
        ticket_medio     DOUBLE  COMMENT 'Valor médio por transação',
        preco_medio      DOUBLE  COMMENT 'Preço unitário médio praticado',
        desconto_medio   DOUBLE  COMMENT 'Percentual médio de desconto aplicado',
        updated_at       TIMESTAMP COMMENT 'Última atualização do registro'
    )
    COMMENT 'Ranking de produtos por volume vendido'
""")

spark.sql("""
    MERGE INTO retail_lakehouse.gold.top_produtos AS target
    USING (
        SELECT
            produto_id,
            COUNT(*)                            AS total_vendas,
            SUM(quantidade)                     AS qtd_itens,
            ROUND(SUM(valor_total), 2)          AS receita_total,
            ROUND(SUM(valor_total) / COUNT(*), 2) AS ticket_medio,
            ROUND(AVG(preco_unitario), 2)       AS preco_medio,
            ROUND(AVG(desconto_pct), 4)         AS desconto_medio,
            current_timestamp()                 AS updated_at
        FROM retail_lakehouse.silver.vendas
        GROUP BY produto_id
    ) AS source
    ON target.produto_id = source.produto_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table("retail_lakehouse.gold.top_produtos").count()
print(f"✔ top_produtos: {count:,} produtos")
display(
    spark.table("retail_lakehouse.gold.top_produtos")
    .orderBy("receita_total", ascending=False)
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Status de Pagamentos

# COMMAND ----------

# Distribuição de pagamentos por método e status.
# Útil para monitorar taxas de aprovação e detectar anomalias
# (ex: spike repentino de pagamentos recusados pode indicar problema).

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.status_pagamentos (
        ano_mes          STRING  COMMENT 'Ano-mês de referência',
        metodo           STRING  COMMENT 'Método de pagamento (pix, debito, credito, dinheiro)',
        status           STRING  COMMENT 'Status do pagamento (aprovado, recusado, estorno)',
        total            BIGINT  COMMENT 'Quantidade de pagamentos nesta combinação',
        pct_do_total     DOUBLE  COMMENT 'Percentual em relação ao total do mês',
        updated_at       TIMESTAMP COMMENT 'Última atualização do registro'
    )
    COMMENT 'Distribuição de pagamentos por método e status'

""")

spark.sql("""
    MERGE INTO retail_lakehouse.gold.status_pagamentos AS target
    USING (
        WITH base AS (
            SELECT
                ano_mes,
                metodo,
                status,
                COUNT(*) AS total
            FROM retail_lakehouse.silver.pagamentos
            GROUP BY ano_mes, metodo, status
        ),
        totais_mes AS (
            SELECT ano_mes, SUM(total) AS total_mes
            FROM base
            GROUP BY ano_mes
        )
        SELECT
            b.ano_mes,
            b.metodo,
            b.status,
            b.total,
            ROUND(b.total / t.total_mes * 100, 2) AS pct_do_total,
            current_timestamp()                    AS updated_at
        FROM base b
        JOIN totais_mes t ON b.ano_mes = t.ano_mes
    ) AS source
    ON  target.ano_mes = source.ano_mes
    AND target.metodo  = source.metodo
    AND target.status  = source.status
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table("retail_lakehouse.gold.status_pagamentos").count()
print(f"✔ status_pagamentos: {count:,} registros")
display(
    spark.table("retail_lakehouse.gold.status_pagamentos")
    .orderBy("ano_mes", "metodo", "status")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Estoque Atual

# COMMAND ----------

# Posição atual de estoque por produto e loja.
# Usa a última movimentação registrada (Window Function com ROW_NUMBER)
# para obter o estoque_atual mais recente de cada combinação produto+loja.
#
# Window Functions são a forma correta de fazer "last value" no Spark.
# Evitamos GROUP BY + MAX porque queremos a linha inteira do último evento,
# não apenas o valor máximo de uma coluna.

spark.sql("""
    CREATE TABLE IF NOT EXISTS retail_lakehouse.gold.estoque_atual (
        produto_id       STRING  COMMENT 'Identificador do produto',
        loja_id          STRING  COMMENT 'Identificador da loja',
        estoque_atual    INT     COMMENT 'Quantidade atual em estoque',
        ultimo_movimento STRING  COMMENT 'Tipo do último movimento registrado',
        ultima_atualizacao TIMESTAMP COMMENT 'Timestamp do último evento de estoque',
        status_estoque   STRING  COMMENT 'critico (<10), baixo (<50), normal (>=50)',
        updated_at       TIMESTAMP COMMENT 'Última atualização do registro'
    )
    COMMENT 'Posição atual de estoque por produto e loja'
""")

spark.sql("""
    MERGE INTO retail_lakehouse.gold.estoque_atual AS target
    USING (
        -- ROW_NUMBER() rankeia os eventos por produto+loja, mais recente primeiro
        -- Ficamos apenas com rank=1 (último evento de cada combinação)
        WITH ranked AS (
            SELECT
                produto_id,
                loja_id,
                estoque_atual,
                tipo_movimento          AS ultimo_movimento,
                timestamp               AS ultima_atualizacao,
                ROW_NUMBER() OVER (
                    PARTITION BY produto_id, loja_id
                    ORDER BY timestamp DESC
                ) AS rn
            FROM retail_lakehouse.silver.estoque
        )
        SELECT
            produto_id,
            loja_id,
            estoque_atual,
            ultimo_movimento,
            ultima_atualizacao,
            -- Classifica o nível de estoque para facilitar alertas
            CASE
                WHEN estoque_atual < 10  THEN 'critico'
                WHEN estoque_atual < 50  THEN 'baixo'
                ELSE                          'normal'
            END                         AS status_estoque,
            current_timestamp()         AS updated_at
        FROM ranked
        WHERE rn = 1
    ) AS source
    ON  target.produto_id = source.produto_id
    AND target.loja_id    = source.loja_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

count = spark.table("retail_lakehouse.gold.estoque_atual").count()
criticos = spark.sql("SELECT COUNT(*) FROM retail_lakehouse.gold.estoque_atual WHERE status_estoque = 'critico'").collect()[0][0]
print(f"✔ estoque_atual: {count:,} combinações produto+loja")
print(f"⚠ Produtos em estoque crítico: {criticos:,}")
display(
    spark.table("retail_lakehouse.gold.estoque_atual")
    .orderBy("estoque_atual")
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Final

# COMMAND ----------

print("=" * 55)
print("GOLD BASIC — RESUMO")
print("=" * 55)

tabelas = [
    "gold.vendas_por_loja_dia",
    "gold.top_produtos",
    "gold.status_pagamentos",
    "gold.estoque_atual",
]

for tabela in tabelas:
    count = spark.table(f"retail_lakehouse.{tabela}").count()
    print(f"✔ {tabela}: {count:,} registros")

print("\n✔ Phase 1 completa — pipeline end-to-end funcionando")
