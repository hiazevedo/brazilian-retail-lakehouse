WITH vendas AS (
    SELECT * FROM {{ ref('int_vendas_enriquecidas') }}
),

clientes AS (
    SELECT * FROM {{ ref('dim_cliente') }}
)

SELECT
    v.venda_id,
    v.vendido_em,
    v.loja_id,
    v.produto_id,
    v.cliente_id,
    v.quantidade,
    v.preco_unitario,
    v.desconto_pct,
    v.valor_total,
    v.canal,
    v.uf,
    v.ano_mes,
    v.hora_dia,
    v.dia_semana,
    v.pagamento_id,
    v.metodo_pagamento,
    v.status_pagamento,
    v.parcelas,
    -- Contexto do cliente no momento da venda
    c.faixa_renda      AS cliente_faixa_renda,
    c.cidade           AS cliente_cidade
FROM vendas v
LEFT JOIN clientes c ON v.cliente_id = c.cliente_id
