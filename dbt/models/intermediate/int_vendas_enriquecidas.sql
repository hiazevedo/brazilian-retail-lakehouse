WITH vendas AS (
    SELECT * FROM {{ ref('stg_vendas') }}
),

pagamentos AS (
    SELECT * FROM {{ ref('stg_pagamentos') }}
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
    p.pagamento_id,
    p.metodo          AS metodo_pagamento,
    p.status          AS status_pagamento,
    p.parcelas
FROM vendas v
LEFT JOIN pagamentos p ON v.venda_id = p.venda_id
