WITH fct AS (
    SELECT * FROM {{ ref('fct_vendas') }}
),

-- Calcula métricas de comportamento por cliente
perfil_cliente AS (
    SELECT
        cliente_id,
        AVG(valor_total)    AS ticket_medio_cliente,
        STDDEV(valor_total) AS desvio_padrao_cliente
    FROM fct
    GROUP BY cliente_id
)

SELECT
    f.venda_id,
    f.vendido_em,
    f.cliente_id,
    f.loja_id,
    f.valor_total,
    f.metodo_pagamento,
    f.status_pagamento,
    f.parcelas,
    p.ticket_medio_cliente,
    -- Venda é suspeita se valor > 3x o ticket médio do cliente
    CASE
        WHEN f.valor_total > (p.ticket_medio_cliente + 3 * p.desvio_padrao_cliente)
            THEN 'alto_risco'
        WHEN f.status_pagamento = 'recusado' AND f.parcelas > 6
            THEN 'medio_risco'
        ELSE 'normal'
    END AS classificacao_risco
FROM fct f
LEFT JOIN perfil_cliente p ON f.cliente_id = p.cliente_id
WHERE
    -- Filtra apenas casos que merecem atenção
    f.valor_total > (p.ticket_medio_cliente + 3 * p.desvio_padrao_cliente)
    OR (f.status_pagamento = 'recusado' AND f.parcelas > 6)
