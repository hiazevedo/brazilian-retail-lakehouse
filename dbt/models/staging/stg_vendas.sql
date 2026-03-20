WITH fonte AS (
    SELECT * FROM {{ source('silver', 'vendas') }}
)

SELECT
    venda_id,
    timestamp        AS vendido_em,
    loja_id,
    produto_id,
    cliente_id,
    quantidade,
    preco_unitario,
    desconto_pct,
    valor_total,
    canal,
    uf,
    ano_mes,
    hora_dia,
    dia_semana
FROM fonte
