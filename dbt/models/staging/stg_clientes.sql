WITH fonte AS (
    SELECT * FROM {{ source('silver', 'clientes') }}
)

SELECT
    cliente_id,
    timestamp        AS atualizado_em,
    tipo_evento,
    cidade,
    uf,
    faixa_renda,
    canal_preferido,
    ano_mes
FROM fonte
