WITH fonte AS (
    SELECT * FROM {{ source('silver', 'estoque') }}
)

SELECT
    evento_id,
    timestamp        AS registrado_em,
    loja_id,
    produto_id,
    tipo_movimento,
    quantidade,
    estoque_atual,
    ano_mes,
    hora_dia
FROM fonte
