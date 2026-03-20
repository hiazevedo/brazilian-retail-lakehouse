WITH estoque AS (
    SELECT * FROM {{ ref('stg_estoque') }}
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY loja_id, produto_id
            ORDER BY registrado_em DESC
        ) AS rn
    FROM estoque
)

SELECT
    loja_id,
    produto_id,
    estoque_atual,
    tipo_movimento  AS ultimo_movimento,
    registrado_em   AS ultima_atualizacao
FROM ranked
WHERE rn = 1
