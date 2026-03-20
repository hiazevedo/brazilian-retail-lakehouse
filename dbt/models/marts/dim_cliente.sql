WITH clientes AS (
    SELECT * FROM {{ ref('stg_clientes') }}
),

-- Pega apenas o registro mais recente de cada cliente
mais_recente AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY cliente_id
            ORDER BY atualizado_em DESC
        ) AS rn
    FROM clientes
)

SELECT
    cliente_id,
    cidade,
    uf,
    faixa_renda,
    canal_preferido,
    atualizado_em
FROM mais_recente
WHERE rn = 1
