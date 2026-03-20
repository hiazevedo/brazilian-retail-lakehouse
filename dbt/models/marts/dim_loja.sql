WITH vendas AS (
    SELECT * FROM {{ ref('stg_vendas') }}
)

SELECT
    loja_id,
    MAX(uf)          AS estado
FROM vendas
GROUP BY loja_id
