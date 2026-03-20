WITH vendas AS (
    SELECT * FROM {{ ref('stg_vendas') }}
)

SELECT DISTINCT
    produto_id,
    -- produto_id vem no formato "PROD-0001" — extraímos o número para ordenação
    CAST(REGEXP_REPLACE(produto_id, 'PROD-', '') AS INT) AS produto_num
FROM vendas
ORDER BY produto_num
