WITH estoque AS (
    SELECT * FROM {{ ref('int_estoque_posicao') }}
)

SELECT
    loja_id,
    produto_id,
    estoque_atual,
    ultimo_movimento,
    ultima_atualizacao,
    CASE
        WHEN estoque_atual <  10 THEN 'critico'
        WHEN estoque_atual <  50 THEN 'baixo'
        ELSE                          'normal'
    END AS status_estoque
FROM estoque
