WITH fct AS (
    SELECT * FROM {{ ref('fct_vendas') }}
)

SELECT
    loja_id,
    uf,
    ano_mes,
    COUNT(DISTINCT venda_id)                                    AS total_vendas,
    SUM(valor_total)                                            AS receita_total,
    AVG(valor_total)                                            AS ticket_medio,
    COUNT(DISTINCT cliente_id)                                  AS clientes_unicos,
    SUM(CASE WHEN metodo_pagamento = 'pix'     THEN 1 ELSE 0 END) AS vendas_pix,
    SUM(CASE WHEN metodo_pagamento = 'credito' THEN 1 ELSE 0 END) AS vendas_credito,
    SUM(CASE WHEN status_pagamento = 'recusado' THEN 1 ELSE 0 END) AS pagamentos_recusados
FROM fct
GROUP BY loja_id, uf, ano_mes
