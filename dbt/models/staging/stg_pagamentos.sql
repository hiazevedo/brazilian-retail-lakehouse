WITH fonte AS (
    SELECT * FROM {{ source('silver', 'pagamentos') }}
)

SELECT
    pagamento_id,
    venda_id,
    timestamp        AS pago_em,
    metodo,
    status,
    parcelas,
    ano_mes
FROM fonte
