with 
vendas_em_2012(
    select sum(total_bruto) as soma_total_bruto
    from {{ ref('int_vendas__metricas_de_vendas') }}
    where data_do_pedido between '2012-01-01' and '2012-12-31'
)

SELECT soma_total_bruto
FROM vendas_em_2012
WHERE soma_total_bruto = 230784.68
