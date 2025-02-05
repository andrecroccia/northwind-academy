with 
ordens as (select * from {{ ref('stg_erp__ordens') }}),
pedido_itens as (select * from {{ ref('stg_erp__ordem_itens') }}),

joined as (
    select

    pedido_itens.fk_produto
    , ordens.fk_funcionario  
    , ordens.fk_cliente
    , ordens.fk_transportadora
    , ordens.data_do_pedido
    , ordens.data_do_envio
    , ordens.data_requerida_entrega
    , pedido_itens.desconto_perc
    , pedido_itens.preco_da_unidade
    , pedido_itens.quantidade
    , ordens.frete 
    , ordens.numero_pedido
    , ordens.nm_destinatario
    , ordens.cidade_destinatario
    , ordens.regiao_destinatario
    , ordens.pais_destinatario
         
    from pedido_itens
    inner join ordens on pedido_itens.fk_pedido = ordens.pk_pedido
)

, metricas as (
    select
      fk_produto
    , fk_funcionario  
    , fk_cliente
    , fk_transportadora
    , data_do_pedido
    , data_do_envio
    , data_requerida_entrega
    , desconto_perc
    , preco_da_unidade
    , quantidade
    , preco_da_unidade * quantidade as total_bruto
    , preco_da_unidade * quantidade * (1 - desconto_perc) as total_liquido
    , cast(
        (frete / count(*) over (partition by numero_pedido))
        as numeric(18,2)) as frete_rateado
    , case 
        when desconto_perc > 0 then true
        else false 
        end as teve_desconto
    , numero_pedido
    , nm_destinatario
    , cidade_destinatario
    , regiao_destinatario
    , pais_destinatario

    from joined
)

select *
from metricas