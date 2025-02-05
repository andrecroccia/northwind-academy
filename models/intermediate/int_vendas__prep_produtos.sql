with
/* chmada dos modelos necessarios */

categorias as (
    select *
    from {{ ref('stg_erp__categorias') }}

)
, fornecedores as (
    select *
    from {{ ref('stg_erp__fornecedores') }}


)
, produtos as (
    select *
    from {{ ref('stg_erp__produtos') }}

)
, enriquecer_produtos as (
    select 
        produtos.PK_PRODUTO
        ,produtos.NM_PRODUTO
        ,produtos.QUANTIDADE_POR_UNIDADE
        ,produtos.PRECO_POR_UNIDADE
        ,produtos.UNIDADE_EM_ESTOQUE
        ,produtos.UNIDADE_POR_PEDIDO
        ,produtos.NIVEL_DE_PEDIDO
        ,produtos.EH_DISCONTINUADO
        , categorias.nome_categoria
        , fornecedores.nome_fornecedor
        ,  fornecedores.cidade_fornecedor
        ,  fornecedores.pais_fornecedor
    from produtos
    left join categorias on produtos.fk_categoria = categorias.pk_categoria
    left join fornecedores on produtos.fk_fornecedor =  fornecedores.pk_fornecedor

)
SELECT *
FROM enriquecer_produtos