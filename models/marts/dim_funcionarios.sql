with 
dim_funcionarios as (
    select *
    from {{ ref('INT_VENDAS__SELF_join_funcionarios') }}
)

select *
from dim_funcionarios