with vendas_deposito as (
    select
        fil.deposito_id,
        vd.produto_id,
        vd.quantidade_venda_diaria
    from {{ ref('stg_vendas_filial') }} as vd
    inner join
        {{ ref('stg_filial_deposito') }} as fil
        on vd.filial_id = fil.filial_id
),

final as (
    select
        deposito_id,
        produto_id,
        sum(quantidade_venda_diaria) as quantidade_venda_diaria
    from vendas_deposito
    group by
        deposito_id,
        produto_id
)

select * from final
