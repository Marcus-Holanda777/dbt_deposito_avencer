with produto_deposito as (
    select
        depo_cd_deposito,
        prme_cd_produto,
        prdp_vl_cmpcsicms,
        prdp_qt_estoqatual
    from {{ source('modelled', 'cosmos_v14b_dbo_produto_deposito') }}
),

renamed as (
    select
        depo_cd_deposito as deposito_id,
        prme_cd_produto as produto_id,
        prdp_vl_cmpcsicms as valor_custo_sicms,
        prdp_qt_estoqatual as quantidade_estoque_atual
    from produto_deposito
),

final as (
    select
        deposito_id,
        produto_id,
        valor_custo_sicms,
        quantidade_estoque_atual
    from renamed
    where
        deposito_id != 101
        and prdp_qt_estoqatual > 0
)

select * from final
