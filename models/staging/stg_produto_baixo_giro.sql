with produto_baixo_giro as (
    select
        prme_cd_produto,
        prme_dias_validade
    from {{ source('modelled', 'cosmos_v14b_dbo_produto_mestre') }}
),

renamed as (
    select
        prme_cd_produto as produto_id,
        prme_dias_validade as dias_validade
    from produto_baixo_giro
),

final as (
    select
        produto_id,
        dias_validade
    from renamed
    where coalesce(dias_validade, 0) > 0
)

select * from final
