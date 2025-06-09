with estoque_reservado as (
    select
        cab.depo_cd_deposito,
        det.nede_cd_produto,
        det.quantidade_saldo_avaria
    from {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_det') }} as det
    inner join {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_cab') }} as cab
        on det.neca_sq_notafiscal = cab.neca_sq_notafiscal
),

renamed as (
    select
        depo_cd_deposito as deposito_id,
        nede_cd_produto as produto_id,
        quantidade_saldo_avaria as quantidade_reservada
    from estoque_reservado
),

final as (
    select
        deposito_id,
        produto_id,
        sum(quantidade_reservada) as quantidade_reservada
    from renamed
    where
        deposito_id is not null
        and quantidade_reservada > 0
    group by deposito_id, produto_id
)

select * from final
