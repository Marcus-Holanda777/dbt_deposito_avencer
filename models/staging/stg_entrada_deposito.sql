with entrada_deposito as (
    select
        cab.neca_sq_notafiscal,
        det.nede_sq_detnf,
        cab.depo_cd_deposito,
        det.nede_cd_produto,
        cab.neca_nr_nf,
        cab.neca_dh_atlz,
        det.nede_qt_fisica,
        det.nede_qt_entrada
    from {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_det') }} as det
    inner join {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_cab') }} as cab
        on det.neca_sq_notafiscal = cab.neca_sq_notafiscal
),

renamed as (
    select
        neca_sq_notafiscal as nota_fiscal_id,
        nede_sq_detnf as detalhe_nota_fiscal_id,
        depo_cd_deposito as deposito_id,
        nede_cd_produto as produto_id,
        neca_nr_nf as numero_nota_fiscal,
        neca_dh_atlz as data_hora_atualizacao,
        coalesce(nede_qt_fisica, nede_qt_entrada, 0.0) as quantidade_fisica
    from entrada_deposito
),

final as (
    select
        nota_fiscal_id,
        detalhe_nota_fiscal_id,
        deposito_id,
        produto_id,
        numero_nota_fiscal,
        data_hora_atualizacao,
        quantidade_fisica
    from renamed
    where
        deposito_id is not null
        and quantidade_fisica > 0.0
        and data_hora_atualizacao >= {{ date_corte() }}
)

select * from final
