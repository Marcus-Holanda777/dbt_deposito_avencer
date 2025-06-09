with base_lotes as (
    select
        lt.neca_sq_notafiscal,
        lt.nede_sq_detnf,
        lt.nelo_dt_venclote,
        lt.nelo_qt_lote
    from {{ source('modelled', 'cosmos_v14b_dbo_nota_entr_lote') }}
),

renamed as (
    select
        neca_sq_notafiscal as nota_fiscal_id,
        nede_sq_detnf as detalhe_nota_fiscal_id,
        nelo_dt_venclote as data_vencimento_lote,
        nelo_qt_lote as quantidade_lote
    from base_lotes
),

final as (
    select
        nota_fiscal_id,
        detalhe_nota_fiscal_id,
        data_vencimento_lote,
        quantidade_lote
    from renamed
    where quantidade_lote > 0.0
)

select * from final
