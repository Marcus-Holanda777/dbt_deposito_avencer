with vendas_filial as (
    select
        kafi_cd_filial,
        kafi_cd_produto,
        kafi_qt_mov
    from {{ source('prevencao-perdas', 'kardex_vendas') }}
    where {{ intervalo_vendas('kafi_dh_ocorrreal', 91) }}
),

renamed as (
    select
        kafi_cd_filial as filial_id,
        kafi_cd_produto as produto_id,
        kafi_qt_mov as quantidade_venda
    from vendas_filial
),

final as (
    select
        filial_id,
        produto_id,
        sum(quantidade_venda) / 90.0 as quantidade_venda_diaria
    from renamed
    group by
        filial_id,
        produto_id
)

select * from final
