with produto_dermo as (
    select
        pm.prme_cd_produto,
        pc.capn_ds_categoria as categoria
    from
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} as pm
    inner join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_categoria_produto_novo') }}
            as pc
        on
            substring(pm.capn_cd_categoria, 1, 5) || '.000.00.00.00.00.00'
            = pc.capn_cd_categoria
),

filtro_dermo as (
    select prme_cd_produto as produto_id
    from produto_dermo
    where categoria = 'DERMO'
)

select * from filtro_dermo
