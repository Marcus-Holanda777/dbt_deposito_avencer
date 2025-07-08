with shelf_life as (
    select
        pm.prme_cd_produto,
        det.capc_cd_catephpai
    from
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_produto_mestre') }} as pm
    left join
        {{ source('prevencao-perdas', 'cosmos_v14b_dbo_categ_prd_ephdet') }}
            as det
        on pm.capd_cd_catephfil = det.capd_cd_catephfil
),

renamed as (
    select
        prme_cd_produto as produto_id,
        COALESCE(capc_cd_catephpai, 999) as shelf_life_category_id
    from shelf_life
),

final as (
    select
        produto_id,
        case when shelf_life_category_id in (4, 7) then 2 else 3 end
            as shelf_life_months
    from renamed
)

select * from final
