with filial_deposito as (
    select
        fd.fili_cd_filial,
        fd.codigo_deposito_principal
    from {{ source('prevencao-perdas', 'cosmos_v14b_dbo_filial') }} as fd
),

renamed as (
    select
        fili_cd_filial as filial_id,
        COALESCE(codigo_deposito_principal, 1) as deposito_id
    from filial_deposito
)

select * from renamed
