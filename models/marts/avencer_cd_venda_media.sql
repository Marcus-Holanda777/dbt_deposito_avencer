{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
    )
}}

with venda_media as (
    select
        acd.deposito_id,
        acd.produto_id,
        acd.shelf_life_days,
        acd.valor_custo_sicms,
        acd.numero_nota_fiscal,
        acd.quantidade_estoque_atual,
        acd.quantidade_fisica,
        acd.media_estoque,
        acd.id_grupo,
        acd.soma,
        acd.soma_acumulada,
        acd.quantidade_distribuida,
        acd.data_hora_atualizacao,
        acd.data_vencimento_lote,
        acd.data_recolhimento,
        COALESCE(ivd.quantidade_venda_diaria, 0.0) as quantidade_venda_diaria
    from {{ ref('avencer_cd_distribuicao') }} as acd
    left join {{ ref('int_venda_deposito') }} as ivd
        on
            acd.deposito_id = ivd.deposito_id
            and acd.produto_id = ivd.produto_id
),

{# Retornar a diferenca, dias ate a data de recolhimento #}
add_dias_ate_recolhimento as (
    select
        *,
        DATE_DIFF(
            'day',
            CURRENT_DATE,
            CAST(data_recolhimento as DATE)
        ) as dias_ate_recolhimento
    from venda_media
),

calculo_pme as (
    select
        *,
        quantidade_distribuida / NULLIF(quantidade_venda_diaria, 0) as pme_dias
    from add_dias_ate_recolhimento
),

menor_dia as (
    select
        *,
        LEAST(pme_dias, dias_ate_recolhimento) as dias_consumo
    from calculo_pme
),

add_dias_consumo_acumulado as (
    select
        *,
        SUM(dias_consumo) over (
            partition by deposito_id, produto_id
            order by data_vencimento_lote asc
        ) as dias_consumo_acumulado
    from menor_dia
),

add_data_fim_estoque as (
    select
        *,
        DATE_ADD('day', CAST(dias_consumo_acumulado as INT), CURRENT_DATE)
            as data_fim_estoque
    from add_dias_consumo_acumulado
),

final as (
    select
        *,
        dias_consumo * quantidade_venda_diaria as quantidade_consumida,
        quantidade_distribuida
        - (dias_consumo * quantidade_venda_diaria) as saldo_restante
    from add_data_fim_estoque
)

select
    deposito_id,
    produto_id,
    shelf_life_days,
    valor_custo_sicms,
    numero_nota_fiscal,
    quantidade_estoque_atual,
    quantidade_fisica,
    media_estoque,
    id_grupo,
    soma,
    soma_acumulada,
    quantidade_distribuida,
    quantidade_venda_diaria,
    pme_dias,
    dias_ate_recolhimento,
    dias_consumo,
    dias_consumo_acumulado,
    quantidade_consumida,
    saldo_restante,
    CAST(data_hora_atualizacao as TIMESTAMP (3)) as data_hora_atualizacao,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote,
    CAST(data_recolhimento as TIMESTAMP (3)) as data_recolhimento,
    CAST(data_fim_estoque as TIMESTAMP (3)) as data_fim_estoque
from final
