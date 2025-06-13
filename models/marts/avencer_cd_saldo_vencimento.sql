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
        deposito_id,
        produto_id,
        shelf_life_days,
        valor_custo_sicms,
        quantidade_estoque_atual,
        quantidade_distribuida,
        data_vencimento_lote,
        quantidade_venda_diaria
    from {{ ref('avencer_cd_venda_media') }}
),

grupo_validade_produto_deposito as (
    select
        deposito_id,
        produto_id,
        shelf_life_days,
        valor_custo_sicms,
        quantidade_estoque_atual,
        quantidade_venda_diaria,
        DATE_TRUNC('day', data_vencimento_lote) as data_vencimento_lote,
        SUM(quantidade_distribuida) as quantidade_distribuida
    from venda_media
    group by 1, 2, 3, 4, 5, 6, 7
),

add_data_recolhimento as (
    select
        *,
        DATE_ADD(
            'day',
            -shelf_life_days,
            data_vencimento_lote
        ) as data_recolhimento
    from grupo_validade_produto_deposito
),

{# Retornar a diferenca, dias ate a data de recolhimento #}
add_dias_ate_recolhimento as (
    select
        *,
        DATE_DIFF(
            'day',
            CURRENT_DATE,
            data_recolhimento
        ) as dias_ate_recolhimento
    from add_data_recolhimento
),

calculo_pme as (
    select
        *,
        quantidade_distribuida / NULLIF(quantidade_venda_diaria, 0) as pme_dias
    from add_dias_ate_recolhimento
),

add_dias_consumo as (
    select
        *,
        LEAST(pme_dias, dias_ate_recolhimento) as dias_consumo
    from calculo_pme
),

add_dias_consumo_acumulado_anterior as (
    select
        *,
        COALESCE(SUM(dias_consumo) over (
            partition by deposito_id, produto_id
            order by data_vencimento_lote asc
            rows between unbounded preceding and 1 preceding
        ), 0) as dias_consumo_acumulado_anterior
    from add_dias_consumo
),

add_dias_restantes_disponivel as (
    select
        *,
        GREATEST(
            dias_ate_recolhimento - dias_consumo_acumulado_anterior, 0
        )
            as dias_restantes_disponiveis
    from add_dias_consumo_acumulado_anterior
),

{# Calcula quanto o lote pode consumir, baseado nos dias restantes #}
consumo_ajustado as (
    select
        *,
        LEAST(
            pme_dias,
            dias_restantes_disponiveis
        ) as dias_consumo_final
    from add_dias_restantes_disponivel
),

distribuicao_final as (
    select
        *,
        -- Quando o lote começa a ser usado
        DATE_ADD(
            'day', CAST(dias_consumo_acumulado_anterior as INT), CURRENT_DATE
        )
            as data_inicio_lote,

        -- Quando o lote termina (ou para de ser usado)
        DATE_ADD(
            'day',
            CAST(dias_consumo_acumulado_anterior + dias_consumo_final as INT),
            CURRENT_DATE
        )
            as data_fim_lote,

        -- Quantidade consumida nesse período
        dias_consumo_final * quantidade_venda_diaria as quantidade_consumida,

        -- O que sobrou no lote (por falta de tempo ou demanda)
        quantidade_distribuida
        - (dias_consumo_final * quantidade_venda_diaria) as saldo_restante
    from consumo_ajustado
)

select
    deposito_id,
    produto_id,
    shelf_life_days,
    valor_custo_sicms,
    quantidade_estoque_atual,
    quantidade_distribuida,
    quantidade_venda_diaria,
    pme_dias,
    dias_ate_recolhimento,
    dias_consumo,
    dias_consumo_acumulado_anterior,
    dias_restantes_disponiveis,
    dias_consumo_final,
    quantidade_consumida,
    saldo_restante,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote,
    CAST(data_recolhimento as TIMESTAMP (3)) as data_recolhimento,
    CAST(data_inicio_lote as TIMESTAMP (3)) as data_inicio_lote,
    CAST(data_fim_lote as TIMESTAMP (3)) as data_fim_lote
from distribuicao_final
