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

add_dias_consumo_anterior as (
    select
        *,
        SUM(dias_consumo) over (
            partition by deposito_id, produto_id
            order by data_vencimento_lote asc
            rows between unbounded preceding and 1 preceding
        ) as dias_consumo_anterior
    from menor_dia
),

-- Etapa 4: Calcula quanto o lote pode consumir agora, baseado nos dias restantes
consumo_ajustado as (
    select
        *,
        COALESCE(dias_consumo_anterior, 0) as dias_acumulados,

        -- Dias restantes permitidos, descontando os dias já consumidos
        GREATEST(
            dias_ate_recolhimento - COALESCE(dias_consumo_anterior, 0),
            0
        ) as dias_restantes_disponiveis,

        -- Dias finais de consumo possíveis neste lote
        LEAST(
            pme_dias,
            GREATEST(
                dias_ate_recolhimento - COALESCE(dias_consumo_anterior, 0),
                0
            )
        ) as dias_consumo_final
    from add_dias_consumo_anterior
),

-- Etapa 5: Monta o resultado final com datas de início, fim e saldo
distribuicao_final as (
    select
        *,

        -- Quando o lote começa a ser usado
        DATE_ADD('day', CAST(dias_acumulados as INT), CURRENT_DATE)
            as data_inicio_lote,

        -- Quando o lote termina (ou para de ser usado)
        DATE_ADD(
            'day',
            CAST(dias_acumulados + dias_consumo_final as INT),
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
    dias_acumulados,
    dias_restantes_disponiveis,
    dias_consumo_final,
    quantidade_consumida,
    saldo_restante,
    CAST(data_hora_atualizacao as TIMESTAMP (3)) as data_hora_atualizacao,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote,
    CAST(data_recolhimento as TIMESTAMP (3)) as data_recolhimento,
    CAST(data_inicio_lote as TIMESTAMP (3)) as data_inicio_lote,
    CAST(data_fim_lote as TIMESTAMP (3)) as data_fim_lote
from distribuicao_final
