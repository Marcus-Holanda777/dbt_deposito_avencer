{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
    )
}}

with lotes_ordenados as (
    select
        vc.*,
        SUM(vc.quantidade_fisica) over (
            partition by vc.deposito_id, vc.produto_id
            order by vc.data_vencimento_lote desc
        ) as soma_acumulada
    from {{ ref('avencer_cd_limite_estoque') }} as vc
),

distribuicao_lotes as (
    select
        l.*,
        case
            when
                l.soma_acumulada <= l.quantidade_estoque_atual
                then l.quantidade_fisica
            when
                l.soma_acumulada - l.quantidade_fisica
                >= l.quantidade_estoque_atual
                then 0
            else
                l.quantidade_estoque_atual
                - (l.soma_acumulada - l.quantidade_fisica)
        end as quantidade_distribuida
    from lotes_ordenados as l
),

filtro_distribuicao as (
    select *
    from distribuicao_lotes
    where quantidade_distribuida > 0
)

select
    deposito_id,
    produto_id,
    valor_custo_sicms,
    numero_nota_fiscal,
    quantidade_estoque_atual,
    quantidade_fisica,
    media_estoque,
    id_grupo,
    soma,
    soma_acumulada,
    quantidade_distribuida,
    CAST(data_hora_atualizacao as TIMESTAMP (3)) as data_hora_atualizacao,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote
from filtro_distribuicao
