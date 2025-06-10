{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
    )
}}

with representa_estoque as (
    select
        dep.deposito_id,
        dep.produto_id,
        ent.numero_nota_fiscal,
        dep.valor_custo_sicms,
        CAST(dep.quantidade_estoque_atual as INT) as quantidade_estoque_atual,
        CAST(ent.quantidade_fisica as INT) as quantidade_fisica,
        DATE_ADD(
            'millisecond', ROW_NUMBER() over (
                partition by dep.deposito_id, dep.produto_id
                order by
                    dep.deposito_id asc,
                    dep.produto_id asc,
                    ent.data_hora_atualizacao desc
            ),
            DATE_TRUNC('day', ent.data_hora_atualizacao)
        ) as data_hora_atualizacao,
        DATE_ADD(
            'millisecond', ROW_NUMBER() over (
                partition by dep.deposito_id, dep.produto_id
                order by
                    dep.deposito_id asc,
                    dep.produto_id asc,
                    ent.data_vencimento_lote desc
            ),
            DATE_TRUNC('day', ent.data_vencimento_lote)
        ) as data_vencimento_lote,
        CAST(
            ent.quantidade_fisica
            * 100.0
            / dep.quantidade_estoque_atual as DECIMAL(12, 2)
        ) as media_estoque
    from {{ ref('int_entrada_deposito') }} as ent
    inner join {{ ref('int_estoque_deposito') }} as dep
        on
            ent.deposito_id = dep.deposito_id
            and ent.produto_id = dep.produto_id
),

soma_acumulada as (
    select
        repr.*,
        ROW_NUMBER() over (
            partition by repr.deposito_id, repr.produto_id
            order by
                repr.deposito_id asc,
                repr.produto_id asc,
                repr.data_hora_atualizacao desc
        ) as id_grupo,
        SUM(repr.media_estoque) over (
            partition by repr.deposito_id, repr.produto_id
            order by
                repr.deposito_id asc,
                repr.produto_id asc,
                repr.data_hora_atualizacao desc
        ) as soma
    from representa_estoque as repr
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
    CAST(data_hora_atualizacao as TIMESTAMP (3)) as data_hora_atualizacao,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote
from soma_acumulada
