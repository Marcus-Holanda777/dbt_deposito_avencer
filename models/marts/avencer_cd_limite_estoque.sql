{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
    )
}}

with limite_minimo as (
    select
        deposito_id,
        produto_id,
        MIN(id_grupo) as limite
    from {{ ref('avencer_cd_representa_estoque') }}
    where soma > 100.0
    group by deposito_id, produto_id
),

filtro_limite_minimo as (
    select
        sa.*,
        lm.limite
    from {{ ref('avencer_cd_representa_estoque') }} as sa
    inner join limite_minimo as lm
        on
            sa.deposito_id = lm.deposito_id
            and sa.produto_id = lm.produto_id
    where sa.id_grupo <= lm.limite
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
from filtro_limite_minimo
