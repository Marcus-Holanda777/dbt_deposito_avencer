{{
    config(
        materialized='table',
        table_type='iceberg',
        format='parquet',
        write_compression='zstd',
    )
}}

with vendas_base as (
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
        COALESCE(ivd.quantidade_venda_diaria, 0.0) as quantidade_venda_diaria
    from {{ ref('avencer_cd_distribuicao') }} as acd
    left join {{ ref('int_venda_deposito') }} as ivd
        on
            acd.deposito_id = ivd.deposito_id
            and acd.produto_id = ivd.produto_id
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
    CAST(data_hora_atualizacao as TIMESTAMP (3)) as data_hora_atualizacao,
    CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote
from vendas_base
