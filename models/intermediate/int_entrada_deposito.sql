with entradas as (
    select
        et.deposito_id,
        et.produto_id,
        et.numero_nota_fiscal,
        et.data_hora_atualizacao,
        et.quantidade_fisica,
        lt.data_vencimento_lote,
        lt.quantidade_lote,
        pb.dias_validade
    from {{ ref('stg_entrada_deposito') }} as et
    left join {{ ref('stg_base_lotes') }} as lt
        on
            et.nota_fiscal_id = lt.nota_fiscal_id
            and et.detalhe_nota_fiscal_id = lt.detalhe_nota_fiscal_id
    left join {{ ref('stg_produto_baixo_giro') }} as pb
        on et.produto_id = pb.produto_id
),

define_vencimento as (
    select
        deposito_id,
        produto_id,
        numero_nota_fiscal,
        data_hora_atualizacao,
        quantidade_lote,
        quantidade_fisica,
        case
            when
                data_vencimento_lote is not null
                and data_vencimento_lote > data_hora_atualizacao
                then data_vencimento_lote
            when dias_validade is not null
                then DATE_ADD('day', dias_validade, data_hora_atualizacao)
            else DATE_ADD('month', 24, data_hora_atualizacao)
        end as data_vencimento_lote
    from entradas
),

ajuste_quantidade_entrada_vencimento as (
    select
        deposito_id,
        produto_id,
        numero_nota_fiscal,
        data_hora_atualizacao,
        CAST(LAST_DAY_OF_MONTH(data_vencimento_lote) as TIMESTAMP)
            as data_vencimento_lote,
        case
            when
                quantidade_lote is null or quantidade_lote <= 0
                then quantidade_fisica
            when quantidade_lote > quantidade_fisica then quantidade_fisica
            else quantidade_lote
        end as quantidade_fisica
    from define_vencimento
)

select * from ajuste_quantidade_entrada_vencimento
