with estoque as (
    select
        dep.deposito_id,
        dep.produto_id,
        dep.valor_custo_sicms,
        dep.quantidade_estoque_atual
        - COALESCE(resp.quantidade_reservada, 0.0) as quantidade_estoque_atual
    from {{ ref('stg_estoque_deposito') }} as dep
    left join {{ ref('stg_estoque_reservado') }} as resp
        on
            dep.deposito_id = resp.deposito_id
            and dep.produto_id = resp.produto_id
),

final as (
    select
        deposito_id,
        produto_id,
        valor_custo_sicms,
        quantidade_estoque_atual
    from estoque
    where quantidade_estoque_atual > 0
)

select * from final
