WITH dim_comercial_prev AS (
    SELECT
        "codigo produto" AS cod_prod,
        "codigo fornecedor principal deposito" AS forn_cod_filho,
        "nome produto" AS nome_prod,
        "fornecedor comercial" AS forn_nm_comercial,
        "nomefabricante" AS forn_nm_pai,
        "nome fornecedor principal deposito" AS forn_nm_filho,
        "gerente compras" AS nome_gerente,
        "comprador" AS nome_comprador,
        "nome nível 1" AS categ_nivel_01,
        "nome nível 2" AS categ_nivel_02,
        "nome nível 3" AS categ_nivel_03,
        "nome nível 4" AS categ_nivel_04,
        "nome nível 5" AS categ_nivel_05,
        CAST(datacadastro AS TIMESTAMP(3)) as datacadastro
    FROM planejamento_comercial.dim_produtos
),

replace_dim_comercial AS (
    SELECT
        CAST(cod_prod AS int) AS cod_prod,
        CAST(
            COALESCE(TRY_CAST(forn_cod_filho AS double), 0) AS int
        ) AS forn_cod_filho,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(nome_prod) = 'nan', NULL, TRIM(nome_prod)), ' +', ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS nome_prod,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(
                    TRIM(forn_nm_comercial) = 'nan',
                    NULL,
                    TRIM(forn_nm_comercial)
                ),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS forn_nm_comercial,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(forn_nm_pai) = 'nan', NULL, TRIM(forn_nm_pai)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS forn_nm_pai,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(forn_nm_filho) = 'nan', NULL, TRIM(forn_nm_filho)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS forn_nm_filho,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(nome_gerente) = 'nan', NULL, TRIM(nome_gerente)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS nome_gerente,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(nome_comprador) = 'nan', NULL, TRIM(nome_comprador)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS nome_comprador,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(categ_nivel_01) = 'nan', NULL, TRIM(categ_nivel_01)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS categ_nivel_01,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(categ_nivel_02) = 'nan', NULL, TRIM(categ_nivel_02)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS categ_nivel_02,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(categ_nivel_03) = 'nan', NULL, TRIM(categ_nivel_03)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS categ_nivel_03,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(categ_nivel_04) = 'nan', NULL, TRIM(categ_nivel_04)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS categ_nivel_04,
        TRANSLATE(
            REGEXP_REPLACE(
                IF(TRIM(categ_nivel_05) = 'nan', NULL, TRIM(categ_nivel_05)),
                ' +',
                ' '
            ),
            'ãäöüẞáäčçďéěíĺľňóôŕšťúůýžÄÖÜẞÁÄČÇĎÉĚÍĹĽŇÓÔŔŠŤÚŮÝŽ',
            'aaousaaccdeeillnoorstuuyzAOUSAACCDEEILLNOORSTUUYZ'
        ) AS categ_nivel_05,
        CAST(datacadastro AS date) AS data_cadastro
    FROM dim_comercial_prev
),

pre_filiais_ativas AS (
    SELECT
        pf.codigo_deposito AS deposito,
        pf.codigo_produto AS cod_prod,
        COUNT(*) AS qtd_filiais_ativas
    FROM master_data.modelado_cadastro_produto_filial AS pf
    WHERE pf.situacao_produto_filial = 'Ativo'
    GROUP BY 1, 2
),

filiais_ativas AS (
    SELECT
        qtd_filiais_ativas,
        CAST(deposito AS int) AS deposito,
        CAST(cod_prod AS int) AS cod_prod
    FROM pre_filiais_ativas
),

pre_produto_deposito_dim AS (
    SELECT
        dep.depo_cd_deposito AS deposito,
        dep.prme_cd_produto AS cod_prod,
        dep.prdp_tp_clabcfat AS classe_fat,
        dep.prdp_tp_sclabcfat AS sigla_fat,
        dep.prdp_fl_situacao AS fl_situacao
    FROM "prevencao-perdas".cosmos_v14b_dbo_produto_deposito AS dep
    WHERE dep.depo_cd_deposito < 100
),

produto_deposito_dim AS (
    SELECT
        deposito,
        cod_prod,
        fl_situacao,
        CONCAT(classe_fat, sigla_fat) AS curva_fat
    FROM pre_produto_deposito_dim
),

ressarcimento AS (
    SELECT
        forn_cd_fornecedor AS forn_cod_filho,
        CASE
            WHEN
                data_inicio_vigencia_dga <= CURRENT_DATE
                AND data_fim_vigencia_dga >= CURRENT_DATE
                THEN 'ATIVA'
            ELSE 'DESATIVADA'
        END AS situacao_ressarcimento,
        COALESCE(percentual_dga, 0) AS percentual_ressarcimento
    FROM "prevencao-perdas".cosmos_v14b_dbo_fornecedor
),

base_a_vencer_cd AS (
    SELECT
        df.deposito_id AS deposito,
        df.produto_id AS cod_prod,
        df.numero_nota_fiscal AS nf,
        df.quantidade_fisica AS quantidade_entrada,
        df.quantidade_estoque_atual AS quantidade_estoque,
        df.quantidade_distribuida AS quantidade_entrada_restante,
        CAST(df.data_hora_atualizacao AS date) AS data_entrada,
        CAST(df.data_vencimento_lote AS date) AS data_vencimento,
        CAST(df.data_recolhimento AS date) AS data_recolhimento,
        df.quantidade_fisica * df.valor_custo_sicms AS valor_entrada,
        df.quantidade_estoque_atual * df.valor_custo_sicms AS valor_estoque,
        IF(df.deposito_id > 5, 'EF', 'PM') AS empresa,
        CAST(df.quantidade_venda_diaria * 30 AS int) AS venda_mensal_loja,
        COALESCE(
            CAST(
                df.quantidade_estoque_atual
                / NULLIF(df.quantidade_venda_diaria, 0) AS int
            ),
            3600
        ) AS pme_dias,
        CAST(df.saldo_restante AS int) AS saldo_recolhimento,
        CAST(df.saldo_restante AS int)
        * df.valor_custo_sicms AS valor_saldo_recolhimento
    FROM "prevencao-perdas".avencer_cd_dataframe AS df
    WHERE
        df.data_recolhimento BETWEEN {start} AND {end}
)

SELECT
    deposito,
    base.empresa,
    cod_prod,
    dim.nome_prod,
    dim.forn_nm_comercial,
    dim.forn_nm_pai,
    forn_cod_filho,
    dim.forn_nm_filho,
    dim.nome_gerente,
    dim.nome_comprador,
    base.nf,
    base.data_entrada,
    base.venda_mensal_loja,
    base.quantidade_entrada,
    base.quantidade_estoque,
    base.saldo_recolhimento as saldo,
    base.quantidade_entrada_restante,
    base.valor_entrada,
    base.valor_estoque,
    base.valor_saldo_recolhimento as valor_saldo,
    base.data_vencimento,
    base.data_recolhimento,
    base.pme_dias,
    pd.fl_situacao,
    rm.situacao_ressarcimento,
    rm.percentual_ressarcimento,
    pd.curva_fat,
    COALESCE(fa.qtd_filiais_ativas, 0) AS qtd_filiais_ativas,
    dim.categ_nivel_01,
    dim.categ_nivel_02,
    dim.categ_nivel_03,
    dim.categ_nivel_04,
    dim.categ_nivel_05,
    dim.data_cadastro
FROM base_a_vencer_cd AS base
INNER JOIN replace_dim_comercial AS dim USING(cod_prod)
INNER JOIN produto_deposito_dim AS pd USING (deposito, cod_prod)
LEFT JOIN filiais_ativas AS fa USING (deposito, cod_prod)
LEFT JOIN ressarcimento AS rm USING (forn_cod_filho)
ORDER BY deposito, cod_prod, data_vencimento
