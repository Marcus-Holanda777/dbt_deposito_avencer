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
        "nome nível 5" AS categ_nivel_05
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
        ) AS categ_nivel_05
    FROM dim_comercial_prev
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

base_a_vencer_loja AS (
    SELECT
        filial,
        cod_prod,
        CAST(dt_vencimento AS timestamp(3)) AS dt_vencimento,
        CAST(recolher AS timestamp(3)) AS recolher,
        saldo,
        valor_saldo
    FROM "prevencao-perdas".cadastro_ultima_chance
)

SELECT
    filial,
    base.cod_prod,
    ARBITRARY(dim.nome_prod) AS nome_prod,
    ARBITRARY(dim.forn_nm_comercial) AS forn_nm_comercial,
    ARBITRARY(dim.nome_gerente) AS nome_gerente,
    ARBITRARY(dim.nome_comprador) AS nome_comprador,
    CAST(SUM(saldo) AS INT) AS saldo,
    SUM(valor_saldo) AS valor_saldo,
    recolher as data_recolhimento,
    dt_vencimento,
    ARBITRARY(situacao_ressarcimento) AS situacao_ressarcimento,
    ARBITRARY(percentual_ressarcimento) AS percentual_ressarcimento,
    ARBITRARY(dim.categ_nivel_01) AS categ_nivel_01,
    ARBITRARY(dim.categ_nivel_02) AS categ_nivel_02,
    ARBITRARY(dim.categ_nivel_03) AS categ_nivel_03,
    ARBITRARY(dim.categ_nivel_04) AS categ_nivel_04,
    ARBITRARY(dim.categ_nivel_05) AS categ_nivel_05
FROM base_a_vencer_loja AS base
INNER JOIN
    replace_dim_comercial AS dim
    ON base.cod_prod = dim.cod_prod
LEFT JOIN ressarcimento USING (forn_cod_filho)
GROUP BY 1, 2, 9, 10
