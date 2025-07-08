import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
from athena_mvsh import Athena, CursorParquetDuckdb
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
from dotenv import dotenv_values

CONFIG = {**dotenv_values()}
MAX_WORKERS = int(os.cpu_count() / 2)


def pipe_avencer(group: DataFrameGroupBy) -> pd.DataFrame:
    """
    Simula o consumo sequencial de lotes de produtos distribuídos, respeitando a data-limite de recolhimento
    e a estimativa de dias de consumo (PME), a fim de calcular a janela de consumo de cada lote.

    Esta função é aplicada a um grupo de dados (agrupados por depósito e produto), e calcula, para cada lote:

    - Quantos dias já foram utilizados antes de consumir o lote atual (`dias_acumulados`);
    - Quantos dias o lote pode ser consumido efetivamente, respeitando:
        - o limite de consumo (`pme_dias`) e
        - o prazo máximo permitido (`dias_ate_recolhimento`);
    - A data de início e fim do consumo do lote, a partir da data atual;
    - A quantidade efetivamente consumida do lote e o saldo restante.

    ### Lógica detalhada:
        - Para cada linha (lote), simula-se o consumo a partir de hoje, somando os dias de consumo anteriores (`acumulado`).
        - O número de dias disponíveis para consumir o lote é calculado como:
              dias_restantes = max(dias_ate_recolhimento - acumulado, 0)
        - O número de dias que será realmente usado para consumo do lote é:
              consumo_final = min(pme_dias, dias_restantes)
        - O consumo termina se o total de dias acumulados alcançar ou ultrapassar `dias_ate_recolhimento`.

    ### Fórmulas calculadas:
        - `dias_acumulados`: dias somados antes do início de consumo deste lote.
        - `dias_consumo_final`: dias que o lote será consumido efetivamente.
        - `data_inicio_lote`: hoje + dias_acumulados.
        - `data_fim_lote`: data_inicio_lote + dias_consumo_final.
        - `quantidade_consumida`: dias_consumo_final * quantidade_venda_diaria.
        - `saldo_restante`: quantidade_distribuida - quantidade_consumida.

    Args:
        group (DataFrameGroupBy): Grupo de dados agrupado por depósito e produto.

    Returns:
        pd.DataFrame: DataFrame com colunas adicionais representando o consumo simulado por lote.
    """

    df: pd.DataFrame
    __, df = group

    acumulado = 0
    dias_acumulados = []
    dias_consumo_final = []

    hoje = pd.to_datetime("today").normalize()

    for __, row in df.iterrows():
        dias_restantes = max(row["dias_ate_recolhimento"] - acumulado, 0)
        consumo_final = min(row["pme_dias"], dias_restantes)

        dias_consumo_final.append(consumo_final)
        dias_acumulados.append(acumulado)
        acumulado += consumo_final

    return df.assign(
        dias_acumulados=dias_acumulados,
        dias_consumo_final=dias_consumo_final,
        data_inicio_lote=lambda _: hoje
        + pd.to_timedelta(_.dias_acumulados, unit="days"),
        data_fim_lote=lambda _: hoje
        + pd.to_timedelta(_.dias_acumulados + _.dias_consumo_final, unit="days"),
        quantidade_consumida=lambda _: _.dias_consumo_final * _.quantidade_venda_diaria,
        saldo_restante=lambda _: _.quantidade_distribuida - _.quantidade_consumida,
    )


def groupby_avencer(df: pd.DataFrame) -> DataFrameGroupBy:
    """
    Agrupa o DataFrame de vencimento de lotes por depósito e produto, e calcula
    as colunas necessárias para o processamento posterior.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados de vencimento de lotes.

    Returns:
        DataFrameGroupBy[tuple]: Objeto de agrupamento por depósito e produto.
    """

    return (
        df.assign(
            date_off_set=lambda _: _.shelf_life_months.map(
                lambda m: pd.DateOffset(months=m)
            ),
            data_recolhimento=lambda _: _.apply(
                lambda _: _.data_vencimento_lote - _.date_off_set, axis=1
            ),
            dias_ate_recolhimento=lambda _: (
                _.data_recolhimento - pd.to_datetime("today").normalize()
            ).dt.days,
            pme_dias=lambda _: (
                _.quantidade_distribuida / _.quantidade_venda_diaria
            ).replace(np.inf, 3600),
            dias_consumo=lambda _: _[["pme_dias", "dias_ate_recolhimento"]].min(axis=1),
        )
        .drop(["date_off_set"], axis=1)
        .sort_values(["deposito_id", "produto_id", "data_vencimento_lote"])
        .groupby(["deposito_id", "produto_id"])
    )


def download_avencer_athena() -> pd.DataFrame:
    """
    Baixa os dados de vencimento de lotes do Athena e retorna um DataFrame.
    Esta função utiliza o cliente Athena para executar uma consulta SQL que recupera
    informações sobre os lotes de produtos, incluindo ID do depósito, ID do produto, ID do grupo,
    numero da nota fiscal, data da entrada do lote, vida útil, custo, quantidade da entrada,
    quantidade em estoque, quantidade distribuída, venda diária e data de vencimento do lote.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame contendo os dados de vencimento de lotes.
    """

    cursor = CursorParquetDuckdb(
        CONFIG.get("s3_stanging_dir"), result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.execute(
            f"""
            select
                deposito_id,
                produto_id,
                id_grupo,
                numero_nota_fiscal,
                shelf_life_months,
                valor_custo_sicms,
                quantidade_estoque_atual,
                quantidade_fisica,
                quantidade_distribuida,
                quantidade_venda_diaria,
                CAST(data_hora_atualizacao as TIMESTAMP(3)) as data_hora_atualizacao,
                CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote
            from "{CONFIG.get("schema")}".{CONFIG.get("table_name_ref")}
            """
        )

        return client.to_pandas()


def update_avencer_athena(
    df: pd.DataFrame, table_name: str, location: str, schema: str
) -> None:
    """
    Atualiza a tabela de vencimento de lotes no Athena com os dados processados.
    Esta função escreve um DataFrame no Athena, substituindo a tabela existente ou criando uma nova.
    A tabela é escrita no local especificado no S3.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados processados de vencimento de lotes.
        table_name (str): Nome da tabela no Athena onde os dados serão escritos.
        location (str): Localização no S3 onde a tabela será armazenada.
        schema (str): Esquema da tabela no Athena.

    Returns:
        None
    """

    cursor = CursorParquetDuckdb(
        CONFIG.get("s3_stanging_dir"), result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.write_table_iceberg(
            df,
            table_name=table_name,
            location=location,
            schema=schema,
        )


if __name__ == "__main__":
    df_original = download_avencer_athena()
    print(f"Totais origem: {df_original.shape} ...")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        group_avencer = groupby_avencer(df_original)
        futuros = [executor.submit(pipe_avencer, group) for group in group_avencer]

        dfs = []
        for future in tqdm(as_completed(futuros), total=len(group_avencer)):
            dfs.append(future.result())

    # NOTE: Envia os dados pro ATHENA
    df_to = pd.concat(dfs, ignore_index=True).assign(
        data_hora_cadastro=pd.to_datetime("today"),
    )
    print(f"Totais destino: {df_to.shape}")

    update_avencer_athena(
        df_to,
        table_name=(table_name := CONFIG.get("table_name")),
        location=f"{CONFIG.get('location')}{table_name}/",
        schema=CONFIG.get("schema"),
    )
