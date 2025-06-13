import pandas as pd
from pandas.core.groupby import DataFrameGroupBy
from athena_mvsh import Athena, CursorParquetDuckdb
import numpy as np
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
import os


MAX_WORKERS = int(os.cpu_count() / 2)


def pipe_avencer(group: DataFrameGroupBy) -> pd.DataFrame:
    """
    Processa os dados de vencimento de lotes para calcular o consumo acumulado e as datas de início e fim do lote.
    Esta função recebe um grupo de dados agrupados por depósito e produto, e calcula:

    - Dias acumulados desde o início do lote.
    - Dias de consumo final para cada lote.
    - Data de início e fim do lote.
    - Quantidade consumida e saldo restante.

    O resultado é um DataFrame com as colunas adicionais calculadas.

    Args:
        group (DataFrameGroupBy): Grupo de dados agrupados por depósito e produto.

    Returns:
        pd.DataFrame: DataFrame com as colunas adicionais calculadas.

    Example:
        >>> df = download_avencer_athena()
        >>> grouped = groupby_avencer(df)
        >>> result = grouped.apply(pipe_avencer).reset_index(drop=True)
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

    Example:
        >>> df = download_avencer_athena()
        >>> grouped = groupby_avencer(df)
        >>> result = grouped.apply(pipe_avencer).reset_index(drop=True)
    """
    return (
        df.assign(
            data_recolhimento=lambda _: _.data_vencimento_lote
            - pd.to_timedelta(_.shelf_life_days, unit="days"),
            dias_ate_recolhimento=lambda _: (
                _.data_recolhimento - pd.to_datetime("today").normalize()
            ).dt.days,
            pme_dias=lambda _: (
                _.quantidade_distribuida / _.quantidade_venda_diaria
            ).replace(np.inf, 3600),
            dias_consumo=lambda _: _[["pme_dias", "dias_ate_recolhimento"]].min(axis=1),
        )
        .sort_values(["deposito_id", "produto_id", "data_vencimento_lote"])
        .groupby(["deposito_id", "produto_id"])
    )


def download_avencer_athena() -> pd.DataFrame:
    """
    Baixa os dados de vencimento de lotes do Athena e retorna um DataFrame.
    Esta função utiliza o cliente Athena para executar uma consulta SQL que recupera
    informações sobre os lotes de produtos, incluindo ID do depósito, ID do produto,
    vida útil, custo, quantidade em estoque, quantidade distribuída, venda diária e data de vencimento do lote.

    Args:
        None

    Returns:
        pd.DataFrame: DataFrame contendo os dados de vencimento de lotes.

    Example:
        >>> df = download_avencer_athena()
        >>> print(df.head())

    """
    cursor = CursorParquetDuckdb(
        "s3://out-of-lake-pmenos-query-results/", result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.execute(
            """
            select
                deposito_id,
                produto_id,
                shelf_life_days,
                valor_custo_sicms,
                quantidade_estoque_atual,
                quantidade_distribuida,
                quantidade_venda_diaria,
                CAST(data_vencimento_lote as TIMESTAMP (3)) as data_vencimento_lote
            from "prevencao-perdas".avencer_cd_venda_media
            """
        )

        return client.to_pandas()


def update_avencer_athena(
    df: pd.DataFrame, table_name: str, location: str, schema: str
) -> None:
    """
    Atualiza a tabela de vencimento de lotes no Athena com os dados processados.
    Esta função escreve um DataFrame no Athena, substituindo a tabela existente ou criando uma nova,
    dependendo do parâmetro `if_exists`. A tabela é escrita no local especificado no S3.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados processados de vencimento de lotes.
        table_name (str): Nome da tabela no Athena onde os dados serão escritos.
        location (str): Localização no S3 onde a tabela será armazenada.
        schema (str): Esquema da tabela no Athena.

    Returns:
        None

    Example:
        >>> df = download_avencer_athena()
        >>> update_avencer_athena(df, "avencer_cd_venda_media_processed", "s3://my-bucket/avencer/", "prevencao-perdas")
    """

    cursor = CursorParquetDuckdb(
        "s3://out-of-lake-pmenos-query-results/", result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.write_dataframe(
            df,
            table_name=table_name,
            location=location,
            schema=schema,
        )


if __name__ == "__main__":
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
        group_avencer = groupby_avencer(download_avencer_athena())
        futuros = [executor.submit(pipe_avencer, group) for group in group_avencer]

        dfs = []
        for future in tqdm(as_completed(futuros), total=len(group_avencer)):
            dfs.append(future.result())

    # NOTE: Envia os dados pro ATHENA
    update_avencer_athena(
        pd.concat(dfs, ignore_index=True),
        table_name=(table_name := "avencer_cd_dataframe"),
        location=f"s3://out-of-lake-prevencao-perdas/tables/{table_name}/",
        schema="prevencao-perdas",
    )
