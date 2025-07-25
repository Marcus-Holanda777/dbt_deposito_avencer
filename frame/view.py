from athena_mvsh import Athena, CursorParquetDuckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import dotenv_values

CONFIG = {**dotenv_values()}


def read_stmt(file: str) -> str:
    """
    Lê um arquivo SQL e retorna seu conteúdo como uma string.

    Args:
        file (str): Caminho para o arquivo SQL a ser lido.

    Returns:
        str: Conteúdo do arquivo SQL.
    """
    with open(f"frame/query/{file}", "r", encoding="utf_8") as f:
        return f.read()


def export_view_excel(file: str) -> None:
    """
    Exporta os dados de uma view do Athena para um arquivo Excel.

    Esta função executa uma consulta SQL definida em um arquivo, obtém os resultados
    e os salva em um arquivo Excel com um nome baseado na data e hora atual.
    Se o arquivo SQL for "banco_cd.sql", a função ajusta os parâmetros de data para
    exportar dados específicos do banco de CD, considerando o início e o fim do período
    de um ano a partir do primeiro dia do próximo mês.

    Args:
        file (str): Nome do arquivo SQL que contém a consulta para exportação.
    Returns:
        None: A função salva o DataFrame resultante em um arquivo Excel.
    """

    name, __ = file.split(".")
    stmt = read_stmt(file)
    params = None

    file_to = f"frame/output/{name}_{datetime.now():%d%m%Y_%H%M%S}.xlsx"

    if name == "banco_cd":
        start = (
            (datetime.now() + relativedelta(months=1))
            .replace(day=1)
            .replace(hour=0, minute=0, second=0, microsecond=0)
        )
        end = (start + relativedelta(years=1)).replace(
            day=31, month=12, hour=23, minute=59, second=59, microsecond=999999
        )

        params = {"start": start, "end": end}
        print(f"Exportando dados do banco de CD... {start} - {end}")

    cursor = CursorParquetDuckdb(
        CONFIG.get("s3_stanging_dir"), result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.execute(stmt, parameters=params)
        df = client.to_pandas()

    df.to_excel(file_to, index=False)


if __name__ == '__main__':
    export_view_excel("banco_cd.sql")