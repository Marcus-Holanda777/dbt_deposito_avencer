from athena_mvsh import Athena, CursorParquetDuckdb
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import dotenv_values
from xlsx import create_table_plan


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


def export_view_excel(
    file: str, template: str, *, sheet_name: str, rng: str, cell_title: str
) -> None:
    """
    Exporta uma consulta SQL para um arquivo Excel, aplicando um template específico.

    Args:
        file (str): Nome do arquivo SQL a ser executado.
        template (str): Caminho para o template Excel a ser utilizado.
        sheet_name (str): Nome da planilha no Excel onde os dados serão inseridos.
        rng (str): Intervalo de células onde os dados serão colocados.
        cell_title (str): Célula onde o título será colocado.

    Returns:
        None: Esta função não retorna nada, mas cria um arquivo Excel com os dados consultados.
    """

    name, __ = file.split(".")
    stmt = read_stmt(file)
    params = {}

    file_to = f"frame/output/{name}_{datetime.now():%d%m%Y_%H%M%S}.xlsx"
    msg = f"Exportando dados [{name}] ... {file_to}"

    if name == "banco_cd":
        start = (
            (datetime.now() + relativedelta(months=1))
            .replace(day=1)
            .replace(hour=0, minute=0, second=0, microsecond=0)
        )
        end = (start + relativedelta(years=1)).replace(
            day=31, month=12, hour=23, minute=59, second=59, microsecond=999999
        )

        params |= {"start": start, "end": end}
    
    print(f"{msg} ...", *params.values())

    cursor = CursorParquetDuckdb(
        CONFIG.get("s3_stanging_dir"), result_reuse_enable=True
    )

    with Athena(cursor) as client:
        client.execute(stmt, parameters=params)
        df = client.to_pandas()

    create_table_plan(
        df,
        template,
        sheet_name=sheet_name,
        rng=rng,
        cell_title=cell_title,
        output=file_to,
    )


if __name__ == "__main__":
    export_view_excel(
        "banco_cd.sql",
        "frame/temp/template_cd.xlsx",
        sheet_name="Banco de Dados",
        rng="E6",
        cell_title="I4",
    )

    export_view_excel(
        "banco_loja.sql",
        "frame/temp/template_loja.xlsx",
        sheet_name="Banco de Dados",
        rng="A6",
        cell_title="D4",
    )
