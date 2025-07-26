import xlwings as xw
import pandas as pd
from typing import NamedTuple
from itertools import starmap, chain


class Estilo(NamedTuple):
    id: int
    name: str
    format: str


def format_columns_number(df: pd.DataFrame) -> list[Estilo]:
    """
    Formata as colunas de um DataFrame para exportação em Excel.
    Esta função identifica os tipos de dados das colunas do DataFrame e aplica
    formatação específica para números, percentuais e moeda, retornando uma lista
    de objetos Estilo que contêm o ID da coluna, o nome e o formato a ser aplicado.

    Args:
        df (pd.DataFrame): DataFrame cujas colunas serão formatadas.

    Returns:
        list[Estilo]: Lista de objetos Estilo com informações de formatação para cada coluna.

    """

    NUMBER = "#.##0"
    PERCENTIL = """_-* #.##0,00_-;-* #.##0,00_-;_-* "-"??_-;_-@_-"""
    CURRENCY = """_-R$ * #.##0,00_-;-R$ * #.##0,00_-;_-R$ * "-"??_-;_-@_-"""

    columns = df.columns.to_list()

    intergers = df.select_dtypes(include="integer").columns
    floatings = set(df.select_dtypes(include="floating").columns)

    perce = set(filter(lambda c: c.startswith("percentual"), floatings))
    floatings = floatings - perce

    def return_estilo(col: str, fmt: str) -> Estilo:
        id = columns.index(col)
        return Estilo(id=id, name=col, format=fmt)

    len_intergers = len(intergers)
    len_floatings = len(floatings)
    len_perce = len(perce)

    estilos = [
        *chain.from_iterable(
            [
                starmap(return_estilo, zip(intergers, [NUMBER] * len_intergers)),
                starmap(return_estilo, zip(floatings, [CURRENCY] * len_floatings)),
                starmap(return_estilo, zip(perce, [PERCENTIL] * len_perce)),
            ]
        )
    ]

    return estilos


def create_table_plan(
    df: pd.DataFrame, template: str, *, sheet_name: str, rng: str, output: str
) -> None:
    """
    Cria uma tabela no Excel a partir de um DataFrame, aplicando formatação específica
    para colunas numéricas. A função abre um modelo de Excel, escreve os dados do DataFrame
    em uma faixa específica, formata as colunas de acordo com os estilos definidos e salva
    o arquivo em um novo local.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem exportados.
        template (str): Caminho para o modelo de Excel a ser utilizado.
        sheet_name (str): Nome da planilha onde os dados serão escritos.
        rng (str): Faixa de células onde os dados do DataFrame serão inseridos.
        output (str): Caminho para o arquivo Excel de saída.
    Returns:
        None: A função salva o DataFrame formatado em um arquivo Excel.

    """
    with xw.App(visible=False, add_book=False) as app:
        with app.properties(
            display_alerts=False, enable_events=False, screen_updating=False
        ):
            book = app.books.open(template)
            sheet = book.sheets[sheet_name]

            sheet[rng].options(
                header=False,
                index=False,
                chunk_size=10_000,
            ).value = df

            estilos = format_columns_number(df)

            num_col = sheet.range(rng).column
            num_row = sheet.range(rng).row

            total_rows = sheet.cells.last_cell.row
            final_row = sheet.range((total_rows, num_col)).end("up").row

            for estilo in estilos:
                start = sheet.cells(num_row, estilo.id + num_col)
                end = sheet.cells(final_row, estilo.id + num_col)
                sheet.range(start, end).number_format = estilo.format

            book.api.RefreshAll()
            sheet.range("I4").value = f"Data Relatório: {pd.Timestamp.now():%d/%m/%Y}"

            book.save(output)
            book.close()
