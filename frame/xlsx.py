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
    df: pd.DataFrame,
    template: str,
    *,
    sheet_name: str,
    rng: str,
    cell_title: str,
    output: str,
) -> None:
    """
    Cria uma tabela em um arquivo Excel a partir de um DataFrame, aplicando formatação
    específica para colunas numéricas. A função abre um template Excel, insere os dados
    do DataFrame em um intervalo específico, aplica formatação às colunas numéricas e
    salva o arquivo Excel resultante.

    Args:
        df (pd.DataFrame): DataFrame contendo os dados a serem inseridos na tabela.
        template (str): Caminho para o template Excel a ser utilizado.
        sheet_name (str): Nome da planilha onde os dados serão inseridos.
        rng (str): Intervalo de células onde os dados serão colocados.
        cell_title (str): Célula onde o título será colocado.
        output (str): Caminho do arquivo Excel de saída.

    Returns:
        None: Esta função não retorna nada, mas cria um arquivo Excel com os dados formatados.
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
            sheet.range(
                cell_title
            ).value = f"Data Relatório: {pd.Timestamp.now():%d/%m/%Y}"

            book.save(output)
            book.close()


def create_table_plan_full(
    dfs: dict[str, pd.DataFrame],
    template: str,
    *,
    rng: str,
    rng_values: str,
    cell_title: str,
    output: str,
) -> None:
    """
    Cria múltiplas tabelas em um arquivo Excel a partir de um dicionário de DataFrames,
    aplicando formatação específica para colunas numéricas. A função abre um template
    Excel, insere os dados de cada DataFrame em intervalos específicos de suas respectivas
    planilhas, aplica formatação às colunas numéricas e salva o arquivo Excel resultante.

    Args:
        dfs (dict[str, pd.DataFrame]): Dicionário onde as chaves são os nomes das planilhas
            e os valores são os DataFrames contendo os dados a serem inseridos.
        template (str): Caminho para o template Excel a ser utilizado.
        rng (str): Intervalo de células onde os dados principais serão colocados.
        rng_values (str): Intervalo de células onde os valores serão colocados.
        cell_title (str): Célula onde o título será colocado.
        output (str): Caminho do arquivo Excel de saída.
        
    Returns:
        None: Esta função não retorna nada, mas cria um arquivo Excel com os dados formatados.
    """

    def write_sheet_df(sheet: xw.Sheet, rng: str, df: pd.DataFrame) -> None:
        sheet[rng].options(
            header=False,
            index=False,
            chunk_size=10_000,
        ).value = df

    with xw.App(visible=False, add_book=False) as app:
        with app.properties(
            display_alerts=False, enable_events=False, screen_updating=False
        ):
            book = app.books.open(template)

            for sheet_name, df in dfs.items():
                print(f"Processando planilha: {sheet_name} ...")
                df_transform = (
                    df.loc[lambda _df: _df["saldo"] > 0, :]
                    .groupby(
                        [
                            "cod_prod",
                            "nome_prod",
                            "nome_comprador",
                            "nome_gerente",
                            "forn_nm_comercial",
                            "data_recolhimento",
                            "categ_nivel_02",
                            "categ_nivel_03",
                            "categ_nivel_04",
                            "categ_nivel_05",
                        ],
                        dropna=False,
                    )
                    .agg({"saldo": "sum", "valor_saldo": "sum"})
                    .reset_index()
                )

                sheet = book.sheets[sheet_name]
                write_sheet_df(sheet, rng, df_transform.iloc[:, :-2])
                write_sheet_df(sheet, rng_values, df_transform.iloc[:, -2:])

                sheet.range(
                    cell_title
                ).value = f"Atualizado: {pd.Timestamp.now():%d/%m/%Y}"

            book.save(output)
            book.close()

            print(f"Arquivo salvo em: {output}")
