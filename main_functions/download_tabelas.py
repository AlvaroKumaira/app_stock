import pandas as pd
from openpyxl.styles import numbers
import numpy as np
import logging
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import pedidos, faturamento, saldo_analitico

# Get a logger
logger = logging.getLogger(__name__)


def download_saldo(filial, open_flag):
    """
    Downloads and processes data for the saldo_analitico query, and saves the result to an Excel file.

    The function downloads data using the saldo_analitico SQL query for a specific filial (branch), 
    then processes the data to prepare it for output to an Excel file. This includes renaming columns, 
    replacing blank strings with NaN, converting string dates to datetime objects, and applying specific 
    number formatting in the Excel file.

    Parameters:
    filial (str): The filial (branch) code to be used as a parameter in the saldo_analitico SQL query.

    Returns:
    None: The result is saved to an Excel file, and the file is opened for viewing.
    """
    # Log the start of the download process
    logger.info("Starting the download process for saldo_analitico.")

    # Try to download the data, catch any exceptions
    try:
        # Use the download function to execute the SQL query and store the result in a DataFrame
        params = (filial, filial)
        data_frame = download(saldo_analitico, params)
        logger.info(f"Downloaded {data_frame.shape[0]} rows of data for saldo_analitico.")
    except Exception as e:
        logger.error(f"An error occurred during download: {str(e)}")
        return

    # Rename columns
    column_mapping = {
        "B1_ZGRUPO": "Agrupamento",
        "B1_COD": "Código",
        "B1_TIPO": "Tipo",
        "B1_GRUPO": "Grupo",
        "B1_DESC": "Descrição",
        "BZ_LOCALI2": "Endereço",
        "B1_UM": "UM",
        "B2_FILIAL": "Filial",
        "B2_LOCAL": "Armazém",
        "B2_QATU_COPY": "Saldo em Estoque",
        "B2_QATU": "Estoque disponível",
        "B2_CM1": "Custo unitário",
        "B2_VATU1": "Valor em estoque",
    }
    data_frame.rename(columns=column_mapping, inplace=True)
    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Remove spaces from certain columns
    for column in ["Código", "Descrição"]:
        data_frame[column] = data_frame[column].str.strip()

    try:
        # Save DataFrame to Excel and open the file
        if open_flag:
            save_to_excel(data_frame, 'saldo_analítico', filial, open_file=True)
        else:
            save_to_excel(data_frame, 'saldo_analítico', filial, open_file=False)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return


def download_pedidos(filial, date, open_flag):
    """
    Downloads and processes data for the pedidos query, and saves the result to an Excel file.

    The function downloads data using the pedidos SQL query for a specific filial (branch), 
    then processes the data to prepare it for output to an Excel file. This includes renaming columns, 
    replacing blank strings with NaN, and applying specific number formatting in the Excel file.

    Parameters:
    filial (str): The filial (branch) code to be used as a parameter in the pedidos SQL query.

    Returns:
    None: The result is saved to an Excel file, and the file is opened for viewing.
    """

    # Log the start of the download process
    logger.info("Starting the download process for pedidos.")

    # Convert the date to string for the Query
    date_str = date.strftime("%Y%m%d")

    # Try to download the data, catch any exceptions
    try:
        # Use the download function to execute the SQL query and store the result in a DataFrame
        params = (date_str, filial)
        data_frame = download(pedidos, params)
        logger.info(f"Downloaded {data_frame.shape[0]} rows of data for pedidos.")
    except Exception as e:
        logger.error(f"An error occurred during download: {str(e)}")
        return
    # Rename columns
    column_mapping = {
        "B1_ZGRUPO": "Agrupamento",
        "C7_NUM": "Num.PC",
        "C7_FORNECE": "Fornecedor",
        "A2_LOJA": "Loja",
        "A2_NOME": "Razão Social",
        "A2_TEL": "Telefone",
        "C7_ITEM": "Item",
        "C7_NUMSC": "Numero da SC",
        "C7_PRODUTO": "Produto",
        "C7_DESCRI": "Descrição",
        "B1_GRUPO": "Grupo",
        "EMI": "Emissão",
        "ENT": "Entrega",
        "C7_QUANT": "Quantidade",
        "C7_UM": "UM",
        "C7_PRECO": "Prc Unitario",
        "DE": "Vl.Desconto",
        "C7_VALIP": "Vlr.IPI",
        "C7_TOTAL": "Vlr.Total",
        "C7_QUJE": "Qtd.Entregue",
        "QRE": "Quant.Receber",
        "SRE": "Saldo Receber",
        "C7_RESIDUO": "Res.Elim."
    }
    data_frame.rename(columns=column_mapping, inplace=True)

    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Remove trailing spaces from certain columns
    for column in ["Razão Social", "Telefone", "Produto", "Descrição", "Grupo"]:
        data_frame[column] = data_frame[column].str.strip()

    # Convert specified columns to date
    columns_to_date = ['Emissão', 'Entrega']
    for column in columns_to_date:
        data_frame[column] = pd.to_datetime(data_frame[column], format='%Y%m%d').dt.date

    try:
        # Save DataFrame to Excel and open the file
        if open_flag:
            save_to_excel(data_frame, 'pedidos', filial, open_file=True)
        else:
            save_to_excel(data_frame, 'pedidos', filial, open_file=False)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return


def download_faturamento(filial, date, open_flag):
    """
    Downloads and processes data for the faturamento query, and saves the result to an Excel file.

    The function downloads data using the faturamento SQL query for a specific filial (branch), then processes
    the data to prepare it for output to an Excel file. This includes renaming columns, replacing blank strings
    with NaN, aggregating values, and calculating the 'valor unitario'.

    Parameters:
    filial (str): The filial (branch) code to be used as a parameter in the faturamento SQL query.

    Returns:
    None: The result is saved to an Excel file, and the file may be opened for viewing if specified.
    """

    # Log the start of the download process
    logger.info("Starting the download process for faturamento.")

    # Convert the date to string for the Query
    date_fat = date.strftime("%Y%m%d")

    # Try to download the data, catch any exceptions
    try:
        # Use the download function to execute the SQL query and store the result in a DataFrame
        params = (date_fat, filial)
        data_frame = download(faturamento, params)
        logger.info(f"Downloaded {data_frame.shape[0]} rows of data for faturamento.")
    except Exception as e:
        logger.error(f"An error occurred during download: {str(e)}")
        return

    column_mapping = {
        "D2_EMISSAO": "Data",
        "B1_ZGRUPO": "Agrupamento",
        "D2_COD": "Código",
        "B1_DESC": "Descrição",
        "D2_UM": "UM",
        "D2_TP": "TP",
        "D2_CLIENTE": "Cod. Cliente",
        "A1_NOME": "Cliente",
        "F4_TEXTO": "Movimentação",
        "D2_QUANT": "Quantidade",
        "VFB": "Val Faturado Bruto",
        "D2_MARGEM": "Margem"
    }
    # Rename columns
    data_frame.rename(columns=column_mapping, inplace=True)

    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Remove trailing spaces from certain columns
    for column in ["Código", "Descrição"]:
        data_frame[column] = data_frame[column].str.strip()

    # Convert specified columns to numeric data
    columns_to_sum = ["Quantidade", "Val Faturado Bruto", "Margem"]
    data_frame[columns_to_sum] = data_frame[columns_to_sum].apply(pd.to_numeric, errors='coerce')

    # Convert specified columns to datetime
    data_frame['Data'] = pd.to_datetime(data_frame['Data'], format='%Y%m%d').dt.date

    # Calculate 'valor unitario'
    data_frame['Valor unitário'] = (data_frame['Val Faturado Bruto'] / data_frame['Quantidade']).round(2)

    try:
        # Save DataFrame to Excel and optionally open the file
        if open_flag:
            save_to_excel(data_frame, 'faturamento', filial, open_file=True)
        else:
            save_to_excel(data_frame, 'faturamento', filial, open_file=False)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return


# noinspection PyShadowingNames
def download_tabelas(filial, saldo, pedidos, faturamento, pedidos_selected_date, faturamento_selected_date):
    """
    Downloads and processes data for specified queries and saves the results to Excel files.

    The function orchestrates the downloading process for multiple queries based on the parameters provided.
    It calls specific download functions for saldo_analitico, pedidos, and faturamento based on the flags set
    by the user.

    Parameters:
    filial (str): The filial (branch) code to be used as a parameter in the respective SQL queries.
    saldo (bool): If True, executes the download_saldo function for the saldo_analitico query.
    pedidos (bool): If True, executes the download_pedidos function for the pedidos query.
    faturamento (bool): If True, executes the download_faturamento function for the faturamento query.

    Returns:
    None: The results are saved to Excel files and may be opened for viewing if specified in the individual functions.
    """
    # Log the start of the download process for the specific queries
    logger.info(f"Starting the download process for filial: {filial}")

    # Define a list of all branches.
    all_filials = ['0101', '0103', '0104', "0105"]

    # If filial is "Todas", loop through all branches
    if filial == 'Todas':
        filials_to_process = all_filials
        open_flag = False
    else:
        filials_to_process = [filial]
        open_flag = True

    for current_filial in filials_to_process:
        logger.info(f"Starting the download process for filial: {current_filial}")

        # Download data for the saldo_analitico query if requested
        if saldo:
            download_saldo(current_filial, open_flag)

        # Download data for the pedidos query if requested
        if pedidos:
            download_pedidos(current_filial, pedidos_selected_date, open_flag)

        # Download data for the faturamento query if requested
        if faturamento:
            download_faturamento(current_filial, faturamento_selected_date, open_flag)

    logger.info("Download process completed.")
