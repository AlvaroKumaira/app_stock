import pandas as pd
from openpyxl.styles import numbers
import numpy as np
import logging
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import pedidos, faturamento, saldo_analitico

# Get a logger
logger = logging.getLogger(__name__)

def download_saldo(filial):
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
        params = (filial,)
        data_frame = download(saldo_analitico, params)
        logger.info(f"Downloaded {data_frame.shape[0]} rows of data for saldo_analitico.")
    except Exception as e:
        logger.error(f"An error occurred during download: {str(e)}")
        return
    
    # Rename columns
    column_mapping = {
        "B1_COD": "CODIGO",
        "B1_TIPO": "TP",
        "B1_GRUPO": "GRUPO",
        "B1_DESC": "DESCRICAO",
        "ENDERECO": "ENDEREÇO",
        "B1_UM": "U.M.",
        "B2_FILIAL": "FL",
        "B2_LOCAL": "ARMZ",
        "B2_QATU_COPY": "SALDO EM ESTOQUE",
        "TEMP_1": "EMPENHO PARA REQ/PV/RESERVA",
        "B2_QATU": "ESTOQUE DISPONIVEL",
        "B2_CM1": "C UNITARIO",
        "B2_VATU1": "VALOR EM ESTOQUE",
        "TEMP_2": "VALOR EMPENHADO",
        "B1_UCOM": "DT.ULT.COMPRA",
        "B2_USAI": "DT.ULT.SAIDA",
        "DAYS_DIFF": "DIAS"
    }
    data_frame.rename(columns=column_mapping, inplace=True)
    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Convert string dates to datetime objects using the renamed columns
    data_frame["DT.ULT.COMPRA"] = pd.to_datetime(data_frame["DT.ULT.COMPRA"], format='%Y%m%d', errors='coerce')
    data_frame["DT.ULT.SAIDA"] = pd.to_datetime(data_frame["DT.ULT.SAIDA"], format='%Y%m%d', errors='coerce')

    # Remove trailing spaces from certain columns
    for column in ["CODIGO", "DESCRICAO"]:
        data_frame[column] = data_frame[column].str.rstrip()

    try:
        # Save DataFrame to Excel and open the file
        excel_file_path, book, sheet = save_to_excel(data_frame, 'saldo_analítico', filial, open_file=True)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return

    # Specify your column formats
    column_format = {
        "CODIGO": "0",
        "SALDO EM ESTOQUE": "#,##0.00",
        "EMPENHO PARA REQ/PV/RESERVA": "#,##0.00",
        "ESTOQUE DISPONIVEL": "#,##0.00",
        "C UNITARIO": "#,##0.00",
        "VALOR EM ESTOQUE": "#,##0.00",
        "VALOR EMPENHADO": "#,##0.00",
        "DT.ULT.COMPRA": numbers.FORMAT_DATE_DDMMYY,
        "DT.ULT.SAIDA": numbers.FORMAT_DATE_DDMMYY
    }
    for col, number_format in column_format.items():
        col_index = data_frame.columns.get_loc(col) + 1  # get column index in Excel (1-indexed)
        for row in range(2, len(data_frame)+2):  # Apply formatting row by row (2+ because of 1-indexing and header)
            sheet.cell(row=row, column=col_index).number_format = number_format

    book.save(excel_file_path)

def download_pedidos(filial, date):
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
        "C7_NUM": "Num.PC",
        "C7_FORNECE": "Fornecedor",
        "A2_LOJA_COPY": "Loja",
        "A2_NOME": "Razao Social",
        "A2_TEL": "Telefone",
        "C7_ITEM": "Item",
        "C7_NUMSC": "Numero da SC",
        "C7_PRODUTO": "Produto",
        "C7_DESCRI": "Descricao",
        "B1_GRUPO": "Grupo",
        "C7_EMISSAO": "Emissao",
        "A2_LOJA":"Lj",
        "ENT": "Entrega",
        "C7_QUANT": "Quantidade",
        "C7_UM": "UM",
        "C7_PRECO": "Prc Unitario",
        "DE": "Vl.Desconto",
        "C7_VALIP": "Vlr.IPI",
        "C7_TOTAL":"Vlr.Total",
        "C7_QUJE":"Qtd.Entregue",
        "QRE":"Quant.Receber",
        "SRE":"Saldo Receber",
        "C7_RESIDUO":"Res.Elim."
    }
    data_frame.rename(columns=column_mapping, inplace=True)

    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Remove trailing spaces from certain columns
    for column in ["Razao Social", "Telefone", "Produto", "Descricao", "Grupo"]:
        data_frame[column] = data_frame[column].str.rstrip()

    try:
        # Save DataFrame to Excel and open the file
        excel_file_path, book, sheet = save_to_excel(data_frame, 'pedidos', filial, open_file=True)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return

    # Specify your column formats
    column_format = {
        "Emissao": numbers.FORMAT_DATE_DDMMYY,
        "Entrega": numbers.FORMAT_DATE_DDMMYY
    }
    for col, number_format in column_format.items():
        col_index = data_frame.columns.get_loc(col) + 1  # get column index in Excel (1-indexed)
        for row in range(2, len(data_frame) + 2):  # Apply formatting row by row (2+ because of 1-indexing and header)
            sheet.cell(row=row, column=col_index).number_format = number_format

    book.save(excel_file_path)

def download_faturamento(filial):
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

    # Try to download the data, catch any exceptions
    try:
        # Use the download function to execute the SQL query and store the result in a DataFrame
        params = (filial,)
        data_frame = download(faturamento, params)
        logger.info(f"Downloaded {data_frame.shape[0]} rows of data for faturamento.")
    except Exception as e:
        logger.error(f"An error occurred during download: {str(e)}")
        return
    
    # Rename columns
    column_mapping = {
        "D2_TP": "TP",
        "D2_COD": "Codigo",
        "B1_DESC": "Descricao",
        "D2_QUANT": "Quantidade",
        "QUANT_DUP": "Qtd.Movim.",
        "D2_UM": "Unidade",
        "D2_CUSTO1": "(A)Custo Total",
        "D2_PRCVEN": "Custo por Unidade",
        "VFB": "Val Faturado Bruto",
        "VF": "(B)Valor Faturado",
        "INF": "Impostos nao Faturados",
        "VFINP": "Val Faturado- Imp. Nao Fat.",
        "D2_MARGEM": "Margem",
        "TEMP1": "Mix *Mar",
        "TEMP2": "",
        "TEMP3": "Mix",
        "TEMP4":"Custo de Reposicao",
        "TEMP5":"Variacao"
    }
    data_frame.rename(columns=column_mapping, inplace=True)

    # Replace blank strings with NaN
    data_frame.replace(r'^\s*$', np.nan, regex=True, inplace=True)

    # Remove trailing spaces from certain columns
    for column in ["Codigo", "Descricao"]:
        data_frame[column] = data_frame[column].str.rstrip()

    # Convert specified columns to numeric data
    columns_to_sum = ["Quantidade", "Qtd.Movim.", "(A)Custo Total", "Custo por Unidade",
                      "Val Faturado Bruto", "(B)Valor Faturado", "Val Faturado- Imp. Nao Fat.", "Margem"]
    data_frame[columns_to_sum] = data_frame[columns_to_sum].apply(pd.to_numeric, errors='coerce')

    # Aggregate values by 'Codigo' and sum the values of the specified columns
    data_frame = data_frame.groupby(['Codigo', 'Descricao', 'Unidade', 'TP'], as_index=False)[columns_to_sum].sum()

    # Calculate 'valor unitario'
    data_frame['valor unitario'] = (data_frame['Val Faturado Bruto'] / data_frame['Quantidade']).round(2)

    try:
        # Save DataFrame to Excel and optionally open the file
        save_to_excel(data_frame, 'faturamento', filial, open_file=False)
    except Exception as e:
        logger.error(f"An error occurred while saving to Excel: {str(e)}")
        return
    
def download_tabelas(filial, saldo, pedidos, faturamento, selected_date):
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

    # Download data for the saldo_analitico query if requested
    if saldo:
        download_saldo(filial)

    # Download data for the pedidos query if requested
    if pedidos:
        download_pedidos(filial, selected_date)

    # Download data for the faturamento query if requested
    if faturamento:
        download_faturamento(filial)

    logger.info("Download process completed.")
    
if __name__ == "__main__":
    download_tabelas() 