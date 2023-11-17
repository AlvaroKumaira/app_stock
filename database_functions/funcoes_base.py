import pandas as pd
import logging
import os
import datetime
from database_functions.db_connect import Database, config
from openpyxl import load_workbook
from main_functions.fetch_params import merge_sheets

# Get a logger
logger = logging.getLogger(__name__)


def download(query, params=None):
    """
    Downloads data from the database using a specified SQL query.

    Parameters:
    - query (str): SQL query to execute.
    - params (dict, optional): Parameter for the SQL query.

    Returns:
    - DataFrame: DataFrame containing the results or None if an error occurred.
    """
    # Create an instance of the Database class and establish a connection.
    db_instance = Database(db_config=config, db_type='sql_server')
    db = db_instance.connect()

    try:
        # Execute the SQL query and store the result in a DataFrame.
        if params:
            data_frame = pd.read_sql(query, db, params=params)
        else:
            data_frame = pd.read_sql(query, db)
        logger.info("download was successful")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        data_frame = None

    return data_frame


def save_to_excel(data_frame, filename_prefix, filial, open_file=False):
    """
    Saves a DataFrame to an Excel file on the user's Desktop in a folder named 'Resultado'.
    
    Parameters:
    - data_frame (DataFrame): The data to save.
    - filename_prefix (str): Prefix for the Excel filename.
    - filial (str): Additional information for the Excel filename.
    - open_file (bool, optional): If True, the Excel file will be opened. Defaults to False.

    Returns:
    - str: Path to the saved Excel file. If open_file is True, also returns the workbook and sheet objects.
    """
    # Determine the path to the user's Desktop.
    desktop = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop')

    # Ensure the 'Resultado' folder exists on the Desktop.
    result_folder = os.path.join(desktop, 'Resultado')
    if not os.path.exists(result_folder):
        os.makedirs(result_folder)

    # Determine the path for the Excel file inside 'Resultado' folder.
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d')
    excel_file_path = os.path.join(result_folder, f'{filename_prefix}_{filial}_{current_timestamp}.xlsx')

    # Write the DataFrame to an Excel file.
    logger.info(f"Writing data to {excel_file_path}.")
    data_frame.to_excel(excel_file_path, index=False)
    logger.info(f"Saved data to {excel_file_path} successfully.")

    # If open_file is True, open the Excel file and return workbook and sheet objects.
    if open_file:
        try:
            os.startfile(excel_file_path)
        except Exception as e:
            logger.error(f"Could not open the file: {e}")

    return excel_file_path
