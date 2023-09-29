import pandas as pd
import logging
import os
import datetime
from database_functions.db_connect import Database, config
from openpyxl import load_workbook
from main_functions.atualizar_params import merge_sheets

# Get a logger
logger = logging.getLogger(__name__)


def download(query, params):
    """
    Downloads data from the database using a specified SQL query.
    
    Parameters:
    - query (str): SQL query to execute.
    - params (dict): Parameter for the SQL query.

    Returns:
    - DataFrame: DataFrame containing the results or None if an error occurred.
    """
    # Create an instance of the Database class and establish a connection.
    db_instance = Database(db_config=config, db_type='sql_server')
    db = db_instance.connect()

    try:
        # Execute the SQL query and store the result in a DataFrame.
        data_frame = pd.read_sql(query, db, params=params)
        logger.info("download was successful")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        data_frame = None

    return data_frame


def save_to_excel(data_frame, filename_prefix, filial, open_file=False, logger=logger):
    """
    Saves a DataFrame to an Excel file on the user's Desktop in a folder named 'Resultado'.
    
    Parameters:
    - data_frame (DataFrame): The data to save.
    - filename_prefix (str): Prefix for the Excel filename.
    - filial (str): Additional information for the Excel filename.
    - open_file (bool, optional): If True, the Excel file will be opened. Defaults to False.
    - logger (Logger, optional): Logger for recording events. Defaults to the global logger.

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
    current_timestamp = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    excel_file_path = os.path.join(result_folder, f'{filename_prefix}_{filial}_{current_timestamp}.xlsx')

    # Write the DataFrame to an Excel file.
    logger.info(f"Writing data to {excel_file_path}.")
    data_frame.to_excel(excel_file_path, index=False)
    logger.info(f"Saved data to {excel_file_path} successfully.")

    # If open_file is True, open the Excel file and return workbook and sheet objects.
    if open_file:
        book = load_workbook(excel_file_path)  # Load the workbook.
        sheet = book['Sheet1']  # Access the default sheet.
        return excel_file_path, book, sheet

    return excel_file_path


def export_to_mysql(excel_path, tablename):
    """
    Extracts data from an Excel file and exports it to a specified MySQL table.
    
    Parameters:
    - excel_path (str): Path to the Excel file to extract data from.
    - tablename (str): Name of the MySQL table where the data should be saved.

    Note:
    Any existing data in the MySQL table will be replaced with the new data.
    """
    # Create an instance of the Database class and establish a connection.
    db_instance = Database(db_config=config, db_type="mysql")
    engine = db_instance.connect()

    # Placeholder for the merged DataFrame.
    merged_df = None

    try:
        # Merge data from all sheets in the Excel file into a single DataFrame.
        merged_df = merge_sheets(excel_path)
        logger.info("Data extraction and merging were successful")

        # Save the merged DataFrame to the MySQL table.
        merged_df.to_sql(tablename, engine, if_exists='replace', index=False)
        logger.info(f"Data saved successfully to {tablename} in the MySQL database")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


def get_params_from_mysql(tablename):
    """
    Fetch specified columns from a MySQL table.
    
    Parameters:
    - tablename (str): The name of the table in the MySQL database.

    Returns:
    - DataFrame: Data fetched from the specified MySQL table.
    """
    db_instance = Database(db_config=config, db_type="mysql")
    engine = db_instance.connect()
    try:
        params_df = pd.read_sql(f"SELECT B1_ZGRUPO, seguranca, mult, N_Comprar FROM {tablename}", engine)
        return params_df
    except Exception as e:
        logger.error(f"An error occurred while fetching data from MySQL table '{tablename}': {e}")
        return pd.DataFrame()
