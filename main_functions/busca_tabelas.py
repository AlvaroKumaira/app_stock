import logging
import os
import pandas as pd
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import search_table, table_result


def get_table_columns(user_table_search):
    """
    Execute a search based on the user's input.

    This function takes in a user's search term, executes a preliminary search to find
    the table and returns the column names

    Parameters:
    - user_search (str): The user's inputted table.

    Returns:
    - pd.DataFrame: A dataframe containing the search results.
    """

    # Get a logger
    logger = logging.getLogger(__name__)

    # Log the start of the search process
    logger.info("Starting the table search process.")

    # Use the 'download' function to execute the initial search query
    query = search_table(user_table_search)
    search_results = download(query)

    # Check if search results are valid and the required column exists
    if search_results is None or search_results.empty:
        logger.error("No results found for the search.")
        return
    else:
        column_names = search_results.columns.tolist()
        return column_names


def download_save_table(columns, table):
    query = table_result(columns, table)
    table_df = download(query)
    save_to_excel(table_df, f"{table}", "table", True)
