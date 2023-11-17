import logging
import os
import pandas as pd
from database_functions.funcoes_base import download
from database_functions.queries import query_busca, query_resultado


def search_function(user_search):
    """
    Execute a search based on the user's input.
    
    This function takes in a user's search term, executes a preliminary search to find
    the group ID associated with the term, and then retrieves the final data set based 
    on that group ID.
    
    Parameters:
    - user_search (str): The user's inputted search term or product ID.

    Returns:
    - pd.DataFrame: A dataframe containing the search results.
    """

    # Get a logger
    logger = logging.getLogger(__name__)

    # Log the start of the search process
    logger.info("Starting the search process.")

    # Use the 'download' function to execute the initial search query
    search_results = download(query_busca, (user_search,))

    # Check if search results are valid and the required column exists
    if search_results.empty or 'B1_ZGRUPO' not in search_results.columns:
        logger.error("No results found for the search.")
        return

    # Extract the group ID from the initial search results
    group_id = search_results.iloc[0]['B1_ZGRUPO']

    # Use the 'download' function to retrieve the final data set based on the group ID
    data_frame = download(query_resultado, (group_id,))

    # If the final data set is successfully retrieved, perform additional operations
    if not data_frame.empty:
        try:
            # Define the path to the Excel file within the 'params' folder at the project's root
            base_file_path = os.path.join('params', 'Base_df.xlsx')

            # Read the local Excel file from the specified folder
            base_df = pd.read_excel(base_file_path)

            # Convert the group ID columns to string to ensure matching types
            data_frame['B1_ZGRUPO'] = data_frame['B1_ZGRUPO'].astype(str)
            base_df['Agrupamento'] = base_df['Agrupamento'].astype(str)

            # Merge with the data_frame based on the group ID (B1_ZGRUPO to Agrupamento)
            merged_df = data_frame.merge(base_df, left_on='B1_ZGRUPO', right_on='Agrupamento', how='left')

            # Select only the necessary columns including the ones from the local file
            # Replace 'your_columns' with the actual column names from data_frame that you want to keep
            final_df = merged_df[['B1_ZGRUPO', 'B1_COD', 'B1_DESC', 'B2_QATU', 'Nota', 'min', 'max', 'Segurança']]

            # Log the completion of the process and return the merged DataFrame
            logger.info(f"Search complete with {len(final_df)} results, additional data merged from local Excel file.")
            return final_df

        except Exception as e:
            logger.error(f"An error occurred during the merging process: {e}")
            return data_frame  # Return the original DataFrame if merging fails
    else:
        logger.error("An error occurred during the search.")
        return pd.DataFrame()  # Return an empty DataFrame for consistency
