import logging
import os
import pandas as pd
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import query_busca, query_resultado, query_resultado_cod_item


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
    if (search_results.empty or 'B1_ZGRUPO' not in search_results.columns or
            not search_results.iloc[0]['B1_ZGRUPO'].strip()):
        data_frame = download(query_resultado_cod_item, (user_search,))
        return data_frame

    # Extract the group ID from the initial search results
    group_id = search_results.iloc[0]['B1_ZGRUPO']

    # Use the 'download' function to retrieve the final data set based on the group ID
    data_frame = download(query_resultado, (group_id,))

    # If the final data set is successfully retrieved, perform additional operations
    if not data_frame.empty:
        try:
            # Define the path to the Excel file within the 'params' folder at the project's root
            inv_file_path = os.path.join('params', 'inv_df.xlsx')

            inv_df = pd.read_excel(inv_file_path)

            # Convert the group ID columns to string to ensure matching types
            data_frame['Agrupamento'] = data_frame['Agrupamento'].astype(str)
            inv_df['Agrupamento'] = inv_df['Agrupamento'].astype(str)
            inv_df['Filial'] = inv_df['Filial'].astype(str).str.zfill(4)
            data_frame['Filial'] = data_frame['Filial'].astype(str).str.zfill(4)

            columns_to_drop = ['Descrição', 'Grupo', 'Estoque']
            inv_df = inv_df.drop(columns=columns_to_drop)

            merged_df = data_frame.merge(inv_df[['Agrupamento', 'Filial', 'Ind. Stk', 'min', 'max', 'Segurança', 'Nota',
                                                 'Vendas no período', 'Demanda no período']],
                                         on=['Agrupamento', 'Filial'], how='left')

            merged_df.fillna(0, inplace=True)

            # Log the completion of the process and return the merged DataFrame
            logger.info(f"Search complete with {len(merged_df)} results, additional data merged from local Excel file.")
            return merged_df

        except Exception as e:
            logger.error(f"An error occurred during the merging process: {e}")
            return data_frame  # Return the original DataFrame if merging fails
    else:
        logger.error("An error occurred during the search.")
        return pd.DataFrame()  # Return an empty DataFrame for consistency
