import logging
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

    # If the final data set is successfully retrieved, return it. Otherwise, log an error.
    if data_frame is not None:
        logger.info(f"Search complete with {len(data_frame)} results.")
        return data_frame
    else:
        logger.error("An error occurred during the search.")
        return

