import pandas as pd
import numpy as np
import logging
from database_functions.funcoes_base import download, save_to_excel, get_params_from_mysql
from main_functions.processamento import calculate_grades, calculate_min_max_columns, calculate_stock_suggestion
from database_functions.queries import info_gerais, historico_faturamento, quantidade_receber

# Get a logger
logger = logging.getLogger(__name__)



def download_method( query, params):
    """
    Download data using a given SQL query and parameters.
    This function additionally filters out rows where 'B1_ZGRUPO' is missing or an empty string.
    
    Parameters:
    - query (str): SQL query to fetch the data.
    - params (tuple): Parameters for the SQL query.

    Returns:
    - DataFrame: Filtered data.
    """
    data_frame = download(query, params)
    # Remove rows where 'B1_ZGRUPO' is missing, null, or an empty string
    data_frame = data_frame[data_frame['B1_ZGRUPO'].str.strip() != '']
    return data_frame

def general_information(filial):
    """
    Fetch and process the general information data frame for a specific branch (filial).

    Parameters:
    - filial (str): The branch code.

    Returns:
    - DataFrame: Processed general information.
    """
    logger.info(f"Fetching general information for branch {filial}.")
    
    query = info_gerais
    params = (filial, filial,)
    gi_data_frame = download_method(query, params)

    # Convert 'B2_QATU' column to numeric data
    column_to_sum = "B2_QATU"
    gi_data_frame[column_to_sum] = gi_data_frame[column_to_sum].apply(pd.to_numeric, errors='coerce')

    # Aggregate by 'B1_ZGRUPO'
    gi_data_frame = gi_data_frame.groupby('B1_ZGRUPO', as_index=False).agg({
        'B1_DESC': 'first',
        'B1_COD': 'first',
        column_to_sum: 'sum'
    })

    logger.info(f"Processed general information for branch {filial}.")
    return gi_data_frame

def orders(filial):
    """
    Fetch and process the order information data frame for a specific branch (filial).

    Parameters:
    - filial (str): The branch code.

    Returns:
    - DataFrame: Processed order information.
    """
    logger.info(f"Fetching order information for branch {filial}.")
    
    query = quantidade_receber
    params = (filial,)
    o_data_frame = download_method(query, params)

    # Convert 'QRE' column to numeric data
    column_to_sum = "QRE"
    o_data_frame[column_to_sum] = o_data_frame[column_to_sum].apply(pd.to_numeric, errors='coerce')

    # Aggregate by 'B1_ZGRUPO'
    o_data_frame = o_data_frame.groupby('B1_ZGRUPO', as_index=False).agg({column_to_sum: 'sum'})

    logger.info(f"Processed order information for branch {filial}.")
    return o_data_frame
    
def fat_history(filial):
    """
    Fetch and process the fat_history information for a specific branch (filial).
    
    Parameters:
    - filial (str): The branch code.

    Returns:
    - DataFrame: Processed fat_history information.
    """
    logger.info(f"Fetching fat_history for branch {filial}.")
    
    query = historico_faturamento
    params = (filial,)
    fh_data_frame = download_method(query, params)

    # Process columns for datetime and numeric formats
    fh_data_frame['D2_EMISSAO'] = pd.to_datetime(fh_data_frame['D2_EMISSAO'], format='%Y%m%d', errors='coerce')
    fh_data_frame['Month_Year'] = fh_data_frame['D2_EMISSAO'].dt.to_period('M')
    column_to_sum = "D2_QUANT"
    fh_data_frame[column_to_sum] = fh_data_frame[column_to_sum].apply(pd.to_numeric, errors='coerce')

    # Aggregate data and process further
    result = fh_data_frame.groupby(['B1_ZGRUPO', 'Month_Year'], as_index=False)[column_to_sum].sum()
    pivot_result = result.pivot_table(index='B1_ZGRUPO', columns='Month_Year', values=column_to_sum, fill_value=0, aggfunc='sum')
    pivot_result['total_sum'] = pivot_result.sum(axis=1)
    pivot_result['avg_last_two_months'] = np.ceil(pivot_result.iloc[:, -3:-1].mean(axis=1)).astype(int)
    pivot_result['avg_last_three_months'] = np.ceil(pivot_result.iloc[:, -5:-2].mean(axis=1)).astype(int)

    fh_data_frame = calculate_grades(pivot_result)

    logger.info(f"Processed fat_history for branch {filial}.")
    return fh_data_frame

def join_parts(*data_frames):
    """
    Join multiple data frames on the 'B1_ZGRUPO' column.
    NaN values will be filled with 0.
    
    Parameters:
    - *data_frames (DataFrames): One or more data frames to be joined.

    Returns:
    - DataFrame: The joined data frame.
    """
    if not data_frames:
        raise ValueError("At least one dataframe must be provided.")
    
    joined_df = data_frames[0]
    for df in data_frames[1:]:
        joined_df = pd.merge(joined_df, df, on='B1_ZGRUPO', how='left')
    joined_df.fillna(0, inplace=True)
    return joined_df

def create_final_df(filial):
    """
    Create the final data frame by merging and computing different columns.

    Parameters:
    - filial (str): The specific branch.

    Returns:
    - None: The function saves the output to an Excel file.
    """
    
    logger.info(f"Creating final data frame for branch {filial}.")

    # Retrieve necessary data
    general_info = general_information(filial)
    order_info = orders(filial)
    fat_info = fat_history(filial)
    
    # Join the tables
    joined_table = join_parts(general_info, order_info, fat_info)

    if filial == "0101":
        # Fetch params from MySQL and prepare for merging
        params_df = get_params_from_mysql('params')
        joined_table['B1_ZGRUPO'] = joined_table['B1_ZGRUPO'].astype(str)
        params_df['B1_ZGRUPO'] = params_df['B1_ZGRUPO'].astype(str)

        # Merge the tables and calculate necessary columns
        intermediate_df = pd.merge(joined_table, params_df, on='B1_ZGRUPO', how='left')
    else:
        intermediate_df = joined_table

    intermediate_df = calculate_min_max_columns(filial, intermediate_df)
    final_df = calculate_stock_suggestion(filial, intermediate_df)

    # Save the final result
    save_to_excel(final_df, "sugestão_compra_", filial, open_file=True)
    logger.info(f"Final data frame for branch {filial} saved to Excel.")

