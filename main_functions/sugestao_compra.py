import pandas as pd
import numpy as np
import logging
from database_functions.funcoes_base import download, save_to_excel
from main_functions.fetch_params import merge_sheets
from main_functions.processamento import calculate_grades, calculate_min_max_columns, calculate_stock_suggestion
from database_functions.queries import info_gerais, historico_faturamento, quantidade_receber

# Get a logger
logger = logging.getLogger(__name__)


def download_method(query, params):
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
    params = (filial,)
    gi_data_frame = download_method(query, params)

    # Convert 'B2_QATU' column to numeric data
    column_to_sum = "B2_QATU"
    gi_data_frame[column_to_sum] = gi_data_frame[column_to_sum].apply(pd.to_numeric, errors='coerce')

    # Aggregate by 'B1_ZGRUPO'
    gi_data_frame = gi_data_frame.groupby(['B1_ZGRUPO', 'B2_FILIAL'], as_index=False).agg({
        'B1_DESC': 'first',
        'B1_COD': 'first',
        'B1_GRUPO': 'first',
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
    o_data_frame = o_data_frame.groupby(['B1_ZGRUPO', 'C7_FILIAL'], as_index=False).agg({column_to_sum: 'sum'})

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

    # Aggregate data
    result = fh_data_frame.groupby(['B1_ZGRUPO', 'D2_FILIAL', 'Month_Year'], as_index=False)[column_to_sum].sum()

    # Pivot with both 'B1_ZGRUPO' and 'B1_FILIAL' in the index
    pivot_result = result.pivot_table(index=['B1_ZGRUPO', 'D2_FILIAL'], columns='Month_Year', values=column_to_sum,
                                      fill_value=0, aggfunc='sum')

    # Calculate additional metrics
    pivot_result['total_sum'] = pivot_result.sum(axis=1)
    pivot_result['avg_last_two_months'] = np.ceil(pivot_result.iloc[:, -3:-1].mean(axis=1)).astype(int)
    pivot_result['avg_last_three_months'] = np.ceil(pivot_result.iloc[:, -5:-2].mean(axis=1)).astype(int)

    # Reset the index to make 'B1_ZGRUPO' and 'B1_FILIAL' columns again
    pivot_result = pivot_result.reset_index()

    fh_data_frame = calculate_grades(pivot_result)

    logger.info(f"Processed fat_history for branch {filial}.")
    return fh_data_frame


def join_parts(*data_frames):
    """
    Join multiple data frames on 'Agrupamento' and 'Filial' columns.
    NaN values will be filled with 0.

    Parameters:
    - *data_frames (DataFrames): One or more data frames to be joined.

    Returns:
    - DataFrame: The joined data frame.
    """
    if not data_frames:
        raise ValueError("At least one dataframe must be provided.")

        # Standardize the 'Filial' column name in the first data frame
    filial_column = next((col for col in ['B2_FILIAL', 'D2_FILIAL', 'C7_FILIAL'] if col in data_frames[0].columns), None)
    if filial_column is None:
        raise ValueError("First data frame is missing 'Filial' column")
    joined_df = data_frames[0].rename(columns={filial_column: 'Filial'})

    for df in data_frames[1:]:
        # Identify the 'Filial' column in the current data frame
        filial_column = next((col for col in ['B2_FILIAL', 'D2_FILIAL', 'C7_FILIAL'] if col in df.columns), None)
        if filial_column is None:
            raise ValueError("Data frame is missing 'Filial' column")

        # Rename the 'Filial' column to a standard name for consistency
        df = df.rename(columns={filial_column: 'Filial'})

        # Check for 'B1_ZGRUPO' column and merge
        if 'B1_ZGRUPO' in df.columns:
            joined_df = pd.merge(joined_df, df, on=['B1_ZGRUPO', 'Filial'], how='left')
        else:
            raise ValueError("Data frame is missing 'B1_ZGRUPO' column")

    joined_df.fillna(0, inplace=True)
    return joined_df


def create_final_df(filial, func):
    """
    Create the final data frame by merging and computing different columns.

    Parameters:
    - filial (str): The specific branch or 'Todas' for all branches.
    - func (bool): Flag to indicate whether to save the results to Excel.

    Returns:
    - pd.DataFrame: The final data frame.
    """

    # Define a list of all branches
    all_filials = ['0101', '0103', '0104', "0105"]

    if filial == 'Todas':
        filials_to_process = all_filials
    else:
        filials_to_process = [filial]

    aggregated_df = pd.DataFrame()

    for current_filial in filials_to_process:
        logger.info(f"Creating final data frame for branch {current_filial}.")

        general_info = general_information(current_filial)
        order_info = orders(current_filial)
        fat_info = fat_history(current_filial)

        # Join the tables
        joined_table = join_parts(general_info, order_info, fat_info)
        params_df = merge_sheets(filial=current_filial)

        # Ensure 'B1_ZGRUPO' is a string for merging purposes
        joined_table['B1_ZGRUPO'] = joined_table['B1_ZGRUPO'].astype(str)
        params_df['B1_ZGRUPO'] = params_df['B1_ZGRUPO'].astype(str)

        # Merge the tables including the 'Filial' column in the merge to ensure differentiation
        intermediate_df = pd.merge(joined_table, params_df, on='B1_ZGRUPO', how='left')

        intermediate_df = calculate_min_max_columns(intermediate_df)
        final_df = calculate_stock_suggestion(intermediate_df)

        column_mapping_excel = {
            'B1_ZGRUPO': 'Agrupamento',
            'Filial': 'Filial',
            'B1_GRUPO': 'Grupo',
            'B1_DESC': 'Descrição',
            'B1_COD': 'Código',
            'B2_QATU': 'Estoque',
            'QRE': 'Quantidade pedida',
            'total_sum': 'Total',
            'avg_last_two_months': 'Média_2',
            'avg_last_three_months': 'Média_3',
            'grade': 'Nota',
            'stock_suggestion': 'Sugestão de Compra'
        }
        # Rename and map columns as needed
        final_df = final_df.rename(columns=column_mapping_excel)

        # Concatenate the current final_df into the aggregated_df
        if aggregated_df.empty:
            aggregated_df = final_df
        else:
            aggregated_df = pd.concat([aggregated_df, final_df], ignore_index=True)

    # Ensure the 'Filial' column is formatted correctly
    if 'Filial' in aggregated_df.columns:
        aggregated_df['Filial'] = aggregated_df['Filial'].astype(str).apply(lambda x: x.zfill(4))

    print(aggregated_df.head())

    if func:
        file_name = "sugestão_compra_" + ('Todas' if filial == 'Todas' else filial)
        save_to_excel(aggregated_df, file_name, '', open_file=True)
        logger.info(
            f"Final data frame for {'all branches' if filial == 'Todas' else 'branch ' + filial} saved to Excel.")

    return aggregated_df
