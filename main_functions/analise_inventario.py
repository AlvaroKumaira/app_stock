import pandas as pd
import numpy as np
import os
import logging
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import report_query, report_query_orders
from main_functions.processamento import classify_stock_items

# Get a logger
logger = logging.getLogger(__name__)


# Get the sales information
def get_data(filial, period, select_func):
    # Map user-selected period to number of days
    period_to_days = {
        3: 89,
        6: 182,
        12: 365,
        24: 730
    }

    # Get the number of days based on the user-selected period
    query_time = period_to_days.get(period, 0)

    # If query_time is still 0, it means the user provided an invalid period.
    if query_time == 0:
        return None

    if select_func == 1:
        # Generate the query string and fetch the sales data
        query_string = report_query(query_time, filial)
        sales_df = download(query_string)

        sales_df = sales_df.rename(columns={'B1_ZGRUPO': 'Agrupamento'})

        return sales_df
    else:
        query_string = report_query_orders(query_time, filial)
        orders_df = download(query_string)

        orders_df = orders_df.rename(columns={'B1_ZGRUPO': 'Agrupamento'})

        return orders_df


def calculate_sales_metrics(data_frame, months):
    """
    Calculate the sales metrics for a given DataFrame.

    Parameters:
    - data_frame (DataFrame): The sales data.
    - months (int): The number of months selected by the user.

    Returns:
    - DataFrame: DataFrame containing the sales metrics for each B1_ZGRUPO.
    """
    # Group by B1_ZGRUPO and calculate metrics
    metrics = data_frame.groupby('Agrupamento').agg(
        sales_period_count=pd.NamedAgg(column='D2_COD', aggfunc='size'),
        demand_period_sum=pd.NamedAgg(column='D2_QUANT', aggfunc='sum'),
    )

    # Calculate averages
    metrics['average_demand'] = np.ceil(metrics['demand_period_sum'] / months).astype(int)

    return metrics


def calculate_order_metrics(data_frame):
    """
    Calculate the orders metrics for a given DataFrame.

    Parameters:
    - data_frame (DataFrame): The order data.
    - months (int): The number of months selected by the user.

    Returns:
    - DataFrame: DataFrame containing the sales metrics for each B1_ZGRUPO.
    """
    # Group by B1_ZGRUPO and calculate metrics
    metrics = data_frame.groupby('Agrupamento').agg(
        cost_period_sum=pd.NamedAgg(column='C7_PRECO', aggfunc='sum'),
        cost_period_size=pd.NamedAgg(column='C7_PRECO', aggfunc='size')
    )

    # Calculate averages
    metrics['average_cost'] = np.round(metrics['cost_period_sum'] / metrics['cost_period_size'], decimals=2)

    return metrics


def merge_data(original_data, sales_metrics, orders_metrics):
    """
    Merge the original data with the sales metrics using the B1_ZGRUPO column.

    Parameters:
    - original_data (DataFrame): The original Excel data.
    - sales_metrics (DataFrame): The calculated sales metrics.

    Returns:
    - DataFrame: Merged data containing original columns and sales metrics.
    """

    # Ensure 'Filial' and 'Agrupamento' are strings in all DataFrames to avoid type mismatch
    columns_to_convert = ['Agrupamento', 'Filial']
    for df in [original_data, sales_metrics, orders_metrics]:
        for column in columns_to_convert:
            if column in df.columns:
                df[column] = df[column].astype(str).apply(lambda x: x.zfill(4))

    merged_data = pd.merge(original_data, sales_metrics, on=columns_to_convert, how='left')
    merged_data = pd.merge(merged_data, orders_metrics, on=columns_to_convert, how='left')

    return merged_data


def create_report(filial, period, func):
    """
    Generates a sales report for a given branch and period.

    Parameters:
    - filial (str): The branch for which the report is to be generated. 'Todas' indicates all branches.
    - period (str): The period for the report, such as '3 meses', '6 meses', '12 meses', or '24 meses'.
    - func (bool): Flag to determine the mode of report generation. True for individual branch reports, False for aggregated.

    Returns:
    - DataFrame: The generated report as a DataFrame.
    """

    # Convert period from string to integer
    period_mapping = {"3 meses": 3, "6 meses": 6, "12 meses": 12, "24 meses": 24}
    period_int = period_mapping.get(period, 0)

    # Base_df path
    data_file_path = os.path.join('params', 'Base_df.xlsx')

    # Define all available branches
    all_branches = ['0101', '0103', '0104', '0105']

    # Determine branches to process
    filials_to_process = all_branches if filial == 'Todas' else [filial]

    aggregated_report_df = None

    for current_filial in filials_to_process:
        # Process sales information
        sales_info_df = get_data(current_filial, period_int, select_func=1)
        sales_info_df = calculate_sales_metrics(sales_info_df, period_int)
        sales_info_df['Filial'] = current_filial

        # Process order information
        order_info_df = get_data(current_filial, period_int, select_func=0)
        order_info_df = calculate_order_metrics(order_info_df)
        order_info_df['Filial'] = current_filial

        # Create and merge final data
        base_df = pd.read_excel(data_file_path)
        report_df = merge_data(base_df, sales_info_df, order_info_df)

        # Concatenate this report_df to the aggregated_report_df
        if aggregated_report_df is None:
            aggregated_report_df = report_df
        else:
            aggregated_report_df = pd.concat([aggregated_report_df, report_df], ignore_index=True)

    # coluna filial em primeiro e coluna de ind_estoque antes da nota
    columns_to_keep = ['Filial', 'Agrupamento', 'Código', 'Descrição', 'Grupo', 'Estoque', 'Quantidade pedida', 'Nota',
                       'Segurança', 'min', 'max', 'sales_period_count', 'demand_period_sum', 'average_demand',
                       'average_cost']

    intermediate_df = aggregated_report_df[columns_to_keep].copy()

    intermediate_df.replace(np.nan, 0, inplace=True)

    final_df = classify_stock_items(intermediate_df)
    # Define the new order and the new names for specific columns
    column_order = ['Filial', 'Agrupamento', 'Código', 'Descrição', 'Grupo', 'Estoque', 'Quantidade pedida', 'Ind. Stk',
                    'Nota', 'Segurança', 'min', 'max', 'Vendas no período', 'Demanda no período', 'Demanda media',
                    'Custo médio']

    # Dictionary for renaming columns
    rename_dict = {
        'sales_period_count': 'Vendas no período',
        'demand_period_sum': 'Demanda no período',
        'average_demand': 'Demanda media',
        'average_cost': 'Custo médio'
    }

    # Rename and reorder the columns
    final_df_renamed = final_df.rename(columns=rename_dict).copy()
    final_df_ordered = final_df_renamed[column_order].copy()

    # Remove duplicate rows based on 'Filial' and 'Agrupamento' columns
    final_df_ordered = final_df_ordered.drop_duplicates(subset=['Filial', 'Agrupamento'])

    # Filter the DataFrame for the specified 'Filial' if not processing all branches
    if filial != 'Todas':
        final_df_ordered = final_df_ordered[final_df_ordered['Filial'] == filial]

    # After processing all branches, save the aggregated DataFrame
    if not func:
        save_to_excel(final_df_ordered, f"analise_inventario_{period_int}",
                      'Todas' if filial == 'Todas' else filial, open_file=False)

    return final_df_ordered
