import pandas as pd
import numpy as np
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import report_query, report_query_orders
from main_functions.sugestao_compra import create_final_df


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

    merged_data = pd.merge(original_data, sales_metrics, on='Agrupamento', how='left')
    merged_data = pd.merge(merged_data, orders_metrics, on='Agrupamento', how='left')
    return merged_data


def create_report(filial, period):
    # Map string representation of periods to their corresponding integer values
    period_str_to_int = {
        "3 meses": 3,
        "6 meses": 6,
        "12 meses": 12,
        "24 meses": 24
    }

    period_int = period_str_to_int.get(period, 0)

    sales_info_df = get_data(filial, period_int, select_func=1)
    sales_info_df = calculate_sales_metrics(sales_info_df, period_int)
    order_info_df = get_data(filial, period_int, select_func=0)
    order_info_df = calculate_order_metrics(order_info_df)
    base_df = create_final_df(filial, func=False)
    report_df = merge_data(base_df, sales_info_df, order_info_df)

    columns_to_keep = ['Agrupamento', 'Descrição', 'Código', 'Estoque', 'Quantidade pedida', 'Nota', 'Segurança', 'min',
                       'max', 'sales_period_count', 'demand_period_sum', 'average_demand', 'average_cost']

    report_df = report_df[columns_to_keep]

    # Rename columns
    column_mapping = {
        'Estoque': 'Saldo CO',
        'Quantidade Pedida': 'Qtd. Pedida',
        'min': 'Min',
        'max': 'Max',
        'sales_period_count': 'Vendas no período',
        'demand_period_sum': 'Demanda no período',
        'average_demand': 'Demanda média mensal',
        'average_cost': 'Custo médio'
    }

    report_df = report_df.rename(columns=column_mapping)

    save_to_excel(report_df, f"analise_inventario_{period_int}", filial, open_file=True)
