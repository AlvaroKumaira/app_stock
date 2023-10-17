import pandas as pd
import numpy as np
from database_functions.funcoes_base import download, save_to_excel
from database_functions.queries import report_query
from main_functions.sugestao_compra import create_final_df


# Get the sales information
def get_sales_data(filial, period):
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
    # You can either handle this with an error message or just return None.
    if query_time == 0:
        return None

    # Generate the query string and fetch the sales data
    query_string = report_query(query_time, filial)
    sales_df = download(query_string)

    return sales_df


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
    metrics = data_frame.groupby('B1_ZGRUPO').agg(
        sales_period_count=pd.NamedAgg(column='D2_COD', aggfunc='size'),
        demand_period_sum=pd.NamedAgg(column='D2_QUANT', aggfunc='sum')
    )

    # Calculate average demand
    metrics['average_demand'] = np.ceil(metrics['demand_period_sum'] / months).astype(int)

    return metrics


def merge_data(original_data, sales_metrics):
    """
    Merge the original data with the sales metrics using the B1_ZGRUPO column.

    Parameters:
    - original_data (DataFrame): The original Excel data.
    - sales_metrics (DataFrame): The calculated sales metrics.

    Returns:
    - DataFrame: Merged data containing original columns and sales metrics.
    """

    merged_data = pd.merge(original_data, sales_metrics, on='B1_ZGRUPO', how='left')
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

    info_df = get_sales_data(filial, period_int)
    sales_df = calculate_sales_metrics(info_df, period_int)
    base_df = create_final_df(filial, func=False)
    report_df = merge_data(base_df, sales_df)

    columns_to_keep = ['B1_ZGRUPO', 'B1_DESC', 'B1_COD', 'B2_QATU', 'QRE', 'grade', 'Segurança', 'min', 'max',
                       'sales_period_count', 'demand_period_sum', 'average_demand']

    report_df = report_df[columns_to_keep]

    # Rename columns
    column_mapping = {
        'B1_ZGRUPO': 'Agrupamento',
        'B1_DESC': 'Descrição',
        'B1_COD': 'Código',
        'B2_QATU': 'Saldo CO',
        'QRE': 'Qtd. Pedida',
        'grade': 'Nota',
        'Segurança': 'Segurança',
        'min': 'Min',
        'max': 'Max',
        'sales_period_count': 'Vendas no período',
        'demand_period_sum': 'Demanda no período',
        'average_demand': 'Demanda média mensal'
    }

    report_df = report_df.rename(columns=column_mapping)

    save_to_excel(report_df, f"analise_inventario_{period_int}", filial, open_file=True)