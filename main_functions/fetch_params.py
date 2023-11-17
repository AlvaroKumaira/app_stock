import pandas as pd
import os
import logging

# Get a logger
logger = logging.getLogger(__name__)


def round_and_stringify(value):
    try:
        # Replace the comma with a dot to treat the value as a float
        float_value = float(str(value).replace(',', '.'))

        # Round the float to the nearest integer
        rounded_value = round(float_value)

        # Convert the rounded integer back to a string
        return str(rounded_value)
    except Exception as e:
        logger.error(f"Error when running the round_stringify function: {e}")
        # If there's any error in the process, just return the original value as a string
        return str(value)


def simplify_columns(df):
    """Simplify the column headers by using only the top level if the second level is unnamed."""
    new_columns = []
    for col in df.columns:
        if "Unnamed" in col[1]:
            new_columns.append(col[0])
        else:
            new_columns.append((col[0], col[1]))
    df.columns = new_columns
    return df


def merge_sheets(filial, local_folder="params", file_name="Dados_Sug.xlsx"):

    excel_path = os.path.join(local_folder, file_name)

    # Read sheets
    df1 = pd.read_excel(excel_path, sheet_name="Plan1")

    df1_columns_to_keep = ['cod_agrup', f'SEG_{filial}', f'PN_{filial}']

    df1 = df1[df1_columns_to_keep]

    # Rename columns for clarity
    df1.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df1.rename(columns={f"SEG_{filial}": "Segurança"}, inplace=True)
    df1.rename(columns={f"PN_{filial}": "N_comprar"}, inplace=True)

    df1['B1_ZGRUPO'] = df1['B1_ZGRUPO'].apply(round_and_stringify)

    # Fill NaN values with appropriate defaults
    df1.fillna({"Segurança": 0, "N_comprar": 0}, inplace=True)

    # Group by 'B1_ZGRUPO' and get indices of rows with the highest 'seguranca' value
    idx = df1.groupby('B1_ZGRUPO')['Segurança'].idxmax()

    # Filter dataframe using these indices
    df1 = df1.loc[idx]

    return df1
