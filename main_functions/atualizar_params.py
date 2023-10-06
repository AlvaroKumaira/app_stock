import pandas as pd
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
    except:
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


def merge_sheets(excel_path):
    # Read sheets
    df2 = pd.read_excel(excel_path, sheet_name="T_MULT", header=[0, 1])
    df3 = pd.read_excel(excel_path, sheet_name="P_N", header=[0, 1])
    df1 = pd.read_excel(excel_path, sheet_name="P_ESP", header=[0, 1])

    # Drop the columns with parent header "Parâmetro Atual" if it exists
    if 'Parâmetro Atual' in df1.columns.get_level_values(0):
        df1 = df1.drop(columns='Parâmetro Atual', level=0)
    if 'Parâmetro Atual' in df2.columns.get_level_values(0):
        df2 = df2.drop(columns='Parâmetro Atual', level=0)
    if 'Parâmetro Atual' in df3.columns.get_level_values(0):
        df3 = df3.drop(columns='Parâmetro Atual', level=0)

    df1 = simplify_columns(df1)
    df2 = simplify_columns(df2)
    df3 = simplify_columns(df3)

    df1_columns_to_drop = ['Descrição', 'Código', 'N°', 'Est.', 'Observação']
    df2_columns_to_drop = ['N°', 'Código', 'Descrição', 'Est.']
    df3_columns_to_drop = ['N°', 'Código', 'Descrição', 'Est.', 'Observação']

    df1 = df1.drop(columns=df1_columns_to_drop)
    df2 = df2.drop(columns=df2_columns_to_drop)
    df3 = df3.drop(columns=df3_columns_to_drop)

    # Rename columns for clarity
    df1.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df2.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df3.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df2.rename(columns={"Mult.": "Mult"}, inplace=True)
    df1.rename(columns={"Qt.": "Segurança"}, inplace=True)

    df1['B1_ZGRUPO'] = df1['B1_ZGRUPO'].apply(round_and_stringify)
    df2['B1_ZGRUPO'] = df2['B1_ZGRUPO'].apply(round_and_stringify)
    df3['B1_ZGRUPO'] = df3['B1_ZGRUPO'].apply(round_and_stringify)

    # Add a temporary column to df3 to help identify its rows after merging
    df3['TempN_Comprar'] = 1

    # Merge all three dataframes on B1_ZGRUPO using outer join
    merged_df = df1.merge(df2, on='B1_ZGRUPO', how='outer')
    merged_df = merged_df.merge(df3, on='B1_ZGRUPO', how='outer')
    merged_df.dropna(subset=['B1_ZGRUPO'], inplace=True)

    # If TempN_Comprar is NaN, it means B1_ZGRUPO is not from df3
    merged_df['N_comprar'] = merged_df['TempN_Comprar'].fillna(0).astype(int)
    merged_df['Mult'] = merged_df['Mult'].fillna(1).astype(int)

    # Drop the temporary column
    merged_df.drop('TempN_Comprar', axis=1, inplace=True)

    # Fill NaN values with appropriate defaults
    merged_df.fillna({"Mult": 1, "Segurança": 0, "N_comprar": 0}, inplace=True)

    # Group by 'B1_ZGRUPO' and get indices of rows with the highest 'seguranca' value
    idx = merged_df.groupby('B1_ZGRUPO')['Segurança'].idxmax()

    # Filter dataframe using these indices
    merged_df = merged_df.loc[idx]

    return merged_df
