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


def merge_sheets(excel_path):
    # Read sheets
    df2 = pd.read_excel(excel_path, sheet_name="T_MULT")
    df3 = pd.read_excel(excel_path, sheet_name="P_N")
    df1 = pd.read_excel(excel_path, sheet_name="P_ESP")

    df1 = df1.drop(columns='Descrição')
    df2 = df2.drop(columns='Descrição')
    df3 = df3.drop(columns='Descrição')

    # Rename columns for clarity
    df1.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df2.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df3.rename(columns={"cod_agrup": "B1_ZGRUPO"}, inplace=True)
    df2.rename(columns={"Mult.": "mult"}, inplace=True)
    df1.rename(columns={"Qt.": "seguranca"}, inplace=True)

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
    merged_df['n_comprar'] = merged_df['TempN_Comprar'].fillna(0).astype(int)
    merged_df['mult'] = merged_df['mult'].fillna(1).astype(int)

    # Drop the temporary column
    merged_df.drop('TempN_Comprar', axis=1, inplace=True)

    # Fill NaN values with appropriate defaults
    merged_df.fillna({"mult": 1, "seguranca": 0, "n_comprar": 0}, inplace=True)

    # Group by 'B1_ZGRUPO' and 'Descrição' and get indices of rows with the highest 'seguranca' value
    idx = merged_df.groupby('B1_ZGRUPO')['seguranca'].idxmax()

    # Filter dataframe using these indices
    merged_df = merged_df.loc[idx]

    return merged_df
