import pandas as pd
import numpy as np
import math
import logging

# Get a logger
logger = logging.getLogger(__name__)

def calculate_grades(data_frame):
    """
    Calculate grades for sales data based on defined rules.
    
    Parameters:
    - data_frame (DataFrame): Sales data where columns include date periods and rows represent individual sales.

    Returns:
    - DataFrame: Data with an additional 'grade' column indicating the calculated grade.

    Rules for Grade Calculation:
    - Grade 3: Sales recorded in all of the last three months.
    - Grade 2: Sales in at least two of the last three months.
    - Grade 1: Sales in at least three of the last five months, and no zero sales for two consecutive months.
    - Default Grade 0: If none of the above conditions is met.
    """
    # Extract columns that are date periods.
    date_columns = [col for col in data_frame.columns if isinstance(col, pd._libs.tslibs.period.Period)]
    date_df = data_frame[date_columns]

    # Rule 1: Check sales in all of the last three months.
    last_three_months = date_df.iloc[:, 2:5]
    rule_1_condition = (last_three_months != 0).all(axis=1)

    # Rule 2: Check sales in at least two of the last three months.
    last_three_months_check = (date_df.iloc[:, 2:5] != 0).sum(axis=1).ge(2)
    rule_2_condition = last_three_months_check

    # Rule 3: Check sales in at least three of the last five months and no zero sales for two consecutive months.
    three_of_five_months_check = (date_df != 0).sum(axis=1).ge(3)
    rolled_sum = date_df.T.rolling(window=2).sum().T
    no_consecutive_zeros = (rolled_sum != 0).all(axis=1)
    rule_3_condition = three_of_five_months_check & no_consecutive_zeros
    
    # Create conditions list for grade assignment.
    conditions = [
        rule_1_condition,
        ~rule_1_condition & rule_2_condition,
        ~rule_1_condition & ~rule_2_condition & rule_3_condition
    ]

    # Assign grades based on conditions.
    grades = [3, 2, 1]
    data_frame['grade'] = np.select(conditions, grades, default=0)

    return data_frame

def calculate_min_max_columns(data_frame):
    """
    Calculate the 'min' and 'max' columns for the given data frame based on predefined rules.
    
    Parameters:
    - data_frame (DataFrame): The input data frame for which the 'min' and 'max' columns will be calculated.

    Returns:
    - DataFrame: The updated data frame with the 'min' and 'max' columns.
    """

    logger.info("Starting min-max column calculations.")

    # Define the internal function for 'min' column calculation
    def calculate_min_stock(row):
        if row['grade'] == 3:
            daily_avg = row['avg_last_three_months'] / 30
        elif row['grade'] == 2:
            daily_avg = row['avg_last_three_months'] / 20
        else:
            daily_avg = 0

        computed_stock = daily_avg * 10
        return math.ceil(max(computed_stock, row['seguranca']))

    data_frame['min'] = data_frame.apply(calculate_min_stock, axis=1)

    # Define the internal function for 'max' column calculation
    def calculate_max_stock(row):
        if row['avg_last_three_months'] <= 1:
            return row['min']
        elif row['grade'] == 3:
            return math.ceil(row['min'] + row['avg_last_three_months'])
        elif row['grade'] == 2:
            return math.ceil(row['min'] + (0.5 * row['avg_last_two_months']))
        else:
            return 0

    data_frame['max'] = data_frame.apply(
        lambda row: row['min'] if calculate_max_stock(row) <= row['min'] else calculate_max_stock(row), 
        axis=1
    )

    logger.info("Finished min-max column calculations.")
    return data_frame

def calculate_stock_suggestion(data_frame):
    """
    Calculate the 'stock_suggestion' column based on the given rules.

    Parameters:
    - data_frame (pd.DataFrame): The input data frame.

    Returns:
    - pd.DataFrame: Data frame with the 'stock_suggestion' column.
    """

    logger.info("Calculating stock suggestions.")

    def suggestion(row):
        """Calculate the stock suggestion based on the rules provided."""
        # Return 0 if N_Comprar value is 1
        if row['N_Comprar'] == 1:
            return 0
        
        # Calculate the sum of B2_QATU and QRE
        sum_val = row['B2_QATU'] + row['QRE']
        
        # Return 0 if the sum exceeds or equals the min value
        if sum_val >= row['min']:
            return 0
        
        # Calculate suggestion based on the difference between max value and the sum
        return row['max'] - sum_val

    data_frame['stock_suggestion'] = data_frame.apply(suggestion, axis=1)
    logger.info("Stock suggestions calculated.")
    return data_frame