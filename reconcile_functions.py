import pandas as pd
import streamlit as st #type: ignore
import copy
from mapping_data import state_codes, state_mis_match_mapping
import numpy as np

def create_dataframes(uploaded_files):
    current_df = None
    next_df = None
    customer_df = None

    if uploaded_files:
        for file in uploaded_files:
            # Check file type
            if file.name.endswith(('.xlsx', '.xls')):
                # Read the Excel file and extract sheets
                excel_data = pd.ExcelFile(file)
                
                # Assign 'Current Month Filing' and 'Next Month Filing' sheets to DataFrames
                if 'Current Month Filing' in excel_data.sheet_names:
                    current_df = excel_data.parse('Current Month Filing')
                
                if 'Next Month Filing' in excel_data.sheet_names:
                    next_df = excel_data.parse('Next Month Filing')
            
            elif file.name.endswith('.csv'):
                # Read the CSV file into a DataFrame
                customer_df = pd.read_csv(file)
    
    return current_df, next_df, customer_df

def update_state_column(df, column_name, state_codes):
    # Create a mapping of state-related values to their respective state names
    state_mapping = {}
    for entry in state_codes:
        # Normalize keys: strip leading zeros from code_number and code
        state_mapping[str(entry['State'])] = entry['State']
        state_mapping[entry['code'].lstrip('0')] = entry['State']
        state_mapping[entry['code_number'].lstrip('0')] = entry['State']
    
    # Normalize the input column: strip leading zeros and convert to string
    df[column_name] = df[column_name].astype(str).str.lstrip('0').map(state_mapping).fillna(df[column_name])
    return df

def add_place_of_origin(customer_df):
    # Update place_of_origin where it is NaN or None
    customer_df['place_of_origin'] = customer_df.apply(
        lambda row: int(row['gstin_supplier'][:2]) if pd.isna(row['place_of_origin']) or row['place_of_origin'] is None else row['place_of_origin'],
        axis=1
    )

    # Ensure the column is of integer type (optional, depending on your use case)
    customer_df['place_of_origin'] = customer_df['place_of_origin'].astype(int)

    return customer_df

def format_customer_df(customer_df):
    invoices = customer_df['invoice_number'].unique().tolist()
    updated_customer_rows = []

    for invoice in invoices:
        # Extract invoice-level details
        gstin_supplier = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['gstin_supplier']
        supplier_name = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['supplier_name']
        invoice_number = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['invoice_number']
        invoice_date = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['invoice_date']
        invoice_value = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['invoice_value']
        place_of_supply = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['place_of_supply']
        place_of_origin = customer_df[customer_df['invoice_number'] == invoice].iloc[0]['place_of_origin']
        taxable_value = customer_df[customer_df['invoice_number'] == invoice]['taxable_value'].sum()

        # Extract unique tax rates
        rates = customer_df[customer_df['invoice_number'] == invoice]['tax_rate'].unique().tolist()

        # Initialize tax amounts
        igst_amount = 0
        sgst_amount = 0
        cgst_amount = 0

        # Calculate tax amounts
        for rate in rates:
            taxable_value = customer_df[
                (customer_df['invoice_number'] == invoice) & (customer_df['tax_rate'] == rate)
            ]['taxable_value'].sum()
            
            if place_of_origin == place_of_supply:
                # Intra-state: Split between CGST and SGST
                cgst_amount += taxable_value * rate / 100 / 2
                sgst_amount += taxable_value * rate / 100 / 2
            else:
                # Inter-state: IGST applies
                igst_amount += taxable_value * rate / 100

        # Collect the row as a dictionary
        updated_customer_rows.append({
            'gstin_supplier': gstin_supplier,
            'supplier_name': supplier_name,
            'invoice_number': invoice_number,
            'invoice_date': invoice_date,
            'invoice_value': invoice_value,
            'taxable_value': taxable_value,
            'place_of_supply': place_of_supply,
            'place_of_origin': place_of_origin,
            'igst_amount': igst_amount,
            'sgst_amount': sgst_amount,
            'cgst_amount': cgst_amount
        })

    # Create a new DataFrame from the list of rows
    updated_customer_df = pd.DataFrame(updated_customer_rows)

    return updated_customer_df

def mark_existance_of_invoices(current_df, next_df, customer_df):
    # Combine all invoice numbers from the three dataframes
    unique_invoices = pd.concat([
        current_df['Invoice number'],
        next_df['Invoice number'],
        customer_df['Invoice number']
    ]).unique()

    # Create a new dataframe with unique invoice numbers
    result_df = pd.DataFrame({'Invoice number': unique_invoices})

    # Define a helper function to determine which dataframes an invoice exists in
    def check_existence(invoice):
        exists_in = []
        if invoice in current_df['Invoice number'].values:
            exists_in.append('current_month')
        if invoice in next_df['Invoice number'].values:
            exists_in.append('next_month')
        if invoice in customer_df['Invoice number'].values:
            exists_in.append('customer_data')
        return exists_in

    # Add the 'exists_in' column by applying the helper function
    result_df['exists_in'] = result_df['Invoice number'].apply(check_existence)

    return result_df

def add_values(result_df, current_df, next_df, customer_df):
    # Define the columns to be added
    columns_to_add = [
        'GSTIN of supplier', 'Trade/Legal name',
        'Invoice Date', 'Invoice Value(₹)', 'Place of supply',
        'Taxable Value', 'Integrated Tax', 'Central Tax', 'State/UT Tax'
    ]

    # Initialize the new columns in result_df with None
    for column in columns_to_add:
        result_df[column] = None

    # Helper function to get values from the dataframes based on the priority
    def get_values(invoice):
        exists_in_values = result_df.loc[result_df['Invoice number'] == invoice, 'exists_in'].values
        if len(exists_in_values) == 0:
            return {}  # Return an empty dictionary if no match found
        exists_in = exists_in_values[0]
        for df, name in zip([current_df, next_df, customer_df], ['current_month', 'next_month', 'customer_data']):
            if name in exists_in:
                # Return the row as a dictionary of column values
                matched_rows = df[df['Invoice number'] == invoice]
                if not matched_rows.empty:
                    return matched_rows.iloc[0].to_dict()
        return {}

    # Iterate over the rows of result_df
    for index, row in result_df.iterrows():
        invoice = row['Invoice number']
        values = get_values(invoice)
        for column in columns_to_add:
            if column in values:
                result_df.at[index, column] = values[column]

    return result_df

def flag_mismatch(result_df, current_df, next_df, customer_df):
    # Initialize a new column for mismatches
    result_df['mismatches'] = None

    # Define a helper function to compare row values across dataframes
    def find_mismatches(invoice):
        mismatched_columns = []
        # Get the values for the invoice from each dataframe
        current_row = current_df[current_df['Invoice number'] == invoice].iloc[0] if invoice in current_df['Invoice number'].values else None
        next_row = next_df[next_df['Invoice number'] == invoice].iloc[0] if invoice in next_df['Invoice number'].values else None
        customer_row = customer_df[customer_df['Invoice number'] == invoice].iloc[0] if invoice in customer_df['Invoice number'].values else None

        # Compare each column's value in result_df with the dataframes
        for column in current_df.columns:
            result_values = result_df.loc[result_df['Invoice number'] == invoice, column].values
            if len(result_values) == 0:
                continue  # Skip if no matching rows in result_df
            result_value = result_values[0]

            # Compare with current_df value if exists
            if current_row is not None and column in current_row:
                if not pd.isna(current_row[column]) and current_row[column] != result_value:
                    mismatched_columns.append((column, 'current_month'))

            # Compare with next_df value if exists
            if next_row is not None and column in next_row:
                if not pd.isna(next_row[column]) and next_row[column] != result_value:
                    mismatched_columns.append((column, 'next_month'))

            # Compare with customer_df value if exists
            if customer_row is not None and column in customer_row:
                if not pd.isna(customer_row[column]) and customer_row[column] != result_value:
                    mismatched_columns.append((column, 'customer_data'))

        return mismatched_columns

    # Iterate over the rows of result_df
    for index, row in result_df.iterrows():
        invoice = row['Invoice number']
        mismatches = find_mismatches(invoice)
        if mismatches:
            result_df.at[index, 'mismatches'] = mismatches

    return result_df

def flag_matched_or_check(result_df):
    # Initialize the new column for approval status
    result_df['match_status'] = None

    # Define the logic for 'Matched' and 'Check'
    def determine_status(row):
        # Check if 'exists_in' contains exactly 'customer_date' and 'current_month'
        exists_in_condition = sorted(row['exists_in']) == ['current_month', 'customer_data']

        # Check if 'mismatches' is None (no mismatches)
        no_mismatch_condition = row['mismatches'] is None

        # Return 'Matched' if both conditions are true, otherwise 'Check'
        if exists_in_condition and no_mismatch_condition:
            return 'Matched'
        else:
            return 'Check'

    # Apply the logic to each row in the dataframe
    result_df['match_status'] = result_df.apply(determine_status, axis=1)

    return result_df

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')





# 
