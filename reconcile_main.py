import pandas as pd
import streamlit as st #type: ignore
import copy
from mapping_data import state_codes, state_mis_match_mapping
import numpy as np
from reconcile_functions import create_dataframes, update_state_column, add_place_of_origin, format_customer_df, mark_existance_of_invoices, add_values, flag_mismatch, flag_matched_or_check, convert_df_to_csv

def reconcile_main():
    st.title('Reconciliation')
    uploaded_files = st.file_uploader("Upload portal data in Excel & customer data in csv.", accept_multiple_files=True, type=['xlsx', 'xls', 'csv'])

    if uploaded_files:
        current_df, next_df, customer_df = create_dataframes(uploaded_files)

        current_df = current_df.drop(columns=['GSTR1 Filing Date'])
        next_df = next_df.drop(columns=['GSTR1 Filing Date'])

        current_df['Invoice Date'] = pd.to_datetime(current_df['Invoice Date'])
        next_df['Invoice Date'] = pd.to_datetime(next_df['Invoice Date'])
        customer_df['invoice_date'] = pd.to_datetime(customer_df['invoice_date'])

        current_df['Integrated Tax'] = current_df['Integrated Tax'].replace("0", pd.NA)
        current_df['Central Tax'] = current_df['Central Tax'].replace("0", pd.NA)
        current_df['State/UT Tax'] = current_df['State/UT Tax'].replace("0", pd.NA)

        next_df['Integrated Tax'] = next_df['Integrated Tax'].replace(0, pd.NA)
        next_df['Central Tax'] = next_df['Central Tax'].replace(0, pd.NA)
        next_df['State/UT Tax'] = next_df['State/UT Tax'].replace(0, pd.NA)

        # Add needed columns to customer data
        customer_df = add_place_of_origin(customer_df)
        customer_df = update_state_column(customer_df, 'place_of_supply', state_codes)
        customer_df = update_state_column(customer_df, 'place_of_origin', state_codes)

        customer_df = format_customer_df(customer_df)    

        customer_df = customer_df.rename(columns={
            "gstin_supplier": "GSTIN of supplier",
            "supplier_name": "Trade/Legal name",
            "invoice_number": "Invoice number",
            "invoice_date": "Invoice Date",
            "invoice_value": "Invoice Value(₹)",
            "place_of_supply": "Place of supply",
            "taxable_value": "Taxable Value",
            "igst_amount": "Integrated Tax",
            "cgst_amount": "Central Tax",
            "sgst_amount": "State/UT Tax"
        })

        current_df = current_df.replace({None: np.nan})
        next_df = next_df.replace({None: np.nan})
        customer_df = customer_df.replace({None: np.nan})

        customer_df['Integrated Tax'] = customer_df['Integrated Tax'].replace(0, pd.NA)
        customer_df['Central Tax'] = customer_df['Central Tax'].replace(0, pd.NA)
        customer_df['State/UT Tax'] = customer_df['State/UT Tax'].replace(0, pd.NA)

        customer_df['Invoice Value(₹)'] = pd.to_numeric(customer_df['Invoice Value(₹)'], errors='coerce').round(2)
        customer_df['Taxable Value'] = pd.to_numeric(customer_df['Taxable Value'], errors='coerce').round(2)
        customer_df['Integrated Tax'] = pd.to_numeric(customer_df['Integrated Tax'], errors='coerce').round(2)
        customer_df['Central Tax'] = pd.to_numeric(customer_df['Central Tax'], errors='coerce').round(2)
        customer_df['State/UT Tax'] = pd.to_numeric(customer_df['State/UT Tax'], errors='coerce').round(2)

        current_df['Invoice Value(₹)'] = pd.to_numeric(current_df['Invoice Value(₹)'], errors='coerce').round(2)
        current_df['Taxable Value'] = pd.to_numeric(current_df['Taxable Value'], errors='coerce').round(2)
        current_df['Integrated Tax'] = pd.to_numeric(current_df['Integrated Tax'], errors='coerce').round(2)
        current_df['Central Tax'] = pd.to_numeric(current_df['Central Tax'], errors='coerce').round(2)
        current_df['State/UT Tax'] = pd.to_numeric(current_df['State/UT Tax'], errors='coerce').round(2)

        next_df['Invoice Value(₹)'] = pd.to_numeric(next_df['Invoice Value(₹)'], errors='coerce').round(2)
        next_df['Taxable Value'] = pd.to_numeric(next_df['Taxable Value'], errors='coerce').round(2)
        next_df['Integrated Tax'] = pd.to_numeric(next_df['Integrated Tax'], errors='coerce').round(2)
        next_df['Central Tax'] = pd.to_numeric(next_df['Central Tax'], errors='coerce').round(2)
        next_df['State/UT Tax'] = pd.to_numeric(next_df['State/UT Tax'], errors='coerce').round(2)

        result_df = mark_existance_of_invoices(current_df, next_df, customer_df)
        result_df = add_values(result_df, current_df, next_df, customer_df)
        result_df = flag_mismatch(result_df, current_df, next_df, customer_df)
        result_df = flag_matched_or_check(result_df)

        # Convert DataFrame to CSV
        csv_data = convert_df_to_csv(result_df)
        
        # Provide a download link
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name='dataframe.csv',
            mime='text/csv',
        )
