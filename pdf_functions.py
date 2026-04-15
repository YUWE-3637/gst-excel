import streamlit as st #type:ignore
import requests #type: ignore
import pandas as pd
import numpy as np
from datetime import datetime
import zipfile
from io import BytesIO 
import io  # Import io module
import random
import string
from datetime import datetime, timedelta
import json
from elasticsearch import Elasticsearch  # type: ignore
from typing import Tuple, Dict, Optional
import time
import copy


def file_to_response_json_anthropod(file):
    # API Configuration
    api_url = "https://uncover.anthropod.in/api/v1/doc-extraction/"
    bearer_token = "Pz6@D8wCq!bKxLz2#Vf3Uj$RyHnKmT1*Ij0&Fa9QmXtZoP@Vq5WsLuDd?9ZyVe3TgW"
    
    try:
        # Open the file in binary mode
        files = {'file': file}

        # Add the Authorization header
        headers = {
            "Authorization": f"Bearer {bearer_token}"
        }

        # id = generate_key(100)
        # st.write(id)
        # st.print(id)
        
        # Payload data
        data = {"id": file.name}
        
        # Send the POST request
        print("Sending request to:", api_url)
        response = requests.post(api_url, files=files, headers=headers, data=data, timeout=180)
        
        # Log the response details
        print("Status Code:", response.status_code)
        print("Response Headers:", response.headers)
        if str(response.status_code) in ('200', '435', '436'):
            print("Response:", response.json())
            return response.json(), response.status_code
        else:
            print(f"Error: Received status code {response.status_code}")
            print("Response Text:", response.text)
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        return None, None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None, None

def file_to_response_json_santa_fe(file):
    # API Configuration
    api_url = "https://sfrpl.in/invoice/extract_file"
    bearer_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxNjc3NmY3MjJmMDcwNzI4NzMwODA1ZThlMTUzNjE4NjU1OTdmZTNkOWUxMTAxZDU5NjA4MmEyNDE0ZjU0YWNiIn0.vRKYhZ6SIJ0ajUkiqVaaPdMXClvGkUMhRY6krRA2Qzg"
    
    try:
        # Open the PDF file in binary mode
        files = {'invoice_file': file}

        # Add the Authorization header
        headers = {
            "Authorization": f"Bearer {bearer_token}"
        }
        
        # Send the POST request
        print("Sending request to:", api_url)
        response = requests.post(api_url, files=files, headers=headers, timeout=3000)
        
        # Log the response details
        print("Status Code:", response.status_code)
        print("Response Headers:", response.headers)
        if str(response.status_code) in ('200','435','436'):
            # response_json = response.json()
            print("Response:", response.json())
            return response.json(), response.status_code
        else:
            print(f"Error: Received status code {response.status_code}")
            print("Response Text:", response.text)
            return None, None
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {str(e)}")
        return None, None
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None, None

def file_to_response_json_affine(file):
    # Define the API endpoint and credentials
    endpoint = "https://jupiter.origamis.ai:9001/zolvit/processInvoice"
    access_token = "gAAAAABnhSlPQIiqO3d5qFFLF0qTVl1YujXLFeusqI_GSJhHBzPmuLpcUcpB1Z0056TUt-w5jyZsRwBE9NyfL7xLK0AITMJDUsK19RpcRAKi8obPXbLovDGP1DxeAE_4Js8QxFIX0iP8WdaQO66IG8hpEdZp0hl5Gg=="
    email = "Zolvit@origamis.ai"

    try:
        # Prepare the multipart form data
        files = {
            "file": (file.name, file, "application/pdf")
        }

        # Prepare the form data
        data = {
            "access_token": access_token,
            "email": email
        }

        # Make the POST request with a timeout of 60 seconds
        try:
            response = requests.post(endpoint, data=data, files=files, timeout=60)
        except requests.exceptions.Timeout:
            # Custom code and message for timeout
            return 408, [], "Request timed out after 60 seconds."

        # Initialize variables to return
        status_code = response.status_code
        invoice_data = []
        status = ""

        # Check for HTTP request success
        if status_code == 200:
            # Parse the response JSON
            response_json = response.json()

            # Extract specific details
            invoice_list = response_json.get("invoice_list", [])
            if invoice_list:
                # Extract the first invoice's details
                first_invoice = invoice_list[0]
                status = first_invoice.get("status", "Unknown")
                invoice_data = first_invoice.get("invoice_data", [])

        elif status_code in (435,436):
            response_json = response.json()

            invoice_data = response_json.get('detail')
            status = status_code

        else:
            # In case of failure, add an error status
            status = f"API call failed with status code {status_code}: {response.text}"

        return status_code, invoice_data, status

    except FileNotFoundError:
        return None, None, "Error: The file was not found."
    except Exception as e:
        return None, None, f"An error occurred: {str(e)}"



def extract_required_data_from_anthropod(input_json):
    extracted_data = {}
    
    # Extracting required sections from the JSON
    if "doc_analytics" in input_json and "invoices" in input_json["doc_analytics"]:
        invoices = input_json["doc_analytics"]["invoices"]
        if invoices:
            invoice_data = invoices[0]  # Assuming we need the first invoice
            
            extracted_data["Invoice Details"] = invoice_data.get("Invoice Details", {})
            extracted_data["Line Items"] = invoice_data.get("Line Items", [])
            extracted_data["Total Summary"] = invoice_data.get("Total Summary", {})
    
    return extracted_data

def response_json_to_dataframes(response_json, api):

    try:
        # Extracting DataFrames
        invoice_details = response_json.get("Invoice Details", {})
        invoice_df = pd.DataFrame([invoice_details])

        line_items = response_json.get("Line Items", [])
        line_items_df = pd.DataFrame(line_items)

        # Check if the column exists and rename it
        if "rate_per_item" in line_items_df.columns:
            line_items_df.rename(columns={"rate_per_item": "rate_per_item_after_discount"}, inplace=True)

        total_summary = response_json.get("Total Summary", {})
        total_summary_df = pd.DataFrame([total_summary])

        return invoice_df, line_items_df, total_summary_df
    except:
        return None, None, None



def fill_line_items_from_summary(line_items_df, total_summary_df):

    if not pd.isna(line_items_df['taxable_value']).any():
        try:
            line_items_df['taxable_value'] = pd.to_numeric(line_items_df['taxable_value'])
            total_summary_df['total_taxable_value'] = pd.to_numeric(total_summary_df['total_taxable_value'])
        except:
            return line_items_df
        

    if pd.isna(line_items_df['sgst_amount']).all() and pd.isna(line_items_df['cgst_amount']).all() and pd.isna(line_items_df['igst_amount']).all() and pd.isna(line_items_df['sgst_rate']).all() and pd.isna(line_items_df['cgst_rate']).all() and pd.isna(line_items_df['igst_rate']).all():
        
        # SGST
        if not pd.isna(total_summary_df['total_sgst_amount']).any():

            try:
                total_summary_df['total_sgst_amount'] = pd.to_numeric(total_summary_df['total_sgst_amount'])
            except:
                return line_items_df

            for index, row in line_items_df.iterrows():
                ratio = row['taxable_value'] / total_summary_df['total_taxable_value'].sum()
                line_items_df.at[index, 'sgst_amount'] = total_summary_df['total_sgst_amount'].sum() * ratio

        # CGST
        if not pd.isna(total_summary_df['total_cgst_amount']).any():

            try:
                total_summary_df['total_cgst_amount'] = pd.to_numeric(total_summary_df['total_cgst_amount'])
            except:
                return line_items_df

            for index, row in line_items_df.iterrows():
                ratio = row['taxable_value'] / total_summary_df['total_taxable_value'].sum()
                line_items_df.at[index, 'cgst_amount'] = total_summary_df['total_cgst_amount'].sum() * ratio

        # IGST  
        if not pd.isna(total_summary_df['total_igst_amount']).any():

            try:
                total_summary_df['total_igst_amount'] = pd.to_numeric(total_summary_df['total_igst_amount'])
            except:
                return line_items_df

            for index, row in line_items_df.iterrows():
                ratio = row['taxable_value'] / total_summary_df['total_taxable_value'].sum()
                line_items_df.at[index, 'igst_amount'] = total_summary_df['total_igst_amount'].sum() * ratio

        # Total tax amount
        if not pd.isna(total_summary_df['total_tax_amount']).any():

            try:
                total_summary_df['total_tax_amount'] = pd.to_numeric(total_summary_df['total_tax_amount'])
            except:
                return line_items_df

            for index, row in line_items_df.iterrows():
                ratio = row['taxable_value'] / total_summary_df['total_taxable_value'].sum()
                line_items_df.at[index, 'tax_amount'] = total_summary_df['total_tax_amount'].sum() * ratio

    return line_items_df

def missing_value_check(invoice_df, line_items_df, total_summary_df):

    invoice_fields = ['invoice_number','invoice_date','place_of_origin','gstin_supplier','supplier_name']

    # if (
    #     pd.isna(invoice_df['invoice_number']).any() or
    #     pd.isna(invoice_df['invoice_date']).any() or
    #     pd.isna(invoice_df['place_of_origin']).any() or
    #     pd.isna(invoice_df['gstin_supplier']).any() or
    #     pd.isna(invoice_df['supplier_name']).any()
    # ):
    #     return False, 'Missing Values', 'Fields missing in invoice details.'
    if pd.isna(invoice_df['invoice_number']).any():
        return False, 'Missing Values in Invoice Details', 'invoice_number'
    if pd.isna(invoice_df['invoice_date']).any():
        return False, 'Missing Values in Invoice Details', 'invoice_date'
    if pd.isna(invoice_df['place_of_origin']).any():
        return False, 'Missing Values in Invoice Details', 'place_of_origin'
    if pd.isna(invoice_df['gstin_supplier']).any():
        return False, 'Missing Values in Invoice Details', 'gstin_supplier'
    if pd.isna(invoice_df['supplier_name']).any():
        return False, 'Missing Values in Invoice Details', 'supplier_name'

    state_codes = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "23", "24",
                   "25", "26", "27", "29", "30", "31", "32", "33", "34", "35", "36", "37", "38", "97", "1","2","3","4","5","6","7","8","9"]

    # Validate 'place_of_origin' (must always have a valid value)
    invalid_origin = ~invoice_df['place_of_origin'].astype(str).isin(state_codes)

    # Validate 'place_of_supply' only if there is a value present
    invalid_supply = invoice_df['place_of_supply'].notna() & ~invoice_df['place_of_supply'].astype(str).isin(state_codes)

    # Check for any invalid entries
    if invalid_origin.any():
        return False, 'Not in options.', 'place_of_origin'

    if invalid_supply.any():
        return False, 'Not in options.', 'place_of_supply'
    
    taxable_value_group_condition = (
        (pd.isna(line_items_df['quantity']).any() or pd.isna(line_items_df['rate_per_item_after_discount']).any()) and
        pd.isna(line_items_df['taxable_value']).any()
    )
    
    tax_group_condition = (
        pd.isna(line_items_df['tax_rate']).any() and
        pd.isna(line_items_df['tax_amount']).any() and 
        pd.isna(line_items_df['igst_rate']).any() and
        pd.isna(line_items_df['igst_amount']).any() and
        (pd.isna(line_items_df['sgst_rate']).any() or pd.isna(line_items_df['cgst_rate']).any()) and
        (pd.isna(line_items_df['sgst_amount']).any() or pd.isna(line_items_df['cgst_amount']).any())
    )

    final_amount_group_condition = (
        pd.isna(line_items_df['final_amount']).any()
    )

    missing_groups = 0

    if taxable_value_group_condition:
        missing_groups += 1

    if tax_group_condition:
        missing_groups += 1

    if final_amount_group_condition:
        missing_groups += 1

    if missing_groups > 1:
        return False, 'Missing Values', 'Line items'

    
    # taxable value, tax amount related, invoice value
    total_condition1 = (pd.isna(total_summary_df['total_taxable_value']) | pd.isna(total_summary_df['total_invoice_value'])).any()
    total_condition2 = (pd.isna(total_summary_df['total_tax_amount']) | pd.isna(total_summary_df['total_invoice_value'])).any()
    total_condition3 = (pd.isna(total_summary_df['total_tax_amount']) | pd.isna(total_summary_df['total_taxable_value'])).any()
    total_condition4 = ((pd.isna(total_summary_df['total_sgst_amount']) & pd.isna(total_summary_df['total_cgst_amount']) & pd.isna(total_summary_df['total_igst_amount'])) | pd.isna(total_summary_df['total_invoice_value'])).any()
    total_condition5 = ((pd.isna(total_summary_df['total_sgst_amount']) & pd.isna(total_summary_df['total_cgst_amount']) & pd.isna(total_summary_df['total_igst_amount'])) | pd.isna(total_summary_df['total_taxable_value'])).any()

    if (total_condition1 and total_condition2 and total_condition3 and total_condition4 and total_condition5):
        return False, 'Missing Values', 'Not Enough fields in summary details.'   

    # Allowed tax rates
    tax_rates = ['0', '2.5', '5', '6', '9', '12', '18', '24', '3', '1.5', '28', '14']
    tax_rates_float = [float(rate) for rate in tax_rates]  # Normalize tax rates to float

    # Columns to validate
    columns_to_check = ['igst_rate', 'sgst_rate', 'cgst_rate', 'tax_rate']

    for index, row in line_items_df.iterrows():
        for column in columns_to_check:
            value = row[column]
            
            # Skip validation for null, NaN, or None values
            if pd.isna(value) or value is None:
                continue
            
            try:
                # Convert the column value to float
                tax_rate_float = float(value)
            except ValueError:
                return False, column, f'Invalid {column} format: {value}'
            
            # Check if the tax rate is in the allowed list
            if tax_rate_float not in tax_rates_float:
                return False, column, f'{column} in line items: {value} is not valid.'


    return True, 'No Missing Values', 'Proceed with next check.'

def data_type_check(invoice_df, line_items_df, total_summary_df):
    try:
        # Replace None with np.nan to standardize missing value handling
        invoice_df.replace({None: np.nan}, inplace=True)
        line_items_df.replace({None: np.nan}, inplace=True)
        total_summary_df.replace({None: np.nan}, inplace=True)
        
        # Validate and convert date columns in invoice_df
        if 'invoice_date' in invoice_df.columns:
            mask = ~invoice_df['invoice_date'].isna()
            invoice_df.loc[mask, 'invoice_date'] = pd.to_datetime(invoice_df.loc[mask, 'invoice_date'], format="%d-%b-%Y", errors='coerce')
            if invoice_df.loc[mask, 'invoice_date'].isna().any():
                invalid_rows = invoice_df[mask & invoice_df['invoice_date'].isna()]
                raise ValueError(f"Invalid date values found in 'invoice_date':\n{invalid_rows}")
        
        # Validate and convert numeric columns in invoice_df
        numeric_cols_invoice = ['taxable_value', 'invoice_value', 'tax_amount', 'place_of_supply', 'place_of_origin']
        for col in numeric_cols_invoice:
            if col in invoice_df.columns:
                mask = ~invoice_df[col].isna()
                invoice_df.loc[mask, col] = pd.to_numeric(invoice_df.loc[mask, col], errors='coerce')
                if invoice_df.loc[mask, col].isna().any():
                    invalid_rows = invoice_df[mask & invoice_df[col].isna()]
                    raise ValueError(f"Invalid numeric values found in '{col}' of invoice_df:\n{invalid_rows}")
        
        # Validate and convert numeric columns in line_items_df
        numeric_cols_items = ['quantity', 'rate_per_item_after_discount', 'taxable_value',
                              'sgst_amount', 'cgst_amount', 'igst_amount', 'tax_amount',
                              'tax_rate', 'final_amount', 'sgst_rate', 'cgst_rate', 'igst_rate', 'discount']
        for col in numeric_cols_items:
            if col in line_items_df.columns:
                mask = ~line_items_df[col].isna()
                line_items_df.loc[mask, col] = pd.to_numeric(line_items_df.loc[mask, col], errors='coerce')
                if line_items_df.loc[mask, col].isna().any():
                    invalid_rows = line_items_df[mask & line_items_df[col].isna()]
                    raise ValueError(f"Invalid numeric values found in '{col}' of line_items_df:\n{invalid_rows}")
        
        # Validate and convert numeric columns in total_summary_df
        numeric_cols_summary = ['total_taxable_value', 'total_invoice_value', 'total_tax_amount', 
                                 'total_cgst_amount', 'total_sgst_amount', 'total_igst_amount', 'Addition', 'Deduction']
        for col in numeric_cols_summary:
            if col in total_summary_df.columns:
                mask = ~total_summary_df[col].isna()
                total_summary_df.loc[mask, col] = pd.to_numeric(total_summary_df.loc[mask, col], errors='coerce')
                if total_summary_df.loc[mask, col].isna().any():
                    invalid_rows = total_summary_df[mask & total_summary_df[col].isna()]
                    raise ValueError(f"Invalid numeric values found in '{col}' of total_summary_df:\n{invalid_rows}")
        
        return True, "Data type validation", "All data types are valid and converted successfully."
    
    except Exception as e:
        return False, "Data type mismatch", f"Error during validation: {str(e)}"

def fill_taxable_from_qty_rate(df):
    for index, row in df.iterrows():
        # Use pd.isnull() to check for missing values
        if pd.isnull(row['taxable_value']) and not pd.isnull(row['quantity']) and not pd.isnull(row['rate_per_item_after_discount']):
            df.at[index, 'taxable_value'] = row['quantity'] * row['rate_per_item_after_discount']

    return df

def relation_check(invoice_df, line_items_df, total_summary_df):

    # Replace 0 values with NaN in the specified columns of invoice_df
    invoice_df['tax_amount'] = invoice_df['tax_amount'].replace(0, np.nan)

    # Replace 0 values with NaN in the specified columns of line_items_df
    columns_to_replace_line_items = [
        'tax_rate', 'tax_amount', 'igst_rate', 'sgst_rate', 'cgst_rate',
        'igst_amount', 'sgst_amount', 'cgst_amount'
    ]
    line_items_df[columns_to_replace_line_items] = line_items_df[columns_to_replace_line_items].replace(0, np.nan)

    # Replace 0 values with NaN in the specified columns of total_summary_df
    columns_to_replace_total_summary = [
        'total_tax_amount', 'total_igst_amount', 'total_sgst_amount', 'total_cgst_amount'
    ]
    total_summary_df[columns_to_replace_total_summary] = total_summary_df[columns_to_replace_total_summary].replace(0, np.nan)


    # Perform relation check
    for id, row in invoice_df.iterrows():
        # Check if all values are numeric (not NaN)
        if pd.notna(row['invoice_value']) and pd.notna(row['taxable_value']) and pd.notna(row['tax_amount']):
            if not np.isclose(row['invoice_value'], row['taxable_value'] + row['tax_amount'], atol=1, rtol=0):
                return False, "Relation check", "Failed relation check in invoice details."
            

    '''
    
    Total Summary
    
    '''
    # In summary - 'total_taxable_value', 'total_invoice_value', 'total_tax_amount'.   
    if not total_summary_df[['total_taxable_value', 'total_invoice_value', 'total_tax_amount']].isnull().any().any():
        for id, row in total_summary_df.iterrows():
            if not np.isclose(row['total_invoice_value'], row['total_taxable_value'] + row['total_tax_amount'], atol=1, rtol=0):
                return False, "Relation check", "In summary - 'total_taxable_value', 'total_invoice_value', 'total_tax_amount'."
            
    # In summary - 'total_taxable_value', 'total_invoice_value', 'total_igst_amount'.
    if not total_summary_df[['total_taxable_value', 'total_invoice_value', 'total_igst_amount']].isnull().any().any():
        for id, row in total_summary_df.iterrows():
            if not np.isclose(row['total_invoice_value'], row['total_taxable_value'] + row['total_igst_amount'], atol=1, rtol=0):
                return False, "Relation check", "In summary - 'total_taxable_value', 'total_invoice_value', 'total_igst_amount'."

    # In summary - 'total_taxable_value', 'total_invoice_value', 'total_sgst_amount', 'total_cgst_amount'. 
    if not total_summary_df[['total_taxable_value', 'total_invoice_value', 'total_sgst_amount', 'total_cgst_amount']].isnull().any().any():
        for id, row in total_summary_df.iterrows():
            if not np.isclose(row['total_invoice_value'], row['total_taxable_value'] + (row['total_sgst_amount']+row['total_cgst_amount']), atol=1, rtol=0):
                return False, "Relation check", "In summary - 'total_taxable_value', 'total_invoice_value', 'total_sgst_amount', 'total_cgst_amount'."
            
    # In summary - 'total_taxable_value', 'total_invoice_value', 'total_sgst_amount', 'total_cgst_amount', 'total_igst_amount', 'total_tax_amount'.
    if not total_summary_df[['total_taxable_value', 'total_invoice_value']].isnull().any().any() and total_summary_df[['total_sgst_amount', 'total_cgst_amount', 'total_igst_amount', 'total_tax_amount']].isnull().all().all():
        for id, row in total_summary_df.iterrows():
            if not np.isclose(row['total_invoice_value'], row['total_taxable_value'], atol=1, rtol=0):
                return False, "Relation check", "In summary - 'total_taxable_value', 'total_invoice_value', 'total_sgst_amount', 'total_cgst_amount', 'total_igst_amount', 'total_tax_amount'."
            
    # In summary - 'total_tax_amount', 'total_sgst_amount', 'total_cgst_amount', 'total_igst_amount'.
    if not total_summary_df[['total_tax_amount', 'total_sgst_amount', 'total_cgst_amount', 'total_igst_amount']].isnull().any().any():
        for id, row in total_summary_df.iterrows():
            if not np.isclose(row['total_tax_amount'], (row['total_sgst_amount']+row['total_cgst_amount']+row['total_igst_amount']), atol=1, rtol=0):
                return False, "Relation check", "In summary - 'total_tax_amount', 'total_sgst_amount', 'total_cgst_amount', 'total_igst_amount'."
            
    # If IGST is present, CGST should not be positive
    if pd.notna(row['total_igst_amount']) and row['total_igst_amount'] != 0:  
        if pd.notna(row['total_sgst_amount']) and row['total_sgst_amount'] > 0:
            return False, "IGST Conflict", "If IGST is present, SGST should not be positive."
        if pd.notna(row['total_cgst_amount']) and row['total_cgst_amount'] > 0:
            return False, "IGST Conflict", "If IGST is present, CGST should not be positive."
        
    # If SGST is present, CGST should be positive
    if pd.notna(row['total_sgst_amount']) and row['total_sgst_amount'] != 0:  
        if pd.isna(row['total_cgst_amount']) or row['total_cgst_amount'] == 0:
            return False, "SGST Conflict", "If SGST is present, CGST should be positive."

    # If CGST is present, SGST should be positive  
    if pd.notna(row['total_cgst_amount']) and row['total_cgst_amount'] != 0:  
        if pd.isna(row['total_sgst_amount']) or row['total_sgst_amount'] == 0:
            return False, "CGST Conflict", "If CGST is present, SGST should be positive."

    '''
    
    Rate
    
    '''

    if not line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['rate_per_item_after_discount'], row['taxable_value'] / row['quantity'], atol=1, rtol=0):
                return False, 'Relation check', 'Rate.'
    
    # if not line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value', 'final_amount']].isnull().any().any():
    #     for id, row in line_items_df.iterrows():
    #         if not np.isclose(row['rate_per_item_after_discount'], row['taxable_value'] / row['quantity'], atol=1, rtol=0) and not np.isclose(row['rate_per_item_after_discount'], row['final_amount'] / row['quantity'], atol=1, rtol=0):
    #             return False, 'Relation check', 'Rate.1'
                
    # rate = False

    # if not line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value']].isnull().any().any():
    #     for id, row in line_items_df.iterrows():
    #         if not np.isclose(row['rate_per_item_after_discount'], row['taxable_value'] / row['quantity'], atol=1, rtol=0):
    #             rate = False
    #         else:
    #             rate = True

    # if rate == False:
    #     if not line_items_df[['rate_per_item_after_discount', 'quantity', 'final_amount']].isnull().any().any():
    #         for id, row in line_items_df.iterrows():
    #             if not np.isclose(row['rate_per_item_after_discount'], row['final_amount'] / row['quantity'], atol=1, rtol=0):
    #                 rate = False
    #             else:
    #                 rate = True

    # if rate == False:
    #     return False, 'Relation check', 'Rate.2'
            

    '''
    
    Quantity
    
    '''
    if not line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value']].isnull().any().any() and not (line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value']] == 0).any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['quantity'], row['taxable_value'] / row['rate_per_item_after_discount'], atol=1, rtol=0):
                return False, 'Relation check', 'Quantity.'

            


    '''
    
    Taxable Value
    
    '''
    # Taxable Value - from rate & quantity
    if not line_items_df[['rate_per_item_after_discount', 'quantity', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['quantity'] * row['rate_per_item_after_discount'], atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from rate & quantity.'

    # Taxable Value - from final amount & tax amount
    if not line_items_df[['final_amount', 'tax_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] - row['tax_amount'], atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount & tax amount.'
            
    # Taxable Value - from final amount & igst_amount        
    if not line_items_df[['final_amount', 'igst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] - row['igst_amount'], atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount & igst_amount.'
            
    # Taxable Value - from final amount, sgst_amount & cgst_amount         
    if not line_items_df[['final_amount', 'sgst_amount', 'cgst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] - row['sgst_amount'] - row['cgst_amount'], atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount, sgst_amount & cgst_amount.'
            

    # Taxable Value - from final amount & tax rate
    if not line_items_df[['final_amount', 'tax_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] * (100/(100+row['tax_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount & tax rate.'
            
    # Taxable Value - from final amount & igst_rate
    if not line_items_df[['final_amount', 'igst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] * (100/(100+row['igst_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount & igst_rate.'
            
    # Taxable Value - from final amount, sgst_rate & cgst_rate
    if not line_items_df[['final_amount', 'sgst_rate', 'cgst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['final_amount'] * (100/(100+(row['cgst_rate']+row['sgst_rate']))), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from final amount, sgst_rate & cgst_rate.'
            
    
    # Taxable Value - from tax amount & tax rate
    if not line_items_df[['tax_amount', 'tax_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['tax_amount'] * (100/row['tax_rate']), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from tax amount & tax rate.'
            
    # Taxable Value - from igst_amount & tax rate
    if not line_items_df[['igst_amount', 'tax_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['igst_amount'] * (100/row['tax_rate']), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from igst_amount & tax rate.'
            
    # Taxable Value - from tax amount & igst_rate
    if not line_items_df[['tax_amount', 'igst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['tax_amount'] * (100/row['igst_rate']), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from tax amount & igst_rate.'
            
    # Taxable Value - from igst_amount & igst_rate
    if not line_items_df[['igst_amount', 'igst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['igst_amount'] * (100/row['igst_rate']), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from igst_amount & igst_rate.'
            
    # Taxable Value - from tax amount, sgst_rate & cgst_rate
    if not line_items_df[['tax_amount', 'sgst_rate','cgst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], row['tax_amount'] * (100/(row['sgst_rate']+row['cgst_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from tax amount, sgst_rate & cgst_rate.'
            
    # Taxable Value - from sgst_amount, cgst_amount & tax rate
    if not line_items_df[['sgst_amount','cgst_amount', 'tax_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], (row['sgst_amount']+row['cgst_amount']) * (100/row['tax_rate']), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from sgst_amount, cgst_amount & tax rate.'
            
    # Taxable Value - from sgst_amount, cgst_amount, sgst_rate & cgst_rate
    if not line_items_df[['sgst_amount','cgst_amount', 'sgst_rate','cgst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['taxable_value'], (row['sgst_amount']+row['cgst_amount']) * (100/(row['sgst_rate']+row['cgst_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Taxable Value - from sgst_amount, cgst_amount, sgst_rate & cgst_rate.'
            


    '''
    
    Tax Amount
    
    '''
    # Tax amount - from final amount & taxable value
    if not line_items_df[['final_amount', 'tax_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['tax_amount'], row['final_amount'] - row['taxable_value'], atol=1, rtol=0):
                return False, 'Relation check', 'Tax amount - from final amount & taxable value.'
            
    # igst_amount - from final amount & taxable value
    if not line_items_df[['final_amount', 'igst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['igst_amount'], row['final_amount'] - row['taxable_value'], atol=1, rtol=0):
                return False, 'Relation check', 'igst_amount - from final amount & taxable value.'
            
    # sgst_amount & cgst_amount - from final amount & taxable value
    if not line_items_df[['final_amount', 'sgst_amount','cgst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose((row['sgst_amount']+row['cgst_amount']), row['final_amount'] - row['taxable_value'], atol=1, rtol=0):
                return False, 'Relation check', 'sgst_amount & cgst_amount - from final amount & taxable value.'
            
    # Tax amount - from final amount & tax_rate
    if not line_items_df[['final_amount', 'tax_rate', 'tax_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['tax_amount'], row['final_amount'] - ((row['final_amount']*100)/(100+row['tax_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Tax amount - from final amount & tax_rate.'
            
    # Tax amount - from final amount & igst_rate
    if not line_items_df[['final_amount', 'igst_rate', 'tax_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['tax_amount'], row['final_amount'] - ((row['final_amount']*100)/(100+row['igst_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'Tax amount - from final amount & igst_rate.'
                        
    # Tax amount - from final amount, sgst_rate & cgst_rate
    if not line_items_df[['final_amount', 'sgst_rate','cgst_rate', 'tax_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['tax_amount'], row['final_amount'] - ((row['final_amount']*100)/(100+(row['sgst_rate']+row['cgst_rate']))), atol=1, rtol=0):
                return False, 'Relation check', 'Tax amount - from final amount, sgst_rate & sgst_rate.'
            
    # igst_amount - from final amount & tax_rate
    if not line_items_df[['final_amount', 'tax_rate', 'igst_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['igst_amount'], row['final_amount'] - ((row['final_amount']*100)/(100+row['tax_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'igst_amount - from final amount & tax_rate.'
            
    # sgst_amount & cgst_amount - from final amount & tax_rate
    if not line_items_df[['final_amount', 'tax_rate', 'sgst_amount','cgst_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose((row['sgst_amount']+row['cgst_amount']), row['final_amount'] - ((row['final_amount']*100)/(100+row['tax_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'sgst_amount & cgst_amount - from final amount & tax_rate.'
            
    # sgst_amount & cgst_amount - from final amount, sgst_rate & cgst_rate
    if not line_items_df[['final_amount', 'tax_rate', 'sgst_amount','cgst_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose((row['sgst_amount']+row['cgst_amount']), row['final_amount'] - ((row['final_amount']*100)/(100+(row['sgst_rate']+row['cgst_rate']))), atol=1, rtol=0):
                return False, 'Relation check', 'sgst_amount & cgst_amount - from final amount, sgst_rate & cgst_rate.'
            
    # igst_amount - from final amount & igst_rate
    if not line_items_df[['final_amount', 'igst_rate','igst_amount']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['igst_amount'], row['final_amount'] - ((row['final_amount']*100)/(100+row['igst_rate'])), atol=1, rtol=0):
                return False, 'Relation check', 'igst_amount - from final amount & igst_rate.'
            


    '''
    
    Final Amount
    
    '''
    # Final amount - from taxable vlue and tax amount
    if not line_items_df[['final_amount', 'tax_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + row['tax_amount'], atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue and tax amount.'
            
    # Final amount - from taxable vlue and igst_amount
    if not line_items_df[['final_amount', 'igst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + row['igst_amount'], atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue and igst_amount.'
            
    # Final amount - from taxable vlue, sgst_amount and cgst_amount
    if not line_items_df[['final_amount', 'sgst_amount','cgst_amount', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + (row['sgst_amount']+row['cgst_amount']), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue, sgst_amount and cgst_amount.'
            
    # Final amount - from taxable vlue and tax_rate
    if not line_items_df[['final_amount', 'tax_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + (row['taxable_value']*row['tax_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue and tax_rate.'
            
    # Final amount - from taxable vlue and igst_rate
    if not line_items_df[['final_amount', 'igst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + (row['taxable_value']*row['igst_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue and igst_rate.'
            
    # Final amount - from taxable vlue, sgst_rate and cgst_rate
    if not line_items_df[['final_amount', 'sgst_rate','cgst_rate', 'taxable_value']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], row['taxable_value'] + (row['taxable_value']*(row['sgst_rate']+row['cgst_rate'])/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from taxable vlue, sgst_rate and cgst_rate.'
            
    # Final amount - from tax_rate and tax amount
    if not line_items_df[['final_amount', 'tax_amount', 'tax_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*row['tax_amount'])/row['tax_rate']) + (((100*row['tax_amount'])/row['tax_rate'])*row['tax_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from tax_rate and tax amount.'
            
    # Final amount - from tax_rate and igst_amount
    if not line_items_df[['final_amount', 'igst_amount', 'tax_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*row['igst_amount'])/row['tax_rate']) + (((100*row['igst_amount'])/row['tax_rate'])*row['tax_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from tax_rate and igst_amount.'
            
    # Final amount - from igst_rate and igst_amount
    if not line_items_df[['final_amount', 'igst_amount', 'igst_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*row['igst_amount'])/row['igst_rate']) + (((100*row['igst_amount'])/row['igst_rate'])*row['igst_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from igst_rate and igst_amount.'
            
    # Final amount - from igst_rate and tax_amount
    if not line_items_df[['final_amount', 'tax_amount', 'igst_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*row['tax_amount'])/row['igst_rate']) + (((100*row['tax_amount'])/row['igst_rate'])*row['igst_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from igst_rate and tax_amount.'
            
    # Final amount - from tax_rate, sgst_amount and cgst_amount
    if not line_items_df[['final_amount', 'sgst_amount','cgst_amount', 'tax_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*(row['sgst_amount']+row['cgst_amount']))/row['tax_rate']) + (((100*(row['sgst_amount']+row['cgst_amount']))/row['tax_rate'])*row['tax_rate']/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from tax_rate and sgst_amount and cgst_amount.'
            
    # Final amount - from sgst_rate, cgst_rate, sgst_amount and cgst_amount
    if not line_items_df[['final_amount', 'sgst_amount','cgst_amount', 'sgst_rate','cgst_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*(row['sgst_amount']+row['cgst_amount']))/(row['sgst_rate']+row['cgst_rate'])) + (((100*(row['sgst_amount']+row['cgst_amount']))/(row['sgst_rate']+row['cgst_rate']))*(row['sgst_rate']+row['cgst_rate'])/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from sgst_rate, cgst_rate and sgst_amount and cgst_amount.'
            
    # Final amount - from sgst_rate, cgst_rate and tax_amount
    if not line_items_df[['final_amount', 'tax_amount', 'sgst_rate','cgst_rate']].isnull().any().any():
        for id, row in line_items_df.iterrows():
            if not np.isclose(row['final_amount'], ((100*row['tax_amount'])/(row['sgst_rate']+row['cgst_rate'])) + (((100*row['tax_amount'])/(row['sgst_rate']+row['cgst_rate']))*(row['sgst_rate']+row['cgst_rate'])/100), atol=1, rtol=0):
                return False, 'Relation check', 'Final amount - from sgst_rate, cgst_rate and tax_amount.'
            


    '''
    
    SGST+CGST or IGST based on place of supply & origin

    '''
    for index, row in invoice_df.iterrows():
        if pd.isnull(row['place_of_supply']) or row['place_of_supply']==row['place_of_origin']:
            if line_items_df['igst_amount'].sum() > 0:
                return False, 'Relation check', 'IGST should be 0 for intra state supply.'
            
            if line_items_df['igst_rate'].sum() > 0:
                return False, 'Relation check', 'IGST should be 0 for intra state supply.'
            
            if total_summary_df['total_igst_amount'].sum() > 0:
                return False, 'Relation check', 'IGST should be 0 for intra state supply.'

        else:
            if line_items_df['sgst_amount'].sum() > 0:
                return False, 'Relation check', 'SGST should be 0 for inter state supply.'
            if line_items_df['cgst_amount'].sum() > 0:
                return False, 'Relation check', 'CGST should be 0 for inter state supply.'
            
            if line_items_df['sgst_rate'].sum() > 0:
                return False, 'Relation check', 'SGST should be 0 for inter state supply.'
            if line_items_df['cgst_rate'].sum() > 0:
                return False, 'Relation check', 'CGST should be 0 for inter state supply.'
            
            if total_summary_df['total_sgst_amount'].sum() > 0:
                return False, 'Relation check', 'SGST should be 0 for inter state supply.'
            if total_summary_df['total_cgst_amount'].sum() > 0:
                return False, 'Relation check', 'CGST should be 0 for inter state supply.'
            

    
    '''
    
    Overall check
    
    '''

    # Invoice details vs line items 
    if not line_items_df[['final_amount']].isnull().any().any() and not invoice_df[['invoice_value']].isnull().any().any():
        item_invoice = line_items_df['final_amount'].sum()
        invoice_invoice = invoice_df['invoice_value'].sum()

        if not np.isclose(item_invoice, invoice_invoice, atol=1, rtol=0):
            return False, 'Relation check', 'Invoice value does not match between line items and invoice details.'  

    if not line_items_df[['taxable_value']].isnull().any().any() and not invoice_df[['taxable_value']].isnull().any().any():
        item_taxable = line_items_df['taxable_value'].sum()
        invoice_taxable = invoice_df['taxable_value'].sum()

        if not np.isclose(item_taxable, invoice_taxable, atol=1, rtol=0):
            return False, 'Relation check', 'Taxable value does not match between line items and invoice details.'
        
    if not line_items_df[['tax_amount']].isnull().any().any() and not invoice_df[['tax_amount']].isnull().any().any():
        item_tax = line_items_df['tax_amount'].sum()
        invoice_tax = invoice_df['tax_amount'].sum()

        if not np.isclose(item_tax, invoice_tax, atol=1, rtol=0):
            return False, 'Relation check', 'tax_amount does not match between line items and invoice details.'
        

    # Summary vs line items
    # Tax amount
    if line_items_df['tax_amount'].notna().any():  
        if line_items_df['tax_amount'].sum() > 0:
            if total_summary_df['total_tax_amount'].isnull().any() or (total_summary_df['total_tax_amount'] == 0).any():
                return False, 'Relation check', 'Tax amount in line items vs summary.'
            
    # Taxable value
    if line_items_df['taxable_value'].notna().any():  
        if line_items_df['taxable_value'].sum() > 0:
            if total_summary_df['total_taxable_value'].isnull().any() or (total_summary_df['total_taxable_value'] == 0).any():
                return False, 'Relation check', 'Taxable value in line items vs summary.'
    
    # Final amount
    if line_items_df['final_amount'].notna().any():  
        if line_items_df['final_amount'].sum() > 0:
            if total_summary_df['total_invoice_value'].isnull().any() or (total_summary_df['total_invoice_value'] == 0).any():
                return False, 'Relation check', 'Invoice value in line items vs summary.'
    
    # IGST
    if line_items_df['igst_amount'].notna().any():  
        if line_items_df['igst_amount'].sum() > 0:
            if total_summary_df['total_igst_amount'].isnull().any() or (total_summary_df['total_igst_amount'] == 0).any():
                return False, 'Relation check', 'IGST amount in line items vs summary.'
            
    # SGST
    if line_items_df['sgst_amount'].notna().any():  
        if line_items_df['sgst_amount'].sum() > 0:
            if total_summary_df['total_sgst_amount'].isnull().any() or (total_summary_df['total_sgst_amount'] == 0).any():
                return False, 'Relation check', 'SGST amount in line items vs summary.'
            
    # CGST
    if line_items_df['cgst_amount'].notna().any():  
        if line_items_df['cgst_amount'].sum() > 0:
            if total_summary_df['total_cgst_amount'].isnull().any() or (total_summary_df['total_cgst_amount'] == 0).any():
                return False, 'Relation check', 'CGST amount in line items vs summary.'

    if not line_items_df[['tax_amount']].isnull().any().any() and not total_summary_df[['total_tax_amount']].isnull().any().any():
        item_tax = line_items_df['tax_amount'].sum()
        summary_tax = total_summary_df['total_tax_amount'].sum()

        if not np.isclose(item_tax, summary_tax, atol=1, rtol=0):
            return False, 'Relation check', 'tax_amount does not match between line items and summary details.'
        
    if not line_items_df[['taxable_value']].isnull().any().any() and not total_summary_df[['total_taxable_value']].isnull().any().any():
        item_taxable = line_items_df['taxable_value'].sum()
        summary_taxable = total_summary_df['total_taxable_value'].sum()

        if not np.isclose(item_taxable, summary_taxable, atol=1, rtol=0):
            return False, 'Relation check', 'Taxable value does not match between line items and summary details.'
        
    if not line_items_df[['final_amount']].isnull().any().any() and not total_summary_df[['total_invoice_value']].isnull().any().any():
        item_invoice = line_items_df['final_amount'].sum()
        summary_invoce = total_summary_df['total_invoice_value'].sum()

        if not np.isclose(item_invoice, summary_invoce, atol=1, rtol=0):
            return False, 'Relation check', 'final_amount does not match between line items and summary details.'
        
    if not line_items_df[['cgst_amount']].isnull().any().any() and not total_summary_df[['total_cgst_amount']].isnull().any().any():
        item_c = line_items_df['cgst_amount'].sum()
        summary_c = total_summary_df['total_cgst_amount'].sum()

        if not np.isclose(item_c, summary_c, atol=1, rtol=0):
            return False, 'Relation check', 'cgst_amount does not match between line items and summary details.'
        
    if not line_items_df[['sgst_amount']].isnull().any().any() and not total_summary_df[['total_sgst_amount']].isnull().any().any():
        item_s = line_items_df['sgst_amount'].sum()
        summary_s = total_summary_df['total_sgst_amount'].sum()

        if not np.isclose(item_s, summary_s, atol=1, rtol=0):
            return False, 'Relation check', 'sgst_amount does not match between line items and summary details.'
        
    if not line_items_df[['igst_amount']].isnull().any().any() and not total_summary_df[['total_igst_amount']].isnull().any().any():
        item_i = line_items_df['igst_amount'].sum()
        summary_i = total_summary_df['total_igst_amount'].sum()

        if not np.isclose(item_i, summary_i, atol=1, rtol=0):
            return False, 'Relation check', 'igst_amount does not match between line items and summary details.'
        
    return True, 'Passed Relation check', 'proceed.'



def use_deduction_field_on_total_summary_after_tax(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'invoice_value'] = row['invoice_value'] + deduction

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_invoice_value'] = row['total_invoice_value'] + deduction

    return invoice_df2, line_items_df2, total_summary_df2

def before_before_before(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df2['total_taxable_value'].sum()

    # LR
    if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
        for index, row in line_items_df2.iterrows():
            line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * (row['taxable_value'] / total_taxable_value)) / row['quantity'])

    # LT
    for index, row in line_items_df2.iterrows():
        line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)

    # TT
    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction

    return invoice_df2, line_items_df2, total_summary_df2

def before_before_after(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()


    # # LR
    # if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
    #     for index, row in line_items_df2.iterrows():
    #         line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])

    # LT
    for index, row in line_items_df2.iterrows():
        line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)


    # TT
    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction

    return invoice_df2, line_items_df2, total_summary_df2

def before_after_before(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()


    # LR
    if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
        for index, row in line_items_df2.iterrows():
            line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])

    # # LT
    # for index, row in line_items_df2.iterrows():
    #     line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)


    # TT
    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction


    return invoice_df2, line_items_df2, total_summary_df2

def before_after_after(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()


    # # LR
    # if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
    #     for index, row in line_items_df2.iterrows():
    #         line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])

    # # LT
    # for index, row in line_items_df2.iterrows():
    #     line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)

    # TT
    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction



    return invoice_df2, line_items_df2, total_summary_df2

def after_before_before(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()

    # LT
    for index, row in line_items_df2.iterrows():
        line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / (total_taxable_value + deduction))

    # LR
    if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
        for index, row in line_items_df2.iterrows():
            line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])


    # # TT
    # for index, row in invoice_df2.iterrows():
    #     invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    # for index, row in total_summary_df2.iterrows():
    #     total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction

    return invoice_df2, line_items_df2, total_summary_df2

def after_before_after(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()

    # # LR
    # if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
    #     for index, row in line_items_df2.iterrows():
    #         line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])

    # LT
    for index, row in line_items_df2.iterrows():
        line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)


    # # TT
    # for index, row in invoice_df2.iterrows():
    #     invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    # for index, row in total_summary_df2.iterrows():
    #     total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction

    return invoice_df2, line_items_df2, total_summary_df2

def after_after_before(invoice_df, line_items_df, total_summary_df, deduction):

    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    total_taxable_value = total_summary_df['total_taxable_value'].sum()

    # LR
    if line_items_df2['quantity'].notna().all() and line_items_df2['rate_per_item_after_discount'].notna().all():
        for index, row in line_items_df2.iterrows():
            line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - ((deduction * row['taxable_value'] / total_taxable_value) / row['quantity'])

    # # LT
    # for index, row in line_items_df2.iterrows():
    #     line_items_df2.at[index, 'taxable_value'] = row['taxable_value'] - (deduction * row['taxable_value'] / total_taxable_value)


    # # TT
    # for index, row in invoice_df2.iterrows():
    #     invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - deduction

    # for index, row in total_summary_df2.iterrows():
    #     total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - deduction

    return invoice_df2, line_items_df2, total_summary_df2



def addition_field_use_on_final_amount(invoice_df, line_items_df, total_summary_df, addition):
    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'invoice_value'] = row['invoice_value'] - addition

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_invoice_value'] = row['total_invoice_value'] - addition

    return invoice_df2, line_items_df2, total_summary_df2   

def addition_field_use_on_taxable_value(invoice_df, line_items_df, total_summary_df, addition):
    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - addition

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - addition

    return invoice_df2, line_items_df2, total_summary_df2 

def addition_field_use_on_both(invoice_df, line_items_df, total_summary_df, addition):
    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'taxable_value'] = row['taxable_value'] - addition

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_taxable_value'] = row['total_taxable_value'] - addition

    for index, row in invoice_df2.iterrows():
        invoice_df2.at[index, 'invoice_value'] = row['invoice_value'] - addition

    for index, row in total_summary_df2.iterrows():
        total_summary_df2.at[index, 'total_invoice_value'] = row['total_invoice_value'] - addition

    return invoice_df2, line_items_df2, total_summary_df2    



def try_total_summary_combinations_with_addition(invoice_df2, line_items_df2, total_summary_df2, rate, addition):

    # Both
    # 
    invoice_df3 = copy.deepcopy(invoice_df2)
    line_items_df3 = copy.deepcopy(line_items_df2)
    total_summary_df3 = copy.deepcopy(total_summary_df2)

    for index, row in invoice_df3.iterrows():
        invoice_df3.at[index, 'invoice_value'] = row['invoice_value'] + addition
        invoice_df3.at[index, 'taxable_value'] = row['taxable_value'] + addition

    for index, row in total_summary_df3.iterrows():
        total_summary_df3.at[index, 'total_invoice_value'] = row['total_invoice_value'] + addition
        total_summary_df3.at[index, 'total_taxable_value'] = row['total_taxable_value'] + addition

    check, step, remark = relation_check(invoice_df3, line_items_df3, total_summary_df3)

    if check == True:
        return check, step, remark, invoice_df3, line_items_df3, total_summary_df3

    # Taxable value
    # 
    invoice_df3 = copy.deepcopy(invoice_df2)
    line_items_df3 = copy.deepcopy(line_items_df2)
    total_summary_df3 = copy.deepcopy(total_summary_df2)

    for index, row in invoice_df3.iterrows():
        invoice_df3.at[index, 'taxable_value'] = row['taxable_value'] + addition

    for index, row in total_summary_df3.iterrows():
        total_summary_df3.at[index, 'total_taxable_value'] = row['total_taxable_value'] + addition

    check, step, remark = relation_check(invoice_df3, line_items_df3, total_summary_df3)

    if check == True:
        return check, step, remark, invoice_df3, line_items_df3, total_summary_df3

    # Invoice value
    # 
    invoice_df3 = copy.deepcopy(invoice_df2)
    line_items_df3 = copy.deepcopy(line_items_df2)
    total_summary_df3 = copy.deepcopy(total_summary_df2)

    for index, row in invoice_df3.iterrows():
        invoice_df3.at[index, 'invoice_value'] = row['invoice_value'] + addition

    for index, row in total_summary_df3.iterrows():
        total_summary_df3.at[index, 'total_invoice_value'] = row['total_invoice_value'] + addition

    check, step, remark = relation_check(invoice_df3, line_items_df3, total_summary_df3)

    if check == True:
        return check, step, remark, invoice_df3, line_items_df3, total_summary_df3

    return False, False, False, invoice_df2, line_items_df2, total_summary_df2

def try_with_tax(invoice_df, line_items_df, total_summary_df, rate, amount, addition):
    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    try:
        if invoice_df2['place_of_supply'].tolist() == invoice_df2['place_of_origin'].tolist():
            new_row = {
                'item_name':'Addition',
                'rate_per_item_after_discount': addition,
                'discount': 0,
                'quantity': 1,
                'taxable_value': addition,
                'sgst_amount': amount/2,
                'cgst_amount': amount/2,
                'igst_amount': 0,
                'sgst_rate': rate/2,
                'cgst_rate': rate/2,
                'igst_rate': 0,
                'tax_amount': amount,
                'tax_rate': rate,
                'final_amount': addition
            }
        else:
            new_row = {
                'item_name':'Addition',
                'rate_per_item_after_discount': addition,
                'discount': 0,
                'quantity': 1,
                'taxable_value': addition,
                'sgst_amount': 0,
                'cgst_amount': 0,
                'igst_amount': amount,
                'sgst_rate': 0,
                'cgst_rate': 0,
                'igst_rate': rate,
                'tax_amount': amount,
                'tax_rate': rate,
                'final_amount': addition
            }

        line_items_df2 = pd.concat([line_items_df2, pd.DataFrame([new_row])], ignore_index=True)

        check, step, remark, invoice_df2, line_items_df2, total_summary_df2 = try_total_summary_combinations_with_addition(invoice_df2, line_items_df2, total_summary_df2, rate, addition)
        if check == True:
            return check, step, remark, invoice_df2, line_items_df2, total_summary_df2

        return False, False, False, invoice_df, line_items_df, total_summary_df
    
    except:
        return False, False, False, invoice_df, line_items_df, total_summary_df

def try_without_tax(invoice_df, line_items_df, total_summary_df, rate, amount, addition):
    invoice_df2 = copy.deepcopy(invoice_df)
    line_items_df2 = copy.deepcopy(line_items_df)
    total_summary_df2 = copy.deepcopy(total_summary_df)

    try:
        if invoice_df2['place_of_supply'].tolist() == invoice_df2['place_of_origin'].tolist():
            new_row = {
                'item_name':'Addition',
                'rate_per_item_after_discount': addition,
                'discount': 0,
                'quantity': 1,
                'taxable_value': addition,
                'sgst_amount': 0,
                'cgst_amount': 0,
                'igst_amount': 0,
                'sgst_rate': 0,
                'cgst_rate': 0,
                'igst_rate': 0,
                'tax_amount': 0,
                'tax_rate': 0,
                'final_amount': addition
            }
        else:
            new_row = {
                'item_name':'Addition',
                'rate_per_item_after_discount': addition,
                'discount': 0,
                'quantity': 1,
                'taxable_value': addition,
                'sgst_amount': 0,
                'cgst_amount': 0,
                'igst_amount': 0,
                'sgst_rate': 0,
                'cgst_rate': 0,
                'igst_rate': 0,
                'tax_amount': 0,
                'tax_rate': 0,
                'final_amount': addition
            }

        line_items_df2 = pd.concat([line_items_df2, pd.DataFrame([new_row])], ignore_index=True)

        check, step, remark, invoice_df2, line_items_df2, total_summary_df2 = try_total_summary_combinations_with_addition(invoice_df2, line_items_df2, total_summary_df2, rate, addition)
        if check == True:
            return check, step, remark, invoice_df2, line_items_df2, total_summary_df2

        return False, False, False, invoice_df, line_items_df, total_summary_df
    
    except:
        return False, False, False, invoice_df, line_items_df, total_summary_df

def use_addition_field(invoice_df, line_items_df, total_summary_df, addition, rate, amount):
    
    if not rate:
        return False, False, False, invoice_df, line_items_df, total_summary_df
    else:
        check, step, remark, invoice_df2, line_items_df2, total_summary_df2 = try_with_tax(invoice_df, line_items_df, total_summary_df, rate, amount, addition)
        if check == True:
            return check, step, remark, invoice_df2, line_items_df2, total_summary_df2
        check, step, remark, invoice_df3, line_items_df3, total_summary_df3 = try_without_tax(invoice_df, line_items_df, total_summary_df, rate, amount, addition)
        if check == True:
            return check, step, remark, invoice_df3, line_items_df3, total_summary_df3
         
        return False, False, False, invoice_df, line_items_df, total_summary_df



def perform_relation_check_after_using_additional_fields_in_summary(invoice_df, line_items_df, total_summary_df):

    deduction = total_summary_df['Deduction'].sum()
    addition = total_summary_df['Addition'].sum()

    if deduction > 0:
        # TT=B, LR=B, LT=B
        invoice_df2, line_items_df2, total_summary_df2 = before_before_before(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2

        # TT=B, LR=B, LT=A
        invoice_df2, line_items_df2, total_summary_df2 = before_before_after(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2

        # TT=B, LR=A, LT=B
        invoice_df2, line_items_df2, total_summary_df2 = before_after_before(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
        
        # TT=B, LR=A, LT=A
        invoice_df2, line_items_df2, total_summary_df2 = before_after_after(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
        
        # TT=A, LR=B, LT=B
        invoice_df2, line_items_df2, total_summary_df2 = after_before_before(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
        
        # TT=A, LR=B, LT=A
        invoice_df2, line_items_df2, total_summary_df2 = after_before_after(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2

        # TT=A, LR=A, LT=B
        invoice_df2, line_items_df2, total_summary_df2 = after_after_before(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2

        # deduction in total summary after tax
        invoice_df2, line_items_df2, total_summary_df2 = use_deduction_field_on_total_summary_after_tax(invoice_df, line_items_df, total_summary_df, deduction)
        check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
        
    if addition > 0:

        if not line_items_df[['tax_rate']].isnull().any().any():
            rate = line_items_df['tax_rate'].mean()
            amount = addition * rate / 100

        elif not line_items_df[['igst_rate']].isnull().any().any():
            rate = line_items_df['igst_rate'].mean()
            amount = addition * rate / 100

        elif not line_items_df[['sgst_rate','cgst_rate']].isnull().any().any():
            rate = line_items_df2['sgst_rate'].mean() * 2
            amount = addition * rate / 100

        elif not total_summary_df[['total_tax_amount','total_taxable_value']].isnull().any().any():
            rate = total_summary_df['total_tax_amount'].sum() / total_summary_df['total_taxable_value'].sum() * 100
            amount = total_summary_df['total_tax_amount']

        else:
            rate = None
            amount = None
        
        check, stage, remark, invoice_df2, line_items_df2, total_summary_df2 = use_addition_field(invoice_df, line_items_df, total_summary_df, addition, rate, amount)
        if check == True:
            return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
    
    return False, 'Using addition/deduction', 'Still failed.', invoice_df, line_items_df, total_summary_df

def perform_relation_check_after_using_discount_field_in_line_items(invoice_df, line_items_df, total_summary_df):

    def overall_line_item_discount(invoice_df, line_items_df, total_summary_df):

        invoice_df2 = copy.deepcopy(invoice_df)
        line_items_df2 = copy.deepcopy(line_items_df)
        total_summary_df2 = copy.deepcopy(total_summary_df)

        if not line_items_df2[['rate_per_item_after_discount', 'discount']].isnull().any().any():
            for index, row in line_items_df2.iterrows():
                line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - row['discount']

        return invoice_df2, line_items_df2, total_summary_df2

    def specific_rate_per_item_discount(invoice_df, line_items_df, total_summary_df):
        invoice_df2 = copy.deepcopy(invoice_df)
        line_items_df2 = copy.deepcopy(line_items_df)
        total_summary_df2 = copy.deepcopy(total_summary_df)

        if not line_items_df2[['rate_per_item_after_discount', 'discount', 'quantity']].isnull().any().any():
            for index, row in line_items_df2.iterrows():
                line_items_df2.at[index, 'rate_per_item_after_discount'] = row['rate_per_item_after_discount'] - (row['discount']/row['quantity'])

        return invoice_df2, line_items_df2, total_summary_df2
    
    # overall for line item
    invoice_df2, line_items_df2, total_summary_df2 = overall_line_item_discount(invoice_df, line_items_df, total_summary_df)
    check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
    if check == True:
        return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2

    # specific for rate per item
    invoice_df2, line_items_df2, total_summary_df2 = specific_rate_per_item_discount(invoice_df, line_items_df, total_summary_df)
    check, stage, remark = relation_check(invoice_df2, line_items_df2, total_summary_df2)
    if check == True:
        return check, stage, remark, invoice_df2, line_items_df2, total_summary_df2
    
    return False, False, False, invoice_df, line_items_df, total_summary_df



def accuracy_check(invoice_df, line_items_df, total_summary_df):
    
    # missing value
    check, stage, remark = missing_value_check(invoice_df, line_items_df, total_summary_df)
    
    if check == False:

        line_items_df = fill_line_items_from_summary(line_items_df, total_summary_df)

        check, stage, remark = missing_value_check(invoice_df, line_items_df, total_summary_df)

        if check == False:
            return check, stage, remark, invoice_df, line_items_df, total_summary_df
    
    # data type
    check, stage, remark = data_type_check(invoice_df, line_items_df, total_summary_df)
    if check == False:
        return check, stage, remark, invoice_df, line_items_df, total_summary_df

    line_items_df = fill_taxable_from_qty_rate(line_items_df)
    
    check2 = None

    # relation
    r_check, r_stage, r_remark = relation_check(invoice_df, line_items_df, total_summary_df)
    if r_check == False and r_remark in ['Rate.','Rate.1','Rate.2','Quantity.']:
        check3, stage3, remark3, invoice_df3, line_items_df3, total_summary_df3 = perform_relation_check_after_using_discount_field_in_line_items(invoice_df, line_items_df, total_summary_df)
        if check3 == False:
            check2, stage2, remark2, invoice_df2, line_items_df2, total_summary_df2 = perform_relation_check_after_using_additional_fields_in_summary(invoice_df, line_items_df, total_summary_df)
            if check2 == False:
                return r_check, r_stage, r_remark, invoice_df2, line_items_df2, total_summary_df2
            else:
                return check2, stage2, 'After manipulation', invoice_df2, line_items_df2, total_summary_df2
        else:
            return check3, stage3, 'After manipulation', invoice_df3, line_items_df3, total_summary_df3
        
    else:
        if r_check == False:
            check2, stage2, remark2, invoice_df2, line_items_df2, total_summary_df2 = perform_relation_check_after_using_additional_fields_in_summary(invoice_df, line_items_df, total_summary_df)
            if check2 == False:
                return r_check, r_stage, r_remark, invoice_df2, line_items_df2, total_summary_df2
            else:
                return check2, stage2, 'After manipulation', invoice_df2, line_items_df2, total_summary_df2
    

    return True, 'All stages', 'Passed', invoice_df, line_items_df, total_summary_df





def log_data_in_response_df(response_df, file_name, response_json, check, step, remark, status_code):
    # new_row = {'file_name':file_name, 'response_json':response_json, 'check_passed':check, 'step':step, 'remark':remark, 'status_code':status_code}
    new_row = {'file_name':file_name, 'response_json': json.dumps(response_json, ensure_ascii=False), 'check_passed':check, 'step':step, 'remark':remark, 'status_code':status_code}
    st.write(json.dumps(response_json, ensure_ascii=False))
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    st.write(response_df)

    return response_df

def log_data_in_response_df_for_process_error(response_df, file_name, response_json, e, status_code):
    new_row = {'file_name':file_name, 'response_json':response_json, 'check_passed':False, 'step':'Error in processing', 'remark':e, 'status_code':status_code}
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    return response_df

def log_data_in_response_df_for_no_response(response_df, file_name, status_code):
    new_row = {'file_name':file_name, 'response_json':None, 'check_passed':None, 'step':None, 'remark':None, 'status_code':status_code}
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    return response_df

def log_data_in_response_df_for_no_dataframes(response_df, file_name, response_json, status_code):
    new_row = {'file_name':file_name, 'response_json':response_json, 'check_passed':None, 'step':None, 'remark':'Unknown JSON structure', 'status_code':status_code}
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    return response_df

def log_data_in_response_df_for_invalid_file(response_df, file_name, status_code, error_code, message):
    new_row = {'file_name': file_name, 'status_code':status_code, 'check_passed': None, 'step': error_code, 'remark': message}
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    return response_df

def log_data_in_output_dataframe(invoice_df, line_items_df, total_summary_df, final_df):
    
    invoice_number = invoice_df['invoice_number'].iloc[0]
    invoice_date = invoice_df['invoice_date'].iloc[0]
    place_of_supply = invoice_df['place_of_supply'].iloc[0]
    place_of_origin = invoice_df['place_of_origin'].iloc[0]
    supplier_name = invoice_df['supplier_name'].iloc[0]
    gstin_supplier = invoice_df['gstin_supplier'].iloc[0]
    receiver_name = invoice_df['receiver_name'].iloc[0]
    gstin_recipient = invoice_df['gstin_recipient'].iloc[0]
    
    invoice_value = line_items_df['final_amount'].sum()

    grouped = line_items_df.groupby("tax_rate").agg({"taxable_value": "sum"}).reset_index()

    for _, row in grouped.iterrows():
        new_row = pd.DataFrame([{
            "gstin_recipient": gstin_recipient,
            "receiver_name": receiver_name,
            "gstin_supplier": gstin_supplier,
            "supplier_name": supplier_name,
            "invoice_number": invoice_number,
            "invoice_date": invoice_date,
            "invoice_value": invoice_value,
            "place_of_supply": place_of_supply,
            "place_of_origin": place_of_origin,
            "tax_rate": row["tax_rate"],
            "taxable_value": row["taxable_value"]
        }])
        final_df = pd.concat([final_df, new_row], ignore_index=True)

    return final_df

def log_data_in_response_df_for_failed_response(response_df, file_name, status_code, response_json):
    new_row = {'file_name':file_name, 'response_json':response_json, 'check_passed':None, 'step':None, 'remark':'Failed response', 'status_code':status_code}
    response_df = pd.concat([response_df, pd.DataFrame([new_row])], ignore_index=True)

    return response_df

def round_to_nearest_zero(value):
    # Check if the difference from the nearest integer is within 0.02
    if abs(value - round(value)) <= 0.02:
        return round(value)
    return value

def fill_missing_values_line_items_df(df):

  # Convert all rate columns to numeric, coercing errors
  df['final_amount'] = pd.to_numeric(df['final_amount'], errors='coerce').fillna(0)
  df['taxable_value'] = pd.to_numeric(df['taxable_value'], errors='coerce').fillna(0)
  df['tax_amount'] = pd.to_numeric(df['tax_amount'], errors='coerce').fillna(0)
  df['tax_rate'] = pd.to_numeric(df['tax_rate'], errors='coerce').fillna(0)
  df['cgst_rate'] = pd.to_numeric(df['cgst_rate'], errors='coerce').fillna(0)
  df['sgst_rate'] = pd.to_numeric(df['sgst_rate'], errors='coerce').fillna(0)
  df['igst_rate'] = pd.to_numeric(df['igst_rate'], errors='coerce').fillna(0)
  df['cgst_amount'] = pd.to_numeric(df['cgst_amount'], errors='coerce').fillna(0)
  df['sgst_amount'] = pd.to_numeric(df['sgst_amount'], errors='coerce').fillna(0)
  df['igst_amount'] = pd.to_numeric(df['igst_amount'], errors='coerce').fillna(0)
  df['rate_per_item_after_discount'] = pd.to_numeric(df['rate_per_item_after_discount'], errors='coerce').fillna(0)
  df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0)

  for index, row in df.iterrows():

    final_amount = 0 if pd.isna(row['final_amount']) else row['final_amount']
    taxable_value = 0 if pd.isna(row['taxable_value']) else row['taxable_value']
    tax_amount = 0 if pd.isna(row['tax_amount']) else row['tax_amount']
    gst_rate = 0 if pd.isna(row['tax_rate']) else row['tax_rate']
    cgst_rate = 0 if pd.isna(row['cgst_rate']) else row['cgst_rate']
    sgst_rate = 0 if pd.isna(row['sgst_rate']) else row['sgst_rate']
    igst_rate = 0 if pd.isna(row['igst_rate']) else row['igst_rate']
    rate_per_item_after_discount = 0 if pd.isna(row['rate_per_item_after_discount']) else row['rate_per_item_after_discount']
    quantity = 0 if pd.isna(row['quantity']) else row['quantity']
      
    gst_rate_combined = cgst_rate + sgst_rate + igst_rate

    cgst_amount = 0 if pd.isna(row['cgst_amount']) else row['cgst_amount']
    sgst_amount = 0 if pd.isna(row['sgst_amount']) else row['sgst_amount']
    igst_amount = 0 if pd.isna(row['igst_amount']) else row['igst_amount']
    tax_amount_combined = cgst_amount + sgst_amount + igst_amount

    if taxable_value == 0 and rate_per_item_after_discount != 0 and quantity != 0:
        taxable_value = rate_per_item_after_discount * quantity
        df.at[index, 'taxable_value'] = taxable_value

    if tax_amount == 0 and (tax_amount_combined != 0):
        tax_amount = tax_amount_combined
        df.at[index, 'tax_amount'] = tax_amount

    # Fill gst_rate column & variable from gst_rate_combined if gst_rate is 0
    if gst_rate == 0 and (cgst_rate!=0 or sgst_rate!=0 or igst_rate!=0):
      gst_rate = gst_rate_combined
      df.at[index, 'tax_rate'] = gst_rate

    elif gst_rate == 0 and (cgst_rate==0 and sgst_rate==0 and igst_rate==0):
      gst_rate = 0
      df.at[index, 'tax_rate'] = gst_rate

    # Handle the case where gst_rate is like '0.18'
    if gst_rate >= -0.4 and gst_rate <= 0.4:
        gst_rate = gst_rate * 100
        df.at[index, 'tax_rate'] = gst_rate


    if final_amount != 0 and gst_rate != 0 and taxable_value == 0:
            taxable_value = final_amount * 100 / (100 + gst_rate)
            df.at[index, 'taxable_value'] = taxable_value
            # continue

    elif final_amount != 0 and gst_rate == 0 and taxable_value != 0:
        tax_amount = final_amount - taxable_value
        gst_rate = (tax_amount / taxable_value) * 100
        df.at[index, 'tax_rate'] = gst_rate
        # continue

    elif final_amount != 0 and gst_rate == 0 and taxable_value == 0 and tax_amount != 0:
        taxable_value = final_amount - tax_amount
        gst_rate = (tax_amount / taxable_value) * 100
        df.at[index, 'tax_rate'] = gst_rate
        df.at[index, 'taxable_value'] = taxable_value
        # continue

    elif final_amount != 0 and gst_rate == 0 and taxable_value == 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        taxable_value = final_amount * 100 / (100 + gst_rate)
        df.at[index, 'tax_rate'] = gst_rate
        df.at[index, 'taxable_value'] = taxable_value
        # continue

    if final_amount == 0 and gst_rate != 0 and taxable_value != 0:
        final_amount = taxable_value + (taxable_value * gst_rate / 100)
        df.at[index, 'final_amount'] = final_amount
        # continue

    if final_amount == 0 and gst_rate != 0 and taxable_value == 0 and tax_amount != 0:
        taxable_value = tax_amount * 100 / gst_rate
        final_amount = taxable_value + tax_amount
        df.at[index, 'final_amount'] = final_amount
        df.at[index, 'taxable_value'] = taxable_value
        # continue

    elif final_amount == 0 and gst_rate == 0 and taxable_value != 0 and tax_amount != 0:
        gst_rate = (tax_amount / taxable_value) * 100
        final_amount = taxable_value + tax_amount
        df.at[index, 'tax_rate'] = gst_rate
        df.at[index, 'final_amount'] = final_amount
        # continue

    elif final_amount == 0 and gst_rate == 0 and taxable_value != 0 and tax_amount == 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        final_amount = taxable_value + (taxable_value * gst_rate / 100)
        df.at[index, 'tax_rate'] = gst_rate
        df.at[index, 'final_amount'] = final_amount
        continue

    elif final_amount == 0 and gst_rate == 0 and taxable_value == 0 and tax_amount != 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        taxable_value = tax_amount * 100 / gst_rate
        final_amount = taxable_value + tax_amount
        df.at[index, 'tax_rate'] = gst_rate
        df.at[index, 'taxable_value'] = taxable_value
        df.at[index, 'final_amount'] = final_amount
        # continue

    if tax_amount == 0 and final_amount != 0 and taxable_value != 0:
        tax_amount = final_amount - taxable_value
        df.at[index, 'tax_amount'] = tax_amount

    gst_rate = round_to_nearest_zero(gst_rate)

    df.at[index, 'tax_rate'] = gst_rate

  return df

def get_file_name(file):
    file_name = file.name
    return file_name

def get_listed_files(file_name_list, files_container):
    
    filtered_files = []

    for file in files_container:
        if file.name in file_name_list:
            filtered_files.append(file)

    return filtered_files

# def create_zip(file_container, file_name_dict, final_df, response_df, non_blank_error_code_lists):
#     # Create a BytesIO object to store the zip file in memory
#     zip_buffer = BytesIO()

#     # Open a ZipFile in write mode
#     with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
#         # Add main folder files only
#         for folder in ["all_files", "processed_files", "failed_files", "passed_files", "overload_files"]:
#             if folder in file_name_dict:
#                 # Ensure folder path in the zip file
#                 zip_file.writestr(f"{folder}/", "")

#                 # Process each file name in the folder
#                 for file_name in file_name_dict[folder]:
#                     if file_name in file_container:
#                         file_data = file_container[file_name]
#                         zip_file.writestr(f"{folder}/{file_name}", file_data)
#                     else:
#                         print(f"Warning: {file_name} not found in file_container.")

#         # Create folders from non_blank_error_code_lists inside 'invalid_files'
#         if "invalid_files" in file_name_dict:
#             for error_code, file_names in non_blank_error_code_lists.items():
#                 # Ensure error code folder path under invalid_files
#                 error_folder_path = f"invalid_files/{error_code}/"
#                 zip_file.writestr(error_folder_path, "")

#                 # Process each file name in the error code folder
#                 for file_name in file_names:
#                     if file_name in file_container:
#                         file_data = file_container[file_name]
#                         zip_file.writestr(f"{error_folder_path}{file_name}", file_data)
#                     else:
#                         print(f"Warning: {file_name} not found in file_container.")

#         # Add final_df as output.csv
#         if final_df is not None:
#             csv_data = final_df.to_csv(index=False)
#             zip_file.writestr("output.csv", csv_data)

#         # Add response_df as accuracy_data.csv
#         if response_df is not None:
#             csv_data = response_df.to_csv(index=False)
#             zip_file.writestr("accuracy_data.csv", csv_data)

#     # Seek to the start of the buffer
#     zip_buffer.seek(0)

#     return zip_buffer

def create_zip(file_container, file_name_dict, final_df, response_df, non_blank_error_code_lists):
    # Create a BytesIO object to store the zip file in memory
    zip_buffer = BytesIO()

    # Open a ZipFile in write mode
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        # Track all stored files
        stored_files = set()
        
        # Add main folder files only
        for folder in ["processed_files", "failed_files", "passed_files", "overload_files"]:
            if folder in file_name_dict:
                # Ensure folder path in the zip file
                zip_file.writestr(f"{folder}/", "")
                
                # Process each file name in the folder
                for file_name in file_name_dict[folder]:
                    if file_name in file_container:
                        file_data = file_container[file_name]
                        zip_file.writestr(f"{folder}/{file_name}", file_data)
                        stored_files.add(file_name)
                    else:
                        print(f"Warning: {file_name} not found in file_container.")

        # Create folders from non_blank_error_code_lists inside 'invalid_files'
        if "invalid_files" in file_name_dict:
            for error_code, file_names in non_blank_error_code_lists.items():
                # Ensure error code folder path under invalid_files
                error_folder_path = f"invalid_files/{error_code}/"
                zip_file.writestr(error_folder_path, "")
                
                # Process each file name in the error code folder
                for file_name in file_names:
                    if file_name in file_container:
                        file_data = file_container[file_name]
                        zip_file.writestr(f"{error_folder_path}{file_name}", file_data)
                        stored_files.add(file_name)
                    else:
                        print(f"Warning: {file_name} not found in file_container.")

        # Add final_df as output.csv
        if final_df is not None:
            csv_data = final_df.to_csv(index=False)
            zip_file.writestr("output.csv", csv_data)

        # Add response_df as accuracy_data.csv
        if response_df is not None:
            csv_data = response_df.to_csv(index=False)
            zip_file.writestr("accuracy_data.csv", csv_data)

        # Identify skipped files
        if "processed_files" in file_name_dict:
            processed_files = set(file_name_dict["processed_files"])
            referenced_files = set(file_name_dict.get("failed_files", [])) | \
                               set(file_name_dict.get("passed_files", [])) | \
                               set(file_name_dict.get("overload_files", []))
            
            for file_names in non_blank_error_code_lists.values():
                referenced_files.update(file_names)
            
            skipped_files = processed_files - referenced_files
            
            if skipped_files:
                skipped_folder = "skipped_files/"
                zip_file.writestr(skipped_folder, "")
                
                for file_name in skipped_files:
                    if file_name in file_container:
                        file_data = file_container[file_name]
                        zip_file.writestr(f"{skipped_folder}{file_name}", file_data)
                    else:
                        print(f"Warning: {file_name} not found in file_container.")

    # Seek to the start of the buffer
    zip_buffer.seek(0)

    return zip_buffer


def get_month_year():
    """
    Creates a Streamlit dropdown UI to select a month and year.
    Defaults to last month and its corresponding year.
    Returns selected month (numeric) and year (as strings).
    """
    # Get today's date and calculate last month's date
    today = datetime.today()
    first_day_of_this_month = today.replace(day=1)
    last_month_date = first_day_of_this_month - timedelta(days=1)

    # Get last month's name and year
    default_month = last_month_date.strftime("%B")  # Full month name
    default_year = last_month_date.strftime("%Y")   # Year as a string

    # Month and year options for dropdown
    months = [
        "January", "February", "March", "April", "May", 
        "June", "July", "August", "September", "October", 
        "November", "December"
    ]
    years = [str(year) for year in range(2024, today.year + 1)]

    # Streamlit UI for user inputs
    selected_month = st.selectbox("Select Month", months, index=months.index(default_month))
    selected_year = st.selectbox("Select Year", years, index=years.index(default_year))

    # Convert selected month to numeric format
    month_as_number = str(months.index(selected_month) + 1).zfill(2)  # Zero-padded

    # Return month and year as strings
    return month_as_number, selected_year

def generate_key(length=50):
    if length < 1:
        raise ValueError("Key length must be at least 1 character.")

    # Define the character pool: uppercase, lowercase, digits, and special characters
    characters = string.ascii_letters + string.digits + string.punctuation

    # Randomly select characters from the pool
    key = ''.join(random.choices(characters, k=length))

    return key

def push_to_es(payload):
    
    elastic_url="https://elastic:NuwRaaWUktq5FM1QJZe6iexV@my-deployment-3eafc9.es.ap-south-1.aws.elastic-cloud.com:9243/#{@index_name}/_doc"

    index_name="streamlit_pdf_production_1"

    # Initialize Elasticsearch client
    es = Elasticsearch(elastic_url)

    # Define document ID (None for auto-generated IDs)
    document_id = None

    try:
        # Push data to Elasticsearch
        response = es.index(index=index_name, id=document_id, document=payload)
        return response
    except Exception as e:
        return {"error": str(e)}





# 
