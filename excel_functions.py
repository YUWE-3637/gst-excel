import os
import streamlit as st # type: ignore
import pandas as pd
import numpy as np
from io import BytesIO
import io
import tempfile
from dateutil.parser import parse
import datetime
from io import BytesIO
from datetime import datetime
import copy
import time
import json
import requests
from mapping_data import known_sources, known_source_relevenat_columns, state_codes, state_mis_match_mapping, needed_columns
from elasticsearch import Elasticsearch  # type: ignore


def select_columns_from_unknown_source(df, needed_columns, file_name, sheet_name):
    columns = df.columns.tolist()
    available_name_of_needed_columns_dict = {}
    
    st.write("Select the corresponding columns for each needed field:")
    
    for needed_col in needed_columns:
        # Add a "Not Available" option to the list of columns
        options = ["Not Available"] + columns
        
        # Create a selectbox with search functionality for each needed column
        selected_col = st.selectbox(
            f"Select column for '{needed_col}'",
            options,
            key=f"{file_name}_{sheet_name}_select_{needed_col}",
            help=f"Choose the column that corresponds to {needed_col}"
        )
        
        # If a valid column is selected, add it to the dictionary
        if selected_col != "Not Available":
            available_name_of_needed_columns_dict[selected_col] = needed_col
    
    if available_name_of_needed_columns_dict:
        # Select and rename the columns
        df = df[list(available_name_of_needed_columns_dict.keys())]
        df = df.rename(columns=available_name_of_needed_columns_dict)
    else:
        # Create an empty DataFrame if no columns were selected
        df = pd.DataFrame()
    
    # Add missing columns with NaN values
    for col in needed_columns:
        if col not in df.columns:
            df[col] = np.nan
    
    return df

def integers_in_string(s):
    return sum(c.isdigit() for c in s)

def gstin_or_state(df):
    # Check each row and apply the logic
    df['gst_or_state'] = df['Customer GSTIN number/ Place of Supply'].apply(lambda x: 'gst' if integers_in_string(str(x)) > 2 else 'state')
    return df

def convert_uploaded_files(uploaded_files):
    uploaded_files_dict = {}
    
    # Loop through each uploaded file
    for uploaded_file in uploaded_files:
        # Read the file content into memory (since Streamlit uploads files as bytes)
        file_bytes = uploaded_file.read()
        
        # Get the file extension and specify the correct engine
        file_extension = os.path.splitext(uploaded_file.name)[1].lower()  # Get the file extension (.xlsx or .xls)

        if file_extension == '.xlsx':
            engine = 'openpyxl'
        elif file_extension == '.xls':
            engine = 'xlrd'
        elif uploaded_file.name.endswith('.csv'):
            # For CSV files, we don't need to specify an engine for pd.read_csv
            file_dict = pd.read_csv(io.BytesIO(file_bytes)).to_dict(orient='records')
            uploaded_files_dict[uploaded_file.name] = {'CSV': file_dict}
            continue
        else:
            continue
        
        # Read the Excel file with the determined engine
        xls = pd.ExcelFile(io.BytesIO(file_bytes), engine=engine)
        
        file_dict = {}
        
        # Loop through each sheet in the current file
        for sheet_name in xls.sheet_names:
            # Read sheet into a DataFrame and convert to a dictionary (optional: you can process the data further here)
            sheet_data = pd.read_excel(xls, sheet_name=sheet_name)
            file_dict[sheet_name] = sheet_data.to_dict(orient='records')
        
        uploaded_files_dict[uploaded_file.name] = file_dict
    
    return uploaded_files_dict

def select_columns_from_known_source(df, needed_columns, source):
    if source == 'VS internal format':
        # Set the first row as the header
        df = df[2:]  # Take the data less the header row
        df.columns = ['S.No.','Date','Invoice No','Customer GSTIN number/ Place of Supply','Name of Customer','HSN/SAC Code','Invoice Base Amount (Rs.)','Rate of tax (%)','SGST (Rs.)','CGST (Rs.)','IGST (Rs.)','Exempted/Nill rated sales (Rs.)','Invoice Total (Rs.)']

        df = gstin_or_state(df)

        gst_df = df[df['gst_or_state']=='gst'].copy()
        state_df = df[df['gst_or_state']=='state'].copy()

        gst_df['gstin'] = gst_df['Customer GSTIN number/ Place of Supply']
        gst_df['state'] = np.nan

        state_df['state'] = state_df['Customer GSTIN number/ Place of Supply']
        state_df['gstin'] = np.nan

        df = pd.concat([gst_df, state_df], ignore_index=True)

        # HSN ready to file
    
    if source == 'HSN ready to file':

        removed_top_columns = False
        for index, row in df.iterrows():
            if row[0] == 'HSN':
                if index == 0:
                    removed_top_columns = True

        if not removed_top_columns:
            df = df[3:]
            df.columns = ['HSN','Description','UQC','Total Quantity','Rate','Total Value','Taxable Value','Integrated Tax Amount','Central Tax Amount','State/UT Tax Amount','Cess Amount']


    if source == 'b2b ready to file format':

        removed_top_columns = False
        for index, row in df.iterrows():
            if row[0] == 'GSTIN/UIN of Recipient':
                if index == 0:
                    removed_top_columns = True

        if df.columns[0] == 'GSTIN/UIN of Recipient':
            removed_top_columns = True

        if not removed_top_columns:
            df = df[3:]
            df.columns = ['GSTIN/UIN of Recipient', 'Receiver Name',    'Invoice Number',    'Invoice date', 'Invoice Value', 'Place Of Supply',  'Reverse Charge',   'Applicable % of Tax Rate', 'Invoice Type', 'E-Commerce GSTIN', 'Rate', 'Taxable Value' ,'Cess Amount']

    if source == 'b2cs ready to file format':

        removed_top_columns = False
        for index, row in df.iterrows():
            if row[1] == 'Place Of Supply':
                if index == 0:
                    removed_top_columns = True

        if not removed_top_columns:
            df = df[3:]
            df.columns = ['Type',	'Place Of Supply',	'Applicable % of Tax Rate',	'Rate',	'Taxable Value',	'Cess Amount',	'E-Commerce GSTIN']

    available_name_of_needed_columns_dict = known_source_relevenat_columns[source]
    columns_to_keep = list(available_name_of_needed_columns_dict.keys())
    # Only keep columns that actually exist in the DataFrame
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    df = df[existing_columns]
    df = df.rename(columns=available_name_of_needed_columns_dict)

    for col in needed_columns:
        if col not in df.columns:
            df[col] = np.nan

    return df

def format_place_of_supply(df):
    for index, row in df.iterrows():
        place_of_supply = str(row['Place Of Supply'])

        if place_of_supply in state_mis_match_mapping.keys():
            place_of_supply = state_mis_match_mapping[place_of_supply]

        valid_states = [state['State'] for state in state_codes]
        valid_codes = [state['code_number'] for state in state_codes]
        valid_state_codes = [state['code'] for state in state_codes]

        gstin_value = row['GSTIN/UIN of Recipient']
        gstin_state_code = gstin_value[:2] if isinstance(gstin_value, str) and len(gstin_value)>=2 else None

        if gstin_state_code in valid_codes:
            for state in state_codes:
                if state['code_number'] == gstin_state_code:
                    df.at[index, 'Place Of Supply'] = state['code']
                    break
        elif place_of_supply not in valid_state_codes:
            if isinstance(place_of_supply, str) and place_of_supply.lower() in [state.lower() for state in valid_states]:
                for state in state_codes:
                    if state['State'].lower() == place_of_supply.lower():
                        df.at[index, 'Place Of Supply'] = state['code']
                        break
            elif place_of_supply in valid_codes:
                for state in state_codes:
                    if state['code_number'] == place_of_supply:
                        df.at[index, 'Place Of Supply'] = state['code']
                        break

    return df

def round_to_nearest_zero(value):
    # Check if the difference from the nearest integer is within 0.02
    if abs(value - round(value)) <= 0.02:
        return round(value)
    return value

def fill_missing_values(df):
  # Convert all rate columns to numeric, coercing errors
  df['Invoice Value'] = pd.to_numeric(df['Invoice Value'], errors='coerce').fillna(0)
  df['Taxable Value'] = pd.to_numeric(df['Taxable Value'], errors='coerce').fillna(0)
  df['Tax amount'] = pd.to_numeric(df['Tax amount'], errors='coerce').fillna(0)
  df['Rate'] = pd.to_numeric(df['Rate'], errors='coerce').fillna(0)
  df['Cgst Rate'] = pd.to_numeric(df['Cgst Rate'], errors='coerce').fillna(0)
  df['Sgst Rate'] = pd.to_numeric(df['Sgst Rate'], errors='coerce').fillna(0)
  df['Igst Rate'] = pd.to_numeric(df['Igst Rate'], errors='coerce').fillna(0)
  df['Utgst Rate'] = pd.to_numeric(df['Utgst Rate'], errors='coerce').fillna(0)
  df['Cgst Amount'] = pd.to_numeric(df['Cgst Amount'], errors='coerce').fillna(0)
  df['Sgst Amount'] = pd.to_numeric(df['Sgst Amount'], errors='coerce').fillna(0)
  df['Igst Amount'] = pd.to_numeric(df['Igst Amount'], errors='coerce').fillna(0)
  df['Ugst Amount'] = pd.to_numeric(df['Ugst Amount'], errors='coerce').fillna(0)

  for index, row in df.iterrows():

    invoice_value = 0 if pd.isna(row['Invoice Value']) else row['Invoice Value']
    taxable_value = 0 if pd.isna(row['Taxable Value']) else row['Taxable Value']
    tax_amount = 0 if pd.isna(row['Tax amount']) else row['Tax amount']
    gst_rate = 0 if pd.isna(row['Rate']) else row['Rate']
    cgst_rate = 0 if pd.isna(row['Cgst Rate']) else row['Cgst Rate']
    sgst_rate = 0 if pd.isna(row['Sgst Rate']) else row['Sgst Rate']
    igst_rate = 0 if pd.isna(row['Igst Rate']) else row['Igst Rate']
    utgst_rate = 0 if pd.isna(row['Utgst Rate']) else row['Utgst Rate']
    gst_rate_combined = cgst_rate + sgst_rate + igst_rate + utgst_rate

    cgst_amount = 0 if pd.isna(row['Cgst Amount']) else row['Cgst Amount']
    sgst_amount = 0 if pd.isna(row['Sgst Amount']) else row['Sgst Amount']
    igst_amount = 0 if pd.isna(row['Igst Amount']) else row['Igst Amount']
    ugst_amount = 0 if pd.isna(row['Ugst Amount']) else row['Ugst Amount']
    tax_amount_combined = cgst_amount + sgst_amount + igst_amount + ugst_amount

    if tax_amount == 0 and (tax_amount_combined != 0):
        tax_amount = tax_amount_combined
        df.at[index, 'Tax amount'] = tax_amount

    # Fill gst_rate column & variable from gst_rate_combined if gst_rate is 0
    if gst_rate == 0 and (cgst_rate!=0 or sgst_rate!=0 or igst_rate!=0 or utgst_rate!=0):
      gst_rate = gst_rate_combined
      df.at[index, 'Rate'] = gst_rate

    elif gst_rate == 0 and (cgst_rate==0 and sgst_rate==0 and igst_rate==0 and utgst_rate==0):
      gst_rate = 0
      df.at[index, 'Rate'] = gst_rate

    # Handle the case where gst_rate is like '0.18'
    if gst_rate >= -0.4 and gst_rate <= 0.4:
        gst_rate = gst_rate * 100
        df.at[index, 'Rate'] = gst_rate


    if invoice_value != 0 and gst_rate != 0 and taxable_value == 0:
            taxable_value = invoice_value * 100 / (100 + gst_rate)
            df.at[index, 'Taxable Value'] = taxable_value
            # continue

    elif invoice_value != 0 and gst_rate == 0 and taxable_value != 0:
        tax_amount = invoice_value - taxable_value
        gst_rate = (tax_amount / taxable_value) * 100
        df.at[index, 'Rate'] = gst_rate
        # continue

    elif invoice_value != 0 and gst_rate == 0 and taxable_value == 0 and tax_amount != 0:
        taxable_value = invoice_value - tax_amount
        gst_rate = (tax_amount / taxable_value) * 100
        df.at[index, 'Rate'] = gst_rate
        df.at[index, 'Taxable Value'] = taxable_value
        # continue

    elif invoice_value != 0 and gst_rate == 0 and taxable_value == 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        taxable_value = invoice_value * 100 / (100 + gst_rate)
        df.at[index, 'Rate'] = gst_rate
        df.at[index, 'Taxable Value'] = taxable_value
        # continue

    if invoice_value == 0 and gst_rate != 0 and taxable_value != 0:
        invoice_value = taxable_value + (taxable_value * gst_rate / 100)
        df.at[index, 'Invoice Value'] = invoice_value
        # continue

    if invoice_value == 0 and gst_rate != 0 and taxable_value == 0 and tax_amount != 0:
        taxable_value = tax_amount * 100 / gst_rate
        invoice_value = taxable_value + tax_amount
        df.at[index, 'Invoice Value'] = invoice_value
        df.at[index, 'Taxable Value'] = taxable_value
        # continue

    elif invoice_value == 0 and gst_rate == 0 and taxable_value != 0 and tax_amount != 0:
        gst_rate = (tax_amount / taxable_value) * 100
        invoice_value = taxable_value + tax_amount
        df.at[index, 'Rate'] = gst_rate
        df.at[index, 'Invoice Value'] = invoice_value
        # continue

    elif invoice_value == 0 and gst_rate == 0 and taxable_value != 0 and tax_amount == 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        invoice_value = taxable_value + (taxable_value * gst_rate / 100)
        df.at[index, 'Rate'] = gst_rate
        df.at[index, 'Invoice Value'] = invoice_value
        continue

    elif invoice_value == 0 and gst_rate == 0 and taxable_value == 0 and tax_amount != 0 and gst_rate_combined != 0:
        gst_rate = gst_rate_combined
        taxable_value = tax_amount * 100 / gst_rate
        invoice_value = taxable_value + tax_amount
        df.at[index, 'Rate'] = gst_rate
        df.at[index, 'Taxable Value'] = taxable_value
        df.at[index, 'Invoice Value'] = invoice_value
        # continue

    if tax_amount == 0 and invoice_value != 0 and taxable_value != 0:
        tax_amount = invoice_value - taxable_value
        df.at[index, 'Tax amount'] = tax_amount

    gst_rate = round_to_nearest_zero(gst_rate)

    df.at[index, 'Rate'] = gst_rate

  return df

def create_place_of_origin_column(df):
    df['place_of_origin'] = None

    for index, row in df.iterrows():
        supplier_gstin = row['GSTIN/UIN of Supplier']

        if pd.notna(supplier_gstin) and isinstance(supplier_gstin, str):
            if len(supplier_gstin) >= 2:
                supplier_state_code = supplier_gstin[:2]
                for state in state_codes:
                    if state['code_number'] == supplier_state_code:
                        df.at[index, 'place_of_origin'] = state['code']
                        break

    return df

def fill_place_of_supply_with_place_of_origin(df):
    for index, row in df.iterrows():
        if pd.isna(row['Place Of Supply']):
            df.at[index, 'Place Of Supply'] = row['place_of_origin']
    return df

def categorise_transactions(df):
    df['transaction_type'] = None

    for index, row in df.iterrows():
        gstin_of_recipient = row['GSTIN/UIN of Recipient']
        place_of_supply = row['Place Of Supply']
        invoice_value = row['Invoice Value']
        place_of_origin = row['place_of_origin']
        gst_treatment = row['GST treatment']

        if gst_treatment != 'overseas':
            if pd.notna(gstin_of_recipient):
                df.at[index, 'transaction_type'] = 'b2b'
            elif place_of_supply != place_of_origin and invoice_value > 250000:
                df.at[index, 'transaction_type'] = 'b2cl'
            else:
                df.at[index, 'transaction_type'] = 'b2cs'

    return df

def create_b2b_dataframe(df):
    b2b = df[df['transaction_type'] == 'b2b']
    b2b_columns_needed = ['GSTIN/UIN of Recipient', 'Receiver Name', 'Invoice Number', 'Invoice date',
                          'Invoice Value', 'Place Of Supply', 'Reverse Charge', 'Applicable % of Tax Rate',
                          'Invoice Type', 'E-Commerce GSTIN', 'Rate', 'Taxable Value', 'Cess Amount']
    
    # Ensure all needed columns exist
    for col in b2b_columns_needed:
        if col not in b2b.columns:
            b2b[col] = np.nan
    
    return b2b[b2b_columns_needed]

def create_b2cs_dataframe(df):
    b2cs = df[df['transaction_type'] == 'b2cs'].copy()
    b2cs['Type'] = 'b2cs'
    b2cs['Applicable % of Tax Rate'] = np.nan
    b2cs['E-Commerce GSTIN'] = np.nan
    
    # Now perform the aggregation on the filtered data
    b2cs = b2cs.groupby(['Place Of Supply', 'Rate'])[['Taxable Value', 'Cess Amount']].sum().reset_index()
    
    b2cs_columns_needed = ['Type', 'Place Of Supply', 'Applicable % of Tax Rate', 'Rate', 'Taxable Value', 'Cess Amount', 'E-Commerce GSTIN']
    for col in b2cs_columns_needed:
        if col not in b2cs.columns:
            b2cs[col] = np.nan

    b2cs['Type'] = 'OE'

    b2cs = b2cs[b2cs['Taxable Value']!=0]
    
    return b2cs[b2cs_columns_needed]

def create_b2cl_dataframe(df):
    b2cl = df[df['transaction_type'] == 'b2cl']
    b2cl_columns_needed = ['Invoice Number', 'Invoice date', 'Invoice Value', 'Place Of Supply',
                           'Applicable % of Tax Rate', 'Rate', 'Taxable Value', 'Cess Amount', 'E-Commerce GSTIN']
    
    # Ensure all needed columns exist
    for col in b2cl_columns_needed:
        if col not in b2cl.columns:
            b2cl[col] = np.nan
    
    return b2cl[b2cl_columns_needed]

def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def convert_csv_to_excel(csv_file):
    df = pd.read_csv(csv_file)
    output = io.BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    df.to_excel(writer, index=False, sheet_name='Sheet1')
    writer.close()
    processed_data = output.getvalue()
    return processed_data

def process_meesho_files(uploaded_files):
    # Filter all forward and reverse files
    forward_files = [file for file in uploaded_files if file.name.startswith('tcs_sales.')]
    reverse_files = [file for file in uploaded_files if file.name.startswith('tcs_sales_return')]

    # Ensure that there are both forward and reverse files to process
    if forward_files and reverse_files:
        # Concatenate all forward files
        forward_dfs = [pd.read_excel(file) for file in forward_files]
        forward_df = pd.concat(forward_dfs, ignore_index=True)
        
        # Concatenate all reverse files
        reverse_dfs = [pd.read_excel(file) for file in reverse_files]
        reverse_df = pd.concat(reverse_dfs, ignore_index=True)
        
        # Select the columns to keep
        columns_to_keep = ['gst_rate', 'total_taxable_sale_value', 'end_customer_state_new', 'gstin']
        forward_df = forward_df[columns_to_keep]
        reverse_df = reverse_df[columns_to_keep]

        # Multiply tcs_taxable_amount by -1 in the reverse file
        reverse_df['total_taxable_sale_value'] *= -1

        # Combine the forward and reverse dataframes
        combined_df = pd.concat([forward_df, reverse_df], ignore_index=True)

        # Write the combined dataframe to a temporary excel file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp:
            combined_df.to_excel(tmp.name, index=False, engine='openpyxl')
            tmp_path = tmp.name

        # Read the combined file as bytes for output
        with open(tmp_path, 'rb') as file:
            output = io.BytesIO(file.read())

        # Clean up the temporary file
        os.unlink(tmp_path)

        # Replace forward and reverse files with the combined file in the uploaded files list
        new_uploaded_files = [file for file in uploaded_files if not (file.name.startswith('tcs_sales.') or file.name.startswith('tcs_sales_return'))]
        new_uploaded_files.append(('MeeshoForwardReverse.xlsx', output))

        return new_uploaded_files
    
    # Return the original files if no forward or reverse files were found
    return uploaded_files

def fill_missing_supplier_gstins(df, unique_counter_for_key_names, sheet):
    # Handle edge case: remove rows where all columns are empty
    df = df.dropna(how='all')

    if 'GSTIN/UIN of Supplier' not in df.columns:
        # All rows have missing GSTIN
        supplier_gstin = st.text_input(
            "All rows are missing supplier GSTIN. Please enter the GSTIN of the supplier:",
            key=f"missing_gstin_column_{unique_counter_for_key_names}_{sheet}"
        )

        if supplier_gstin:
            df['GSTIN/UIN of Supplier'] = supplier_gstin
            return df
        else:
            # st.error("Please enter a valid GSTIN for the supplier.")
            st.stop()

    else:
        # Check for rows with missing GSTIN
        df_with_no_gstin = df[df['GSTIN/UIN of Supplier'].isna()]

        if len(df_with_no_gstin) == 0:
            return df  # No missing GSTINs, return original dataframe

        elif len(df) == len(df_with_no_gstin):
            # All rows have missing GSTIN
            supplier_gstin = st.text_input(
                "All rows are missing supplier GSTIN. Please enter the GSTIN of the supplier:",
                key=f"all_missing_gstin_{unique_counter_for_key_names}_{sheet}"
            )

            if supplier_gstin:
                df['GSTIN/UIN of Supplier'] = supplier_gstin
                return df
            else:
                # st.error("Please enter a valid GSTIN for the supplier.")
                st.stop()

        else:
            # Some rows have missing GSTIN
            non_nan_gstins = df['GSTIN/UIN of Supplier'].dropna().unique()
            
            if len(non_nan_gstins) == 1:
                # Only one unique non-NaN GSTIN
                df['GSTIN/UIN of Supplier'].fillna(non_nan_gstins[0], inplace=True)
                return df
            else:
                # Multiple unique non-NaN GSTINs
                nan_count = len(df_with_no_gstin)
                st.error(f"{nan_count} transactions do not have Supplier's GSTIN and multiple GSTINs are presnt in other transactions. Please fill and re-upload.")
                st.stop()

def parse_date_with_format(date_string, date_format):
    try:
        if date_format == "%d-%b-%Y":
            # Custom parsing for all %d-%b-%Y formats
            day, month, year = date_string.split('-')
            month = month[:3].capitalize()  # Normalize to first 3 letters and capitalize
            date_string = f"{day}-{month}-{year}"
        return datetime.strptime(date_string, date_format)
    except ValueError:
        try:
            return parse(date_string)
        except ValueError:
            return pd.NaT
        
def parse_date(date, user_month):
    if pd.isna(date):
        return None  # Return None for missing values
    try:
        parsed_date = parse(str(date), dayfirst=False)  # Parse assuming month/day/year
    except ValueError:
        parsed_date = parse(str(date), dayfirst=True)   # Parse assuming day/month/year
    
    if parsed_date.month == user_month:
        return parsed_date
    else:
        # Swap day and month if the user input doesn't match
        try:
            corrected_date = parsed_date.replace(day=parsed_date.month, month=user_month)
            return corrected_date
        except ValueError:
            return None

# def custom_serializer(obj):
#     if isinstance(obj, pd.Timestamp):
#         return obj.isoformat()  # Convert Timestamp to ISO 8601 string
#     # elif isinstance(obj, datetime.datetime):  # Correctly checking for datetime class
#     #     return obj.isoformat()  # Convert datetime to ISO 8601 string
#     raise TypeError(f"Type {type(obj)} not serializable")

def custom_serializer(obj):
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, (np.int64, np.int32, np.float64, np.float32)):
        return obj.item()
    elif hasattr(obj, '__str__'):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def push_to_es(payload):
    
    elastic_url="https://elastic:NuwRaaWUktq5FM1QJZe6iexV@my-deployment-3eafc9.es.ap-south-1.aws.elastic-cloud.com:9243/#{@index_name}/_doc"

    index_name="gst_excel_app_2"

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
