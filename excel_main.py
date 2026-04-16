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
from elasticsearch import Elasticsearch  # type: ignore
from mapping_data import known_sources, known_source_relevenat_columns, state_codes, state_mis_match_mapping, needed_columns
from excel_functions import select_columns_from_unknown_source, integers_in_string, gstin_or_state, convert_uploaded_files, select_columns_from_known_source, format_place_of_supply, round_to_nearest_zero, fill_missing_values
from excel_functions import create_place_of_origin_column, fill_place_of_supply_with_place_of_origin, categorise_transactions, create_b2b_dataframe, create_b2cs_dataframe, create_b2cl_dataframe, convert_df_to_csv, convert_csv_to_excel
from excel_functions import push_to_es, process_meesho_files, fill_missing_supplier_gstins, parse_date_with_format, parse_date, custom_serializer

# Streamlit app
def excel_main():

    st.title("GST FILINGS WORKINGS")
    
    upload_file = st.file_uploader("Choose Excel or CSV files", accept_multiple_files=True, type=['xlsx', 'xls', 'csv'])

    if not upload_file:
        st.session_state.clear()

    sources = set()

    qrmp = None

    if 'uploaded_files_info' not in st.session_state:
        st.session_state.uploaded_files_info = {}

    first_date = None
    last_date = None
    uploaded_files_dict = {}
    output_files_dict = {}

    uploaded_files = copy.deepcopy(upload_file)

    # Store the current files in a set for comparison
    current_files_set = {file.name for file in uploaded_files} if uploaded_files else set()

    # If files have been uploaded, store their information and timestamp
    if uploaded_files:
        for file in uploaded_files:
            if file.name not in st.session_state.uploaded_files_info:
                st.session_state.uploaded_files_info[file.name] = {
                    "timestamp": time.time()
                }

    # Remove files from memory that are no longer uploaded
    uploaded_files_names = {file.name for file in uploaded_files} if uploaded_files else set()
    files_to_remove = set(st.session_state.uploaded_files_info.keys()) - uploaded_files_names

    for file_name in files_to_remove:
        del st.session_state.uploaded_files_info[file_name]



    uploaded_files_copy = copy.deepcopy(uploaded_files)

    uploaded_files_dict = convert_uploaded_files(uploaded_files_copy)
    
    if uploaded_files:
        uploaded_files = process_meesho_files(uploaded_files)

        processed_files = []
        for file in uploaded_files:
            if isinstance(file, tuple):  # Combined Meesho file
                file_name, file_content = file
                file_obj = io.BytesIO(file_content.getvalue())
                file_obj.name = file_name
                processed_files.append(file_obj)
            else:  # Original UploadedFile
                if file.name.endswith('.csv'):
                    processed_data = convert_csv_to_excel(file)
                    processed_file = io.BytesIO(processed_data)
                    processed_file.name = file.name.replace('.csv', '.xlsx')
                    processed_files.append(processed_file)
                else:
                    processed_files.append(file)

        # Dropdown (selectbox) with two options, "Not QRMP" selected by default
        is_QRMP = st.checkbox(f"Is QRMP?", value=False)

        # if 'is_QRMP' not in st.session_state:
        #     st.session_state.is_QRMP = st.checkbox(f"Is QRMP?", value=False)


        all_dataframes = []
        for uploaded_file in processed_files:
            st.header(f"Processing: {uploaded_file.name}")
            
            if uploaded_file.name.endswith(('.xlsx', '.xls')):
                excel_file = pd.ExcelFile(uploaded_file)
                sheet_names = excel_file.sheet_names
                selected_sheets = st.multiselect(f"Select relevant sheets from {uploaded_file.name}", sheet_names)

                unique_counter_for_key_names = 0
                
                for sheet in selected_sheets:
                    unique_counter_for_key_names += 1
                    
                    try:
                        df = excel_file.parse(sheet)
                        st.write(f"✓ Read sheet '{sheet}' - {len(df)} rows")
                        
                        is_known_source = st.checkbox(f"Is {sheet} from a known format?", key=f"{uploaded_file.name}_{sheet}_known", value=True)
                        
                        if is_known_source:
                            source = st.selectbox("Select the format", known_sources, index=0, key=f"{uploaded_file.name}_{sheet}_source")

                            if source == "Select an option":
                                st.warning(f"⚠️ Please select a valid format for '{sheet}' to proceed.")
                                continue  # Skip the rest of the loop until a valid selection is made

                            st.write(f"✓ Selected format: {source}")
                            sources.add(source)
                            df = select_columns_from_known_source(df, needed_columns, source)
                            st.write(f"✓ Processed '{sheet}' with known format - {len(df)} rows after column selection")
                            df = fill_missing_supplier_gstins(df, unique_counter_for_key_names, sheet)
                        else:
                            df = select_columns_from_unknown_source(df, needed_columns, uploaded_file.name, sheet)
                            st.write(f"✓ Processed '{sheet}' with unknown format - {len(df)} rows")
                            df = fill_missing_supplier_gstins(df, unique_counter_for_key_names, sheet)
                        
                        if not df.empty:
                            df = format_place_of_supply(df)

                        df = fill_missing_values(df)
                        df = create_place_of_origin_column(df)
                        df = fill_place_of_supply_with_place_of_origin(df)

                        st.write(f"✓ Completed transformations for '{sheet}'")

                        taxable_value = df['Taxable Value'].sum()
                        tax_amount = df['Tax amount'].sum()
                        igst_tax_amount = df[df['Place Of Supply'] != df['place_of_origin']]['Tax amount'].sum()
                        cgst_tax_amount = df[df['Place Of Supply'] == df['place_of_origin']]['Tax amount'].sum()/2
                        sgst_tax_amount = df[df['Place Of Supply'] == df['place_of_origin']]['Tax amount'].sum()/2

                        st.write(f'Summary of {sheet} for TCS')
                        # Assuming the variables are already calculated
                        summary_data = {
                            "Taxable Value": [taxable_value],
                            "IGST Amount": [igst_tax_amount],
                            "CGST Amount": [cgst_tax_amount],
                            "SGST Amount": [sgst_tax_amount]
                        }

                        # Convert the data into a DataFrame
                        summary_df = pd.DataFrame(summary_data)
                        st.table(summary_df)

                        st.write(df['Invoice date'].head(5))

                        if is_QRMP:

                            qrmp = is_QRMP

                            # Date format selection
                            date_format = st.selectbox("Select the date format in your data:", 
                                                    ["%d-%m-%Y", "%m-%d-%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d", "%d-%b-%Y"],key=f"missing_gstin_column_{unique_counter_for_key_names}_{sheet}")
                            
                            # Parse dates
                            df['Invoice date'] = df['Invoice date'].apply(lambda x: parse_date_with_format(str(x), date_format))
                            
                            # Change the format to '01-Jul-2024', handling NaT values gracefully
                            df['Invoice date'] = df['Invoice date'].apply(lambda x: x.strftime('%d-%b-%y') if pd.notna(x) else None)

                            st.write(df['Invoice date'].head(5))



                        all_dataframes.append(df)
                        st.write(f"✓ Added '{sheet}' to processing queue")
                        
                    except Exception as e:
                        st.error(f"❌ Error processing sheet '{sheet}': {str(e)}")
                        st.error(f"Error type: {type(e).__name__}")
                        import traceback
                        st.code(traceback.format_exc())

        if all_dataframes:
            st.success(f"✅ Successfully processed {len(all_dataframes)} sheet(s)")
            main_df = pd.concat(all_dataframes)
            main_df.reset_index(drop=True, inplace=True)
            st.write(f"✓ Combined dataframe: {len(main_df)} total rows")
        else:
            st.warning("⚠️ No data was processed. Please check your sheet selections and formats.")

            # print(main_df)
            # st.write(main_df)
            # print('main_df')
            
            # customer_state_code = st.selectbox("Select the state code of the supplier", 
            #                                    [state['code'] for state in state_codes])
            
            # main_df = fill_missing_values(main_df)
            # main_df = create_place_of_origin_column(main_df, customer_state_code)
            # main_df = fill_place_of_supply_with_place_of_origin(main_df)
            main_df = categorise_transactions(main_df)
                    
            if not is_QRMP:
                # Dropdown to select the month
                month_names = ["January", "February", "March", "April", "May", "June", 
                            "July", "August", "September", "October", "November", "December"]
                current_month = pd.Timestamp.now().month
                previous_month = (current_month - 2) % 12  # Adjusted to select the previous month by default
                user_month = st.selectbox("Select the month of the dates in your data:", month_names, index=previous_month)
                user_month_index = month_names.index(user_month) + 1  # Convert month name to month number

                # Apply the function to the 'Invoice Date' column
                main_df['Invoice date'] = main_df['Invoice date'].apply(lambda x: parse_date(x, user_month_index))

                # Change the format to '01-Jul-2024', handling NaT values gracefully
                main_df['Invoice date'] = main_df['Invoice date'].apply(lambda x: x.strftime('%d-%b-%y') if pd.notna(x) else None)

            main_df['GSTIN/UIN of Supplier'].fillna('supplier gstin not available', inplace=True)

            main_df['GSTIN/UIN of Supplier'] = main_df['GSTIN/UIN of Supplier'].astype(str).str[:15]
            main_df['GSTIN/UIN of Recipient'] = main_df['GSTIN/UIN of Recipient'].astype(str).str[:15]

            main_df['Reverse Charge'] = 'N'
            main_df['Invoice Type'] = 'Regular B2B'

            main_df_copy = copy.deepcopy(main_df)

            main_df_copy['Invoice date'] = pd.to_datetime(main_df_copy['Invoice date'])

            first_date = main_df_copy['Invoice date'].min()
            last_date = main_df_copy['Invoice date'].max()

            unique_gstins = main_df['GSTIN/UIN of Supplier'].unique()

            st.session_state.unique_gstins = unique_gstins

            # Initialize session state variable if it doesn't exist
            if 'button_clicked' not in st.session_state:
                st.session_state.button_clicked = False

            if 'log_pushed' not in st.session_state:
                st.session_state.log_pushed = False
            
            for gstin in unique_gstins:
                st.write(f"### For GSTIN: {gstin}")
                gstin_df = main_df[main_df['GSTIN/UIN of Supplier'] == gstin]

                gstin_df['Taxable Value'] = gstin_df['Taxable Value'].round(2)
                gstin_df['Invoice Value'] = gstin_df['Invoice Value'].round(2)
                gstin_df['Rate'] = gstin_df['Rate'].round(2)
                
                b2b = create_b2b_dataframe(gstin_df)
                b2cs = create_b2cs_dataframe(gstin_df)
                b2cl = create_b2cl_dataframe(gstin_df)
                
                if not b2b.empty:
                    if st.download_button(
                        label=f"B2B",
                        data=convert_df_to_csv(b2b),
                        file_name=f"b2b_output_{gstin}.csv",
                        mime="text/csv",
                    ):
                        st.session_state.button_clicked = True
                
                if not b2cs.empty:
                    if st.download_button(
                        label=f"B2CS",
                        data=convert_df_to_csv(b2cs),
                        file_name=f"b2cs_output_{gstin}.csv",
                        mime="text/csv",
                    ):
                        st.session_state.button_clicked = True
                
                if not b2cl.empty:
                    if st.download_button(
                        label=f"B2CL",
                        data=convert_df_to_csv(b2cl),
                        file_name=f"b2cl_output_{gstin}.csv",
                        mime="text/csv",
                    ):
                        st.session_state.button_clicked = True

            if st.session_state.button_clicked == True:
                if st.session_state.log_pushed == False:

                    st.session_state.log_pushed = True

                    print('after downloading', st.session_state.log_pushed)

                    if sources and ('uploaded_files_info' in st.session_state) and ('unique_gstins' in st.session_state):

                        GSTIN = ""
                        for i in st.session_state.unique_gstins:
                            GSTIN += i
                            GSTIN += ','

                        short_dict = {
                            "timestamp" : int(time.time()),
                            "GSTIN": GSTIN, 
                            "StartDate": first_date, 
                            "EndDate": last_date, 
                            "IsQRMP": qrmp,
                            "Sources": str(sources)
                        }

                        # Push dict directly to Elasticsearch (not JSON string)
                        response = push_to_es(short_dict)
                        
                        # Log the response for debugging
                        if isinstance(response, dict) and "error" in response:
                            st.error(f"Elasticsearch logging failed: {response['error']}")
                        else:
                            st.success(f"Log pushed to Elasticsearch successfully")

                        # Your final dictionary
                        final_dict = {
                            "timestamp" : int(time.time()),
                            "GSTIN": GSTIN, 
                            "InputFileSummary": str(st.session_state.uploaded_files_info),
                            "StartDate": first_date, 
                            "EndDate": last_date, 
                            "IsQRMP": qrmp,
                            "Sources": str(sources),
                            "InputFile" : str(uploaded_files_dict)
                        }

                        # Convert the dictionary to JSON using the custom serializer
                        payload = json.dumps(final_dict, default=custom_serializer)

                        print(payload)

                        for i in range(20):
                            print('')

                        # Send the POST request with the JSON payload
                        url = 'https://crm.vakilsearch.com/es_data_capture'

                        headers = {
                            'Content-Type': 'application/json'
                        }

                        # Send the POST request with the payload directly (not as a string)
                        response = requests.post(url, data=payload, headers=headers)
                        st.write('Second')
