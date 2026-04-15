import streamlit as st # type: ignore
import requests #type: ignore
import copy
import pandas as pd
import numpy as np
from datetime import datetime
import zipfile
from io import BytesIO 
import io  # Import io module
import time
from pdf_functions import generate_key, get_month_year, file_to_response_json_santa_fe, file_to_response_json_affine, response_json_to_dataframes, missing_value_check, data_type_check, relation_check, accuracy_check, log_data_in_response_df, log_data_in_response_df_for_no_response, log_data_in_response_df_for_no_dataframes, round_to_nearest_zero, fill_missing_values_line_items_df, log_data_in_output_dataframe, get_file_name, get_listed_files, create_zip, push_to_es, log_data_in_response_df_for_invalid_file, log_data_in_response_df_for_process_error, file_to_response_json_anthropod, log_data_in_response_df_for_failed_response, extract_required_data_from_anthropod

# Function to clear all session state variables
def clear_session_state():
    for key in st.session_state.keys():
        del st.session_state[key]

def pdf_main():

    st.title("Invoice Processing System")

    if 'zip_for_download' not in st.session_state:
        st.session_state.zip_for_download = None

    if 'process_completed' not in st.session_state:
        st.session_state.process_completed = False

    # SantaFe or Affine or Anthropod
    api = 'Affine'

    ticket_id = st.text_input("Enter Ticket ID:")

    try:
        ticket_id = int(ticket_id)
    except:
        ticket_id = None
        st.error("Enter Valid Ticket ID")

    options = ["Sales", "Purchase"]
    type = st.selectbox("Select Type of invoices:", options)

    month, year = get_month_year()

    if ticket_id and type and month and year:

        uploaded_files = st.file_uploader("Choose PDF files", accept_multiple_files=True, type="pdf")

        # Add a Start Process button
        start_process = st.button("Start Process")

        if start_process:
            if not st.session_state.process_completed:

                st.session_state.clear()

                batch_id = generate_key(100)

                final_df = pd.DataFrame()
                response_df = pd.DataFrame(columns=['file_name', 'status_code', 'response_json', 'check_passed', 'step', 'remark'])
                file_container = {}
                all_files = []
                processed_files = []
                failed_files = []
                passed_files = []
                invalid_files = []
                overload_files = []

                error_code_lists = {
                    "NOT_AN_INVOICE": [],
                    "BANK_STATEMENT": [],
                    "CHALLAN_SAMPLE": [],
                    "PAYMENT_RECEIPT_SAMPLE": [],
                    "PAYMENT_RECEIPT": [],
                    "INVALID_PDF": [],
                    "FILE_NOT_FOUND": [],
                    "GSTR_DOCUMENT": [],
                    "SALE_RECEIPT": [],
                    "MULTIPLE_INVOICES": [],
                    "INTERNATIONAL": [],
                    "HANDWRITTEN": [],
                    "EMPTY_CONTENT": [],
                    "NO_TEXT_DETECTED": [],
                    "NA": [],
                    "INVALID_FILES": []
                }

                # Create placeholders for dynamic status updates
                total_files_status = st.empty()
                processed_files_status = st.empty()
                invalid_files_status = st.empty()
                passed_files_status = st.empty()
                failed_files_status = st.empty()
                overload_files_status = st.empty()

                total_files_status.write(f"Total Files: {len(uploaded_files)}")
                processed_files_status.write("Processed Files: 0")
                invalid_files_status.write("Invalid Files: 0")
                passed_files_status.write("Passed Files: 0")
                failed_files_status.write("Failed Files: 0")
                overload_files_status.write("Overload Files: 0")

                # Initialize progress bar
                progress_bar = st.progress(0)

                uploaded_files_copy = copy.deepcopy(uploaded_files)
                total_files = len(uploaded_files_copy)

                for file in uploaded_files_copy:
                    file_data = file.read()
                    file_name = file.name
                    file_container[file_name] = file_data

                for index, file in enumerate(uploaded_files):
                    processed_files.append(file_name)

                    file_name = get_file_name(file)

                    if api == 'SantaFe':
                        response_json, status_code = file_to_response_json_santa_fe(file)
                    elif api == 'Anthropod':
                        response_json, status_code = file_to_response_json_anthropod(file)
                        st.write(response_json)
                    else:
                        status_code, response_json, status = file_to_response_json_affine(file)
                        if status_code == 408:
                            response_df = log_data_in_response_df_for_no_response(response_df, file_name, status_code)
                            # processed_files.append(file_name)
                            failed_files.append(file_name)

                            # Update the status dynamically
                            processed_files_status.write(f"Processed Files: {len(processed_files)}")
                            invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                            passed_files_status.write(f"Passed Files: {len(passed_files)}")
                            failed_files_status.write(f"Failed Files: {len(failed_files)}")
                            overload_files_status.write(f"Overload Files: {len(overload_files)}")

                            # Update progress bar
                            progress_bar.progress((index + 1) / total_files)

                            payload = {
                                "ticketId": ticket_id,
                                "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                "batchId": batch_id,
                                "eventType": "fileProcess",
                                "api": api,
                                "type": type,
                                "filingMonth": month,
                                "filingYear": year,
                                "fileName": file_name,
                                "totalFilesInBatch": len(uploaded_files),
                                "check": {
                                    "passed": False,
                                    "step": 'timeout',
                                    "remark": "No response"
                                },
                                "response": {}
                            }

                            log_response = push_to_es(payload)

                            continue


                    print('')
                    print('')
                    print('')
                    print('')

                    try:
                        if not response_json:
                            response_df = log_data_in_response_df_for_no_response(response_df, file_name, status_code)
                            # processed_files.append(file_name)
                            failed_files.append(file_name)

                            # Update the status dynamically
                            processed_files_status.write(f"Processed Files: {len(processed_files)}")
                            invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                            passed_files_status.write(f"Passed Files: {len(passed_files)}")
                            failed_files_status.write(f"Failed Files: {len(failed_files)}")
                            overload_files_status.write(f"Overload Files: {len(overload_files)}")

                            # Update progress bar
                            progress_bar.progress((index + 1) / total_files)

                            payload = {
                                "ticketId": ticket_id,
                                "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                "batchId": batch_id,
                                "eventType": "fileProcess",
                                "api": api,
                                "type": type,
                                "filingMonth": month,
                                "filingYear": year,
                                "fileName": file_name,
                                "totalFilesInBatch": len(uploaded_files),
                                "check": {
                                    "passed": False,
                                    "step": None,
                                    "remark": "No response"
                                },
                                "response": {}
                            }

                            log_response = push_to_es(payload)

                            continue


                        # Check if the status code is 435 or 436
                        if str(status_code) in ('435', '436'):
                            # processed_files.append(file_name)
                            invalid_files.append(file_name)
                            
                            # Get the error code from the response and append to the appropriate list
                            error_code = response_json.get("error_code")
                            if error_code == None:
                                error_code = response_json.get("errorcode")

                            message = response_json.get('message')

                            if error_code not in error_code_lists:
                                error_code_lists[error_code] = []

                            error_code_lists[error_code].append(file_name)

                            response_df = log_data_in_response_df_for_invalid_file(response_df, file_name, status_code, error_code, message)

                            # Update the status dynamically
                            processed_files_status.write(f"Processed Files: {len(processed_files)}")
                            invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                            passed_files_status.write(f"Passed Files: {len(passed_files)}")
                            failed_files_status.write(f"Failed Files: {len(failed_files)}")
                            overload_files_status.write(f"Overload Files: {len(overload_files)}")

                            # Update progress bar
                            progress_bar.progress((index + 1) / total_files)

                            payload = {
                                "ticketId": ticket_id,
                                "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                "batchId": batch_id,
                                "eventType": "fileProcess",
                                "api": api,
                                "type": type,
                                "filingMonth": month,
                                "filingYear": year,
                                "fileName": file_name,
                                "totalFilesInBatch": len(uploaded_files),
                                "check": {
                                    "passed": False,
                                    "step": 'invalid_file',
                                    "remark": error_code
                                },
                                "response": {}
                            }

                            log_response = push_to_es(payload)

                            continue

                        if api == 'Anthropod' and status_code != 200:
                            if status_code == 429:
                                response_df = log_data_in_response_df_for_failed_response(response_df, file_name, status_code, response_json)
                                # processed_files.append(file_name)
                                overload_files.append(file_name)

                                # Update the status dynamically
                                processed_files_status.write(f"Processed Files: {len(processed_files)}")
                                invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                                passed_files_status.write(f"Passed Files: {len(passed_files)}")
                                failed_files_status.write(f"Failed Files: {len(failed_files)}")
                                overload_files_status.write(f"Overload Files: {len(overload_files)}")

                                # Update progress bar
                                progress_bar.progress((index + 1) / total_files)

                                payload = {
                                    "ticketId": ticket_id,
                                    "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                    "batchId": batch_id,
                                    "eventType": "fileProcess",
                                    "api": api,
                                    "type": type,
                                    "filingMonth": month,
                                    "filingYear": year,
                                    "fileName": file_name,
                                    "totalFilesInBatch": len(uploaded_files),
                                    "check": {
                                        "passed": False,
                                        "step": None,
                                        "remark": "No response due to overload"
                                    },
                                    "response": {}
                                }

                                log_response = push_to_es(payload)

                                continue
                            else:
                                response_df = log_data_in_response_df_for_failed_response(response_df, file_name, status_code, response_json)
                                # processed_files.append(file_name)
                                failed_files.append(file_name)

                                # Update the status dynamically
                                processed_files_status.write(f"Processed Files: {len(processed_files)}")
                                invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                                passed_files_status.write(f"Passed Files: {len(passed_files)}")
                                failed_files_status.write(f"Failed Files: {len(failed_files)}")
                                overload_files_status.write(f"Overload Files: {len(overload_files)}")

                                # Update progress bar
                                progress_bar.progress((index + 1) / total_files)

                                payload = {
                                    "ticketId": ticket_id,
                                    "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                    "batchId": batch_id,
                                    "eventType": "fileProcess",
                                    "api": api,
                                    "type": type,
                                    "filingMonth": month,
                                    "filingYear": year,
                                    "fileName": file_name,
                                    "totalFilesInBatch": len(uploaded_files),
                                    "check": {
                                        "passed": False,
                                        "step": None,
                                        "remark": "Failed response"
                                    },
                                    "response": {}
                                }

                                log_response = push_to_es(payload)

                                continue

                        if api == 'Anthropod' and status_code == 200:
                            type_value = response_json.get("doc_analytics", {}).get("invoices", [])[0].get("type", "Unknown")

                            if type_value == 'INVALID':
                                # processed_files.append(file_name)
                                invalid_files.append(file_name)
                                
                                # Get the error code from the response and append to the appropriate list
                                invalid_type = response_json.get("doc_analytics", {}).get("invoices", [])[0].get("invalid_type", "Unknown")

                                if invalid_type not in error_code_lists:
                                    error_code_lists[invalid_type] = []

                                error_code_lists[invalid_type].append(file_name)

                                response_df = log_data_in_response_df_for_invalid_file(response_df, file_name, status_code, invalid_type, invalid_type)

                                # Update the status dynamically
                                processed_files_status.write(f"Processed Files: {len(processed_files)}")
                                invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                                passed_files_status.write(f"Passed Files: {len(passed_files)}")
                                failed_files_status.write(f"Failed Files: {len(failed_files)}")
                                overload_files_status.write(f"Overload Files: {len(overload_files)}")

                                # Update progress bar
                                progress_bar.progress((index + 1) / total_files)

                                payload = {
                                    "ticketId": ticket_id,
                                    "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                    "batchId": batch_id,
                                    "eventType": "fileProcess",
                                    "api": api,
                                    "type": type,
                                    "filingMonth": month,
                                    "filingYear": year,
                                    "fileName": file_name,
                                    "totalFilesInBatch": len(uploaded_files),
                                    "check": {
                                        "passed": False,
                                        "step": 'invalid_file',
                                        "remark": invalid_type
                                    },
                                    "response": {}
                                }

                                log_response = push_to_es(payload)

                                continue
                                
                            elif type_value == 'VALID - INTERNATIONAL':
                                # processed_files.append(file_name)
                                invalid_files.append(file_name)
                                
                                error_code_lists['INTERNATIONAL'].append(file_name)

                                response_df = log_data_in_response_df_for_invalid_file(response_df, file_name, status_code, 'INTERNATIONAL', 'INTERNATIONAL')

                                # Update the status dynamically
                                processed_files_status.write(f"Processed Files: {len(processed_files)}")
                                invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                                passed_files_status.write(f"Passed Files: {len(passed_files)}")
                                failed_files_status.write(f"Failed Files: {len(failed_files)}")
                                overload_files_status.write(f"Overload Files: {len(overload_files)}")

                                # Update progress bar
                                progress_bar.progress((index + 1) / total_files)

                                payload = {
                                    "ticketId": ticket_id,
                                    "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                    "batchId": batch_id,
                                    "eventType": "fileProcess",
                                    "api": api,
                                    "type": type,
                                    "filingMonth": month,
                                    "filingYear": year,
                                    "fileName": file_name,
                                    "totalFilesInBatch": len(uploaded_files),
                                    "check": {
                                        "passed": False,
                                        "step": 'invalid_file',
                                        "remark": invalid_type
                                    },
                                    "response": {}
                                }

                                log_response = push_to_es(payload)

                                continue

                            elif type_value == 'VALID - DOMESTIC':
                                response_json = extract_required_data_from_anthropod(response_json)

                        invoice_df, line_items_df, total_summary_df = response_json_to_dataframes(response_json, api)

                        if invoice_df.empty or line_items_df.empty or total_summary_df.empty:
                            response_df = log_data_in_response_df_for_no_dataframes(response_df, file_name, response_json, status_code)
                            # processed_files.append(file_name)
                            failed_files.append(file_name)

                            # Update the status dynamically
                            processed_files_status.write(f"Processed Files: {len(processed_files)}")
                            invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                            passed_files_status.write(f"Passed Files: {len(passed_files)}")
                            failed_files_status.write(f"Failed Files: {len(failed_files)}")
                            overload_files_status.write(f"Overload Files: {len(overload_files)}")

                            # Update progress bar
                            progress_bar.progress((index + 1) / total_files)

                            payload = {
                                "ticketId": ticket_id,
                                "timestamp": int(datetime.now().timestamp() * 1_000_000),
                                "batchId": batch_id,
                                "eventType": "fileProcess",
                                "api": api,
                                "type": type,
                                "filingMonth": month,
                                "filingYear": year,
                                "fileName": file_name,
                                "totalFilesInBatch": len(uploaded_files),
                                "check": {
                                    "passed": False,
                                    "step": None,
                                    "remark": "Unknown JSON structure"
                                },
                                "response": response_json
                            }

                            log_response = push_to_es(payload)

                            continue

                        check, step, remark, invoice_df, line_items_df, total_summary_df = accuracy_check(invoice_df, line_items_df, total_summary_df)

                        response_df = log_data_in_response_df(response_df, file_name, response_json, check, step, remark, status_code)

                        if not check:
                            failed_files.append(file_name)
                        else:
                            passed_files.append(file_name)

                            line_items_df = fill_missing_values_line_items_df(line_items_df)
                            final_df = log_data_in_output_dataframe(invoice_df, line_items_df, total_summary_df, final_df)

                        # processed_files.append(file_name)
                        all_files.append(file_name)

                        # Update the status dynamically
                        processed_files_status.write(f"Processed Files: {len(processed_files)}")
                        invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                        passed_files_status.write(f"Passed Files: {len(passed_files)}")
                        failed_files_status.write(f"Failed Files: {len(failed_files)}")
                        overload_files_status.write(f"Overload Files: {len(overload_files)}")

                        # Update progress bar
                        progress_bar.progress((index + 1) / total_files)

                        payload = {
                            "ticketId": ticket_id,
                            "timestamp": int(datetime.now().timestamp() * 1_000_000),
                            "batchId": batch_id,
                            "eventType": "fileProcess",
                            "api": api,
                            "type": type,
                            "filingMonth": month,
                            "filingYear": year,
                            "fileName": file_name,
                            "totalFilesInBatch": len(uploaded_files),
                            "check": {
                                "passed": check,
                                "step": step,
                                "remark": remark
                            },
                            "response": response_json
                        }

                        log_response = push_to_es(payload)

                        print(log_response)


                    except Exception as e:
                        # processed_files.append(file_name)
                        all_files.append(file_name)
                        failed_files.append(file_name)

                        response_df = log_data_in_response_df_for_process_error(response_df, file_name, response_json, e, status_code)

                        # Update the status dynamically
                        processed_files_status.write(f"Processed Files: {len(processed_files)}")
                        invalid_files_status.write(f"Invalid Files: {len(invalid_files)}")
                        passed_files_status.write(f"Passed Files: {len(passed_files)}")
                        failed_files_status.write(f"Failed Files: {len(failed_files)}")
                        overload_files_status.write(f"Overload Files: {len(overload_files)}")

                        # Update progress bar
                        progress_bar.progress((index + 1) / total_files)

                        payload = {
                            "ticketId": ticket_id,
                            "timestamp": int(datetime.now().timestamp() * 1_000_000),
                            "batchId": batch_id,
                            "eventType": "fileProcess",
                            "api": api,
                            "type": type,
                            "filingMonth": month,
                            "filingYear": year,
                            "fileName": file_name,
                            "totalFilesInBatch": len(uploaded_files),
                            "check": {
                                "passed": False,
                                "step": None,
                                "remark": None
                            },
                            "response": response_json
                        }

                        log_response = push_to_es(payload)

                        print(log_response)


                file_name_dict = {
                    "all_files": all_files,
                    "processed_files": processed_files,
                    "invalid_files": invalid_files,
                    "failed_files": failed_files,
                    "passed_files": passed_files,
                    "overload_files": overload_files
                }

                # Add only non-blank error code lists to the final dictionary
                non_blank_error_code_lists = {key: value for key, value in error_code_lists.items() if value}
                file_name_dict.update(non_blank_error_code_lists)

                st.session_state.zip_for_download = create_zip(file_container, file_name_dict, final_df, response_df, non_blank_error_code_lists)

                st.session_state.download_payload = {
                    "ticketId": ticket_id,
                    "timestamp": int(datetime.now().timestamp() * 1_000_000),
                    "batchId": batch_id,
                    "eventType": "Download",
                    "api": api,
                    "type": type,
                    "filingMonth": month,
                    "filingYear": year,
                    "fileName": None,
                    "totalFilesInBatch": len(uploaded_files),
                    "check": {
                        "passed": None,
                        "step": None,
                        "remark": None
                    },
                    "response": {}
                }

                st.session_state.process_completed = True

        if st.session_state.zip_for_download:
            # Provide download button
            if st.download_button(
                label="Download ZIP File",
                data=st.session_state.zip_for_download,
                file_name="output_files.zip",
                mime="application/zip"
            ):
                download_log_response = push_to_es(st.session_state.download_payload)
