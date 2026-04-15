import pandas as pd
import numpy as np
import streamlit as st # type: ignore
import copy
import json
from json_functions import get_b2b_list, get_b2b_inv_list, get_b2b_item_list, get_b2cs_list, get_dict, get_month_year, extract_gstin

def json_main():
    st.title('JSON creator')
    # File uploader
    uploaded_files = st.file_uploader(
        "Upload b2b & b2cs files",
        accept_multiple_files=True,
        type=['csv'],
        help="Upload a maximum of two files (one for b2b and one for b2cs)"
    )
    
    if len(uploaded_files) > 2:
        st.error("You can upload a maximum of two files only.")
        return
    
    if uploaded_files:
        # Initialize variables
        b2b = pd.DataFrame()
        b2cs = pd.DataFrame()
        gstins = []
        file_mapping = {}
        # Get month and year
        month, year = get_month_year()
        version = st.text_input("Enter a value:", value="2.2")

        # Process uploaded files
        for uploaded_file in uploaded_files:
            file_name = uploaded_file.name
            gstin = extract_gstin(file_name)
            if gstin:
                gstins.append(gstin)

            # Ask user to specify file type if name does not indicate b2b or b2cs
            if file_name.startswith("b2b"):
                b2b = pd.read_csv(uploaded_file)
            elif file_name.startswith("b2cs"):
                b2cs = pd.read_csv(uploaded_file)
            else:
                file_type = st.selectbox(
                    f"Select the type of file for {file_name}:",
                    options=["b2b", "b2cs"],
                    key=file_name
                )
                file_mapping[file_name] = file_type
                if file_type == "b2b":
                    b2b = pd.read_csv(uploaded_file)
                elif file_type == "b2cs":
                    b2cs = pd.read_csv(uploaded_file)

        # Validate GSTINs
        if not gstins or len(set(gstins)) != 1:
            # st.warning("Could not extract a valid GSTIN from the uploaded files or GSTINs do not match.")
            gstin = st.text_input("Please enter the Supplier's GSTIN manually:")
            if gstin and len(gstin) == 15:
                st.success(f"Supplier's GSTIN set to: {gstin}")
            else:
                # st.error("Invalid GSTIN entered. Please try again.")
                return
        else:
            gstin = gstins[0]
            # st.success(f"Supplier's GSTIN extracted: {supplier_gstin}")
            st.text_input("Change Supplier's GSTIN if required:", value=gstin)

        # st.write(b2b)

        if not b2b.empty or not b2cs.empty:
            final_json = get_dict(gstin, month, year, b2b, b2cs, version)


            # Provide a download button for the JSON file
            st.download_button(
                label="Download JSON File",
                data=final_json,
                file_name=f"{gstin}_{month}-{year}.json",
                mime="application/json"
            )
