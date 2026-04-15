import pandas as pd
import numpy as np
import streamlit as st # type: ignore
import copy
import json
from datetime import datetime, timedelta

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


def get_b2b_list(b2b_df, gstin):

    # st.write(b2b_df)

    b2b_df['Invoice Value'] = b2b_df['Invoice Value'].round(2)
    b2b_df['Taxable Value'] = b2b_df['Taxable Value'].round(2)
    b2b_df['Cess Amount'] = b2b_df['Cess Amount'].fillna(0)

    # Create an empty list
    b2b_list = []

    unique_ctins = b2b_df['GSTIN/UIN of Recipient'].unique().tolist()

    for ctin in unique_ctins:
        # Filter for particular ctin
        ctin_df = b2b_df[b2b_df['GSTIN/UIN of Recipient']==ctin]

        # define values
        inv_list = get_b2b_inv_list(ctin_df, gstin)

        # Create dict
        ctin_dict = {
            "ctin": ctin,
            "inv": inv_list
        }

        # Add dict to list
        b2b_list.append(ctin_dict)

    return b2b_list


def get_b2b_inv_list(ctin_df, gstin):


    # Create a copy of ctin_df
    ctin = copy.deepcopy(ctin_df)

    # Change date format in ctin
    ctin['Invoice date'] = pd.to_datetime(ctin_df['Invoice date'], format='%d-%b-%y').dt.strftime('%d-%m-%Y')

    # create an empty list
    inv_list = []

    unique_inv = ctin['Invoice Number'].unique().tolist()

    for inv in unique_inv:
        # filter rows for specific invoices
        inv_df = ctin[ctin['Invoice Number']==inv]

        # Define values
        inum = str(inv)
        idt = inv_df['Invoice date'].iloc[0]
        val = float(inv_df['Invoice Value'].iloc[0])
        pos = str(inv_df['Place Of Supply'].iloc[0][:2])
        rchrg = str(inv_df['Reverse Charge'].iloc[0][:1])
        inv_typ = str(inv_df['Invoice Type'].iloc[0][:1])
        item_list = get_b2b_item_list(inv_df, gstin)


        # Create dict
        inv_dict = {
            "inum": inum,
            "idt": idt,
            "val": val,
            "pos": pos,
            "rchrg": rchrg,
            "inv_typ": inv_typ,
            "itms": item_list
        }

        # add dict to list
        inv_list.append(inv_dict)
                        
    return inv_list


def get_b2b_item_list(inv_df, gstin):
    inv_df['Cess Amount'] = inv_df['Cess Amount'].fillna(0)
    # get place of supply & origin
    gst_state = str(gstin[:2])

    # Create an empty list
    item_list = []

    for _, row in inv_df.iterrows():
        pos_state = str(row['Place Of Supply'][:2])
        # Define values
        
        txval = row['Taxable Value']
        rt = int(row['Rate'])
        num = float(f'{rt}01')

        iamt = None
        camt = None
        samt = None

        if gst_state == pos_state:
            camt = txval * rt / 100 / 2
            samt = txval * rt / 100 / 2

            camt = round(camt, 2)
            samt = round(samt, 2)

        else:
            iamt = txval * rt / 100
            iamt = round(iamt, 2)

        csamt = row['Cess Amount']

        # Create dict
        if gst_state == pos_state:
            item_dict = {
                "num": num,
                "itm_det": {
                    "txval": txval,
                    "rt": rt,
                    "camt": camt,
                    "samt": samt,
                    "csamt": csamt
                }
            }
        else:
            item_dict = {
                "num": num,
                "itm_det": {
                    "txval": txval,
                    "rt": rt,
                    "iamt": iamt,
                    "csamt": csamt
                }
            }

        # add dict to list
        item_list.append(item_dict)

    return item_list


def get_b2cs_list(b2cs_df, gstin):
    b2cs_df['Taxable Value'] = b2cs_df['Taxable Value'].round(2)
    # Create empty list
    b2cs_list = []

    for _, row in b2cs_df.iterrows():
        # get place of supply and origin
        gst_state = str(gstin[:2])
        pos_state = str(row['Place Of Supply'][:2])

        # Define values
        if gst_state == pos_state:
            sply_ty = "INTRA"
        else:
            sply_ty = "INTER"

        rt = row['Rate']
        typ = row['Type']
        pos = pos_state
        txval = row['Taxable Value']

        iamt = None
        camt = None
        samt = None

        if gst_state == pos_state:
            camt = txval * rt / 100 / 2
            samt = txval * rt / 100 / 2

            camt = round(camt, 2)
            samt = round(samt, 2)

        else:
            iamt = txval * rt / 100
            iamt = round(iamt, 2)

        csamt = row['Cess Amount']

        # crete dict
        if gst_state == pos_state:
            b2cs_dict = {
                "sply_ty": sply_ty,
                "rt": rt,
                "typ": typ,
                "pos": pos,
                "txval": txval,
                "camt": camt,
                "samt": samt,
                "csamt": csamt
            }
        else:
            b2cs_dict = {
                "sply_ty": sply_ty,
                "rt": rt,
                "typ": typ,
                "pos": pos,
                "txval": txval,
                "iamt": iamt,
                "csamt": csamt
            }

        # add dict to list
        b2cs_list.append(b2cs_dict)

    return b2cs_list


def get_dict(gstin, month, year, b2b, b2cs, version):
    b2b_df = copy.deepcopy(b2b)
    b2cs_df = copy.deepcopy(b2cs)

    # st.write(b2b_df)

    if not b2b_df.empty:
        b2b_list = get_b2b_list(b2b_df, gstin)

    if not b2cs_df.empty:
        b2cs_list = get_b2cs_list(b2cs_df, gstin)

    

    if not b2b_df.empty and not b2cs_df.empty:
        final_dict = {
            "b2b": b2b_list,
            "b2cs": b2cs_list,
            "gstin": gstin,
            "fp": str(month) + str(year),
            "version": f'GST{version}',
            "hash": "hash"
        }

    elif not b2b_df.empty:
        final_dict = {
            "b2b": b2b_list,
            "gstin": gstin,
            "fp": str(month) + str(year),
            "version": f'GST{version}',
            "hash": "hash"
        }
                
    elif not b2cs_df.empty:
        final_dict = {
            "b2cs": b2cs_list,
            "gstin": gstin,
            "fp": str(month) + str(year),
            "version": f'GST{version}',
            "hash": "hash"
        }

    else:
        final_dict = {}

    final_json = json.dumps(final_dict, separators=(",", ":"))

    st.write(final_json)

    return final_json


def extract_gstin(file_name):
    """
    Extract GSTIN from the file name.
    GSTIN is assumed to be the string after the second occurrence of "_".
    """
    try:
        parts = file_name.split("_")
        if len(parts) > 2:
            gstin = parts[2].split(".")[0]  # Get the string after the second "_"
            if len(gstin) == 15:
                return gstin
        return None
    except Exception:
        return None





# 
