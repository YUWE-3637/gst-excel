# Define necessary data structures
known_sources = ['Select an option', 'Zoho Books B2B,Export Sales Data', 'Kithab Sales Report', 'Amazon', 'Flipkart - 7(A)(2)',
                 'Flipkart - 7(B)(2)', 'Meesho','b2b ready to file format','b2cs ready to file format','VS internal format',
                 'Amazon B2B','Vyapaar','Jio Mart', 'PDF extractor']

known_source_relevenat_columns = {
      'Zoho Books B2B,Export Sales Data': {
          'GST Identification Number (GSTIN)' : 'GSTIN/UIN of Recipient',
          'Customer Name' : 'Receiver Name',
          'Supplier GST Registration Number' : 'GSTIN/UIN of Supplier',
          'Invoice Number' : 'Invoice Number',
          'Invoice Date' : 'Invoice date',
          'Total' : 'Invoice Value',
          'Place of Supply(With State Code)' : 'Place Of Supply',
          'Item Tax %' : 'Rate',
          'SubTotal' : 'Taxable Value',
          'Item Tax Amount' : 'Tax amount',
          'GST Treatment' : 'GST treatment'
      },
      'PDF extractor': {
        'gstin_recipient': 'GSTIN/UIN of Recipient',
        'receiver_name': 'Receiver Name',
        'gstin_supplier': 'GSTIN/UIN of Supplier',
        'invoice_number': 'Invoice Number',
        'invoice_date': 'Invoice date',
        'invoice_value': 'Invoice Value',
        'place_of_supply': 'Place Of Supply',
        'tax_rate': 'Rate',
        'taxable_value': 'Taxable Value'  
      },
      "HSN ready to file": {
          "HSN":"HSN",
          "Description":"Description",
          "UQC":"UQC",
          "Total Quantity":"Total Quantity",
          "Rate":"Rate",
          "Total Value":"Total Value",
          "Taxable Value":"Taxable Value",
          "Integrated Tax Amount":"Integrated Tax Amount",
          "Central Tax Amount":"Central Tax Amount",
          "State/UT Tax Amount":"State/UT Tax Amount",
          "Cess Amount":"Cess Amount"
      },
      "Flipkart HSN": {
          "HSN Number":"HSN",
          "Total Quantity in Nos.":"Total Quantity",
          "Total Value Rs.":"Total Value",
          "Total Taxable Value Rs.":"Taxable Value",
          "IGST Amount Rs.":"Igst Amount",
          "CGST Amount Rs.":"Cgst Amount",
          "SGST Amount Rs.":"Sgst Amount",
          "Cess Rs.":"Cess Amount"
      },
      'Jio Mart': {
          'Seller GSTIN' : 'GSTIN/UIN of Supplier',
          "Customer's Delivery State" : 'Place Of Supply',
          'CGST Rate' : 'Cgst Rate',
          'SGST Rate (or UTGST as applicable)' : 'Sgst Rate',
          'IGST Rate' : 'Igst Rate',
          'Taxable Value (Final Invoice Amount -Taxes)' : 'Taxable Value'
      },
      'Amazon B2B': {
          'Customer Bill To Gstid' : 'GSTIN/UIN of Recipient',
          'Buyer Name' : 'Receiver Name',
          'Seller Gstin' : 'GSTIN/UIN of Supplier',
          'Invoice Number' : 'Invoice Number',
          'Invoice Date' : 'Invoice date',
          'Invoice Amount' : 'Invoice Value',
          'Ship To State' : 'Place Of Supply',
          'Tax Exclusive Gross' : 'Taxable Value',
          'Cgst Rate' : 'Cgst Rate',
          'Sgst Rate' : 'Sgst Rate',
          'Utgst Rate' : 'Utgst Rate',
          'Igst Rate' : 'Igst Rate',
          'Igst Tax': 'Igst Amount',
          'Cgst Tax': 'Cgst Amount',
          'Sgst Tax': 'Sgst Amount',
          'Utgst Tax': 'Ugst Amount'
      },
    #   'Cgst Amount', 'Sgst Amount', 'Igst Amount', 'Ugst Amount',
      'VS internal format': {
          'gstin' : 'GSTIN/UIN of Recipient',
          'Name of Customer' : 'Receiver Name',
          'Invoice No' : 'Invoice Number',
          'Date' : 'Invoice date',
          'Invoice Total (Rs.)' : 'Invoice Value',
          'state' : 'Place Of Supply',
          'Rate of tax (%)' : 'Rate',
          'Invoice Base Amount (Rs.)' : 'Taxable Value',
          'CGST (Rs.)' : 'Cgst Amount',
          'SGST (Rs.)' : 'Sgst Amount',
          'IGST (Rs.)' : 'Igst Amount'
      },
      'b2b ready to file format': {
          'GSTIN/UIN of Recipient' : 'GSTIN/UIN of Recipient',
          'Receiver Name' : 'Receiver Name',
          'Invoice Number' : 'Invoice Number',
          'Invoice date' : 'Invoice date',
          'Invoice Value' : 'Invoice Value',
          'Place Of Supply' : 'Place Of Supply',
          'Rate' : 'Rate',
          'Taxable Value' : 'Taxable Value'
      },
      'b2cs ready to file format': {
          'Place Of Supply' : 'Place Of Supply',
          'Rate' : 'Rate',
          'Taxable Value' : 'Taxable Value'
      },
      'Kithab Sales Report': {
          'GSTIN Number' : 'GSTIN/UIN of Recipient',
          'Customer name' : 'Receiver Name',
          'Invoice Number' : 'Invoice Number',
          'Invoice Date' : 'Invoice date',
          'GST Rate' : 'Rate',
          'Item Price' : 'Taxable Value'
      },
      'Amazon': {
          'Seller Gstin' : 'GSTIN/UIN of Supplier',
          'Invoice Number' : 'Invoice Number',
          'Invoice Date' : 'Invoice date',
          'Invoice Amount' : 'Invoice Value',
          'Ship To State' : 'Place Of Supply',
          'Tax Exclusive Gross' : 'Taxable Value',
          'Total Tax Amount' : 'Tax amount',
          'Cgst Rate' : 'Cgst Rate',
          'Sgst Rate' : 'Sgst Rate',
          'Utgst Rate' : 'Utgst Rate',
          'Igst Rate' : 'Igst Rate',
          'Igst Tax': 'Igst Amount',
          'Cgst Tax': 'Cgst Amount',
          'Sgst Tax': 'Sgst Amount',
          'Utgst Tax': 'Ugst Amount'
      },
      'Meesho': {
          'gstin' : 'GSTIN/UIN of Supplier',
          'end_customer_state_new' : 'Place Of Supply',
          'gst_rate' : 'Rate',
          'total_taxable_sale_value' : 'Taxable Value'
      },
      'Flipkart - 7(A)(2)': {
          'GSTIN' : 'GSTIN/UIN of Supplier',
          'Aggregate Taxable Value Rs.' : 'Taxable Value',
          'CGST %' : 'Cgst Rate',
          'SGST/UT %' : 'Sgst Rate'
      },
      'Flipkart - 7(B)(2)':{
          'GSTIN' : 'GSTIN/UIN of Supplier',
          'Delivered State (PoS)' : 'Place Of Supply',
          'IGST %' : 'Rate',
          'Aggregate Taxable Value Rs.' : 'Taxable Value',
          'IGST Amount Rs.' : 'Tax amount'
      }
  }

state_codes = [
    {
        "State": "Andaman and Nicobar Islands",
        "code": "35-Andaman and Nicobar Islands",
        "code_number": "35"
    },
    {
        "State": "Andhra Pradesh",
        "code": "37-Andhra Pradesh",
        "code_number": "37"
    },
    {
        "State": "Arunachal Pradesh",
        "code": "12-Arunachal Pradesh",
        "code_number": "12"
    },
    {
        "State": "Assam",
        "code": "18-Assam",
        "code_number": "18"
    },
    {
        "State": "Bihar",
        "code": "10-Bihar",
        "code_number": "10"
    },
    {
        "State": "Chandigarh",
        "code": "04-Chandigarh",
        "code_number": "04"
    },
    {
        "State": "Chhattisgarh",
        "code": "22-Chhattisgarh",
        "code_number": "22"
    },
    {
        "State": "Dadra and Nagar Haveli and Daman and Diu",
        "code": "26-Dadra and Nagar Haveli and Daman and Diu",
        "code_number": "26"
    },
    {
        "State": "Daman and Diu",
        "code": "25-Daman and Diu",
        "code_number": "25"
    },
    {
        "State": "Delhi",
        "code": "07-Delhi",
        "code_number": "07"
    },
    {
        "State": "Goa",
        "code": "30-Goa",
        "code_number": "30"
    },
    {
        "State": "Gujarat",
        "code": "24-Gujarat",
        "code_number": "24"
    },
    {
        "State": "Haryana",
        "code": "06-Haryana",
        "code_number": "06"
    },
    {
        "State": "Himachal Pradesh",
        "code": "02-Himachal Pradesh",
        "code_number": "02"
    },
    {
        "State": "Jammu and Kashmir",
        "code": "01-Jammu and Kashmir",
        "code_number": "01"
    },
    {
        "State": "Jharkhand",
        "code": "20-Jharkhand",
        "code_number": "20"
    },
    {
        "State": "Karnataka",
        "code": "29-Karnataka",
        "code_number": "29"
    },
    {
        "State": "Kerala",
        "code": "32-Kerala",
        "code_number": "32"
    },
    {
        "State": "Ladakh",
        "code": "38-Ladakh",
        "code_number": "38"
    },
    {
        "State": "Lakshadweep",
        "code": "31-Lakshadweep",
        "code_number": "31"
    },
    {
        "State": "Madhya Pradesh",
        "code": "23-Madhya Pradesh",
        "code_number": "23"
    },
    {
        "State": "Maharashtra",
        "code": "27-Maharashtra",
        "code_number": "27"
    },
    {
        "State": "Manipur",
        "code": "14-Manipur",
        "code_number": "14"
    },
    {
        "State": "Meghalaya",
        "code": "17-Meghalaya",
        "code_number": "17"
    },
    {
        "State": "Mizoram",
        "code": "15-Mizoram",
        "code_number": "15"
    },
    {
        "State": "Nagaland",
        "code": "13-Nagaland",
        "code_number": "13"
    },
    {
        "State": "Odisha",
        "code": "21-Odisha",
        "code_number": "21"
    },
    {
        "State": "Other Territory",
        "code": "97-Other Territory",
        "code_number": "97"
    },
    {
        "State": "Puducherry",
        "code": "34-Puducherry",
        "code_number": "34"
    },
    {
        "State": "Punjab",
        "code": "03-Punjab",
        "code_number": "03"
    },
    {
        "State": "Rajasthan",
        "code": "08-Rajasthan",
        "code_number": "08"
    },
    {
        "State": "Sikkim",
        "code": "11-Sikkim",
        "code_number": "11"
    },
    {
        "State": "Tamil Nadu",
        "code": "33-Tamil Nadu",
        "code_number": "33"
    },
    {
        "State": "Telangana",
        "code": "36-Telangana",
        "code_number": "36"
    },
    {
        "State": "Tripura",
        "code": "16-Tripura",
        "code_number": "16"
    },
    {
        "State": "Uttar Pradesh",
        "code": "09-Uttar Pradesh",
        "code_number": "09"
    },
    {
        "State": "Uttarakhand",
        "code": "05-Uttarakhand",
        "code_number": "05"
    },
    {
        "State": "West Bengal",
        "code": "19-West Bengal",
        "code_number": "19"
    }
]

state_mis_match_mapping = {
    "AP": "Andhra Pradesh",
    "AN": "Andaman and Nicobar Islands",
    "AR": "Arunachal Pradesh",
    "AS": "Assam",
    "BR": "Bihar",
    "CG": "Chattisgarh",
    "CH": "Chandigarh",
    "DN": "Dadra and Nagar Haveli and Daman and Diu",
    "DD": "Dadra and Nagar Haveli and Daman and Diu",
    "DL": "Delhi",
    "GA": "Goa",
    "GJ": "Gujarat",
    "HR": "Haryana",
    "HP": "Himachal Pradesh",
    "JK": "Jammu and Kashmir",
    "JH": "Jharkhand",
    "KA": "Karnataka",
    "KL": "Kerala",
    "LA": "Ladakh",
    "LD": "Lakshadweep",
    "MP": "Madhya Pradesh",
    "MH": "Maharashtra",
    "MN": "Manipur",
    "ML": "Meghalaya",
    "MZ": "Mizoram",
    "NL": "Nagaland",
    "OD": "Odisha",
    "PY": "Pondicherry",
    "PB": "Punjab",
    "RJ": "Rajasthan",
    "SK": "Sikkim",
    "TN": "Tamil Nadu",
    "TS": "Telangana",
    "TR": "Tripura",
    "UP": "Uttar Pradesh",
    "UK": "Uttarakhand",
    "WB": "West Bengal",
    "UA": "Uttarakhand",
    "OR": "Odisha",
    "UT": "Uttarakhand",
    "Puducherry": "Pondicherry",
    "The Andaman and Nicobar Islands": "Andaman and Nicobar Islands",
    "Andaman & Nicobar Islands": "Andaman and Nicobar Islands",
    "The Andaman & Nicobar Islands": "Andaman and Nicobar Islands",
    "Orisha": "Odisha",
    "Oddisha": "Odisha",
    "Orrisha": "Odisha",
    "PONDICHERRY": 'Pondicherry',
    "JAMMU & KASHMIR": 'Jammu and Kashmir'

}

needed_columns = [
    'GSTIN/UIN of Recipient', 'Receiver Name', 'GSTIN/UIN of Supplier', 'Invoice Number', 'Invoice date',
    'Invoice Value', 'Place Of Supply', 'Rate', 'Taxable Value', 'Tax amount', 'GST treatment', 'Invoice Type',
    'E-Commerce GSTIN', 'Cess Amount', 'Cgst Rate', 'Sgst Rate', 'Utgst Rate', 'Igst Rate', 'CESS Rate',
    'Cgst Amount', 'Sgst Amount', 'Igst Amount', 'Ugst Amount'
]








# 
