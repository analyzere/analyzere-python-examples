Bulk Upload Example in Python
==============================
============
Overview
============
The Python Bulk Upload Example demonstrates how Python can be leveraged to retrieve data from sources like CSV and SQL and upload them to the Analyze Re platform.

The tool has three main components:

1. *Data Retriever*
       Retrieves the bulk layer and loss data from CSV or SQL.
2. *Data Parser*
       Parses the bulk data and modifies it as per Analyze Re's expectations.
3. *Data Uploader*
        Uploads data to the platform using the Analyze Re Python library.

============
Setup
============

1. Clone the git repo  
2. Change into the repo
    ``cd are_bulk_uploader``
3. Create a virtual environment
    ``python -m venv venv_name``
4. Activate the virtual environment
    On Linux,
           ``source venv_name/bin/activate``
    On Windows,
          ``cd venv\Scripts``
          followed by
          ``Source venv_name\Scripts> activate``
5. Install required packages from the *requirements.txt* file
     ``pip install -r requirements.txt``
6.  Map the input Layer and Loss column names in the *config.ini* file under the **[bulk_layer_terms]** and **[bulk_loss_set]** sections.

============
Usage
============
The command-line tool can accept the following arguments:
 ``bulk_upload.py [--url URL] [--user USER] [--password PASSWORD] {upload_from_csv,upload_from_sql}``

*url*, *user*, *password* are optional. The server details from the *config.ini* are used by default. 

It is mandatory to provide either *upload_from_csv* or *upload_from_sql* as part of the command.

Bulk Upload from SQL
^^^^^^^^^^^^^^^^^^^^^
In order to extract data from SQL, the command can be formatted as:
 ``python bulk_upload.py upload_from_sql``

The SQL configuration in the *config.ini* file would be used for establishing the connection to the database.

Bulk Upload from CSV
^^^^^^^^^^^^^^^^^^^^^
In order to extract data from CSV, the command can be formatted as:
 ``python bulk_upload.py upload_from_csv  --layer path/to/layer_file_name.csv  [--elt| --ylt | --yelt] path/to/loss_file_name.csv``

For example,

 ``python bulk_upload.py upload_from_csv  --layer Contracts_Sample.csv  --yelt YLT_Sample.csv``
