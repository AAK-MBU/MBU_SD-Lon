"""Module for helper functions"""
import tempfile
import pyodbc
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd

from robot_framework.config import SHAREPOINT_SITE_URL, DOCUMENT_LIBRARY


def find_match_ovk(ovk: str):
    # Some lookup
    match_ovk = ""  

    return match_ovk


def find_pair_info(data: dict, number: int):
    """
    Searches for `number` (int) in `data['pair']`. If found, returns the other element from the pair
    and the corresponding name from `pair_names`.

    Args:
        data (dict): Dictionary with keys:
                     - 'ovk' (str)
                     - 'pair' (tuple of two elements)
                     - 'pair_names' (tuple of two elements)
        number (int): Number to search for in the pair

    Returns:
        tuple: (other_value, corresponding_name) if found, else None
    """
    pair = data.get('pair')
    pair_names = data.get('pair_names')

    if isinstance(pair, tuple) and isinstance(pair_names, tuple) and len(pair) == 2 and len(pair_names) == 2:
        if number in pair:
            index = pair.index(number)
            other_index = 1 - index
            return pair[other_index], pair_names[other_index]

    return None


def get_items_from_query(connection_string, query: str):
    """Executes given sql query and returns rows from its SELECT statement"""

    try:
        with pyodbc.connect(connection_string) as conn:
            with conn.cursor() as cursor:

                cursor.execute(query)

                rows = cursor.fetchall()

                # Get column names from cursor description
                columns = [column[0] for column in cursor.description]

                # Convert to list of dictionaries
                result = [dict(zip(columns, row)) for row in rows]

                cursor.close()
                conn.close()
    except pyodbc.Error as e:
        print(f"Database error: {str(e)}")
        print(f"{connection_string}")
    except ValueError as e:
        print(f"Value error: {str(e)}")
    # pylint: disable-next = broad-exception-caught
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")

    if len(result) == 0:
        return None

    return result


def fetch_files(username, password) -> dict:
    """Download Excel files from SharePoint to the specified path."""

    ctx = ClientContext(SHAREPOINT_SITE_URL).with_credentials(UserCredential(username, password))
    target_folder_url = f"/teams/{SHAREPOINT_SITE_URL.rsplit('/teams/', maxsplit=1)[-1]}/{DOCUMENT_LIBRARY}"
    target_folder = ctx.web.get_folder_by_server_relative_url(target_folder_url)
    files = target_folder.files
    ctx.load(files)
    ctx.execute_query()

    if not files:
        print("No files found in the specified SharePoint folder.")

    file_table = None

    for file in files:
        if file.name == 'Styretabel.xlsx':
            file_content = File.open_binary(ctx, file.serverRelativeUrl)
            with tempfile.NamedTemporaryFile(mode='wb', suffix=".xlsx", delete=True, delete_on_close=False) as temp_file:
                # Write to temp
                temp_file.write(file_content.content)
                temp_file_path = temp_file.name

                # Read into pandas dataframe
                file_table = pd.read_excel(temp_file_path, skiprows=1, usecols=[1, 2, 3, 4])

        else:
            pass

    control_table = {
        row[1].iloc[0]: {
            'worker_type': row[1].iloc[2],
            'worker_data': row[1].iloc[3],
            'process_description': row[1].iloc[1]
        }
        for row in file_table.where(pd.notnull(file_table), None).iterrows()
    }

    return control_table
