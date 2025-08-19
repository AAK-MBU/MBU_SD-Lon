"""Module for helper functions"""
import pyodbc
from datetime import date


def format_item(item: dict):
    return {
        key: value.strftime("%d-%m-%Y") if isinstance(value, date) else value
        for key, value in item.items()
    }


def find_match_ovk(ovk: str):
    """To find matching overenskomst, maybe?"""
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
    result = []
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
        raise e
    except ValueError as e:
        print(f"Value error: {str(e)}")
        raise e
    # pylint: disable-next = broad-exception-caught
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        raise e

    if len(result) == 0:
        return None

    return result
