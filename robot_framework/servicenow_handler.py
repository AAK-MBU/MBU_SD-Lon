"""ServiceNow Incident handler - this script creates a new incident in ServiceNow or adds a comment to an existing one"""

import requests

from robot_framework import config


PROD_INSTANCE = "aarhuskommune"
TEST_INSTANCE = "aarhuskommunedev"


def handle_incident(orchestrator_connection, error_dict):
    """
    This function handles an incoming error and determines if a new incident should be created, or an existing one should be updated
    """

    # We start by looking for an existing ServiceNow incident with a matching process name
    existing_incident_sys_id = get_incident(orchestrator_connection)

    if existing_incident_sys_id:
        update_incident(orchestrator_connection, error_dict, existing_incident_sys_id)

    else:
        post_incident(orchestrator_connection, error_dict)


def get_incident(orchestrator_connection):
    """
    Retrieves an existing incident that matches certain criteria from the error_dict.
    """

    process_name = orchestrator_connection.process_name

    # Here we specify the incidents we would like returned - short description must include the process name, state can not be 6 as that means the incident is resolved
    # We order by latest created incident, so we always update the newest returned - in theory the request should only return 1 incident
    query = f"short_descriptionLIKE{process_name}^active=true^state!=6^ORDERBYDESCsys_created_on"

    # get_url = f"https://{PROD_INSTANCE}.service-now.com/api/now/table/incident?sysparm_limit=50&sysparm_query={query}"
    get_url = f"https://{TEST_INSTANCE}.service-now.com/api/now/table/incident?sysparm_limit=50&sysparm_query={query}"

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    service_now_api_username = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).username
    service_now_api_password = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).password

    # pylint: disable=missing-timeout
    response = requests.get(get_url, headers=headers, auth=(service_now_api_username, service_now_api_password))

    # pylint: disable=no-else-return
    if response.status_code == 200:
        results = response.json().get("result", [])

        if results:
            print(results[0].get("sys_id"))

            return results[0].get("sys_id")  # Only return first match

        else:
            return None

    else:
        print(f"Error {response.status_code}: {response.text}")

        return None


def update_incident(orchestrator_connection, error_dict, existing_incident_sys_id):
    """
    Method to update an existing incident - the method adds a new comment to the existing incident
    """

    error_message = error_dict.get("message", "")  # The actual Exception message in str format
    error_trace = error_dict.get("trace", "")  # The traceback.format_exc() in str format

    process_name = orchestrator_connection.process_name

    comment_text = f"The process '{process_name}' has encountered another ApplicationException!\n\n"
    comment_text += f"Exception message:\n{error_message}\n\n"
    comment_text += f"Full Exception trace:\n{error_trace}\n\n"
    comment_text += "Please investigate the source of this error, as this comment is attached to an existing incident with the same process name."

    print()
    print(comment_text)

    # put_url = f"https://{PROD_INSTANCE}.service-now.com/api/now/table/incident/{existing_incident_sys_id}"
    put_url = f"https://{TEST_INSTANCE}.service-now.com/api/now/table/incident/{existing_incident_sys_id}"

    incident_data = {
        "comments": f'{comment_text}'
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    service_now_api_username = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).username
    service_now_api_password = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).password

    # pylint: disable=missing-timeout
    response = requests.put(put_url, headers=headers, auth=(service_now_api_username, service_now_api_password), json=incident_data)

    # pylint: disable=no-else-return
    if response.status_code == 200:
        return response.json().get("result", {})

    else:
        print(f"Error {response.status_code}: {response.text}")

        return None


def post_incident(orchestrator_connection, error_dict):
    """
    Create a new incident for the caught ApplicationException in ServiceNow
    """

    print("inside post_incident() function ...")

    error_message = error_dict.get("message", "")  # The actual Exception message in str format
    error_trace = error_dict.get("trace", "")  # The traceback.format_exc() in str format

    # post_url = f"https://{PROD_INSTANCE}.service-now.com/api/now/table/incident"
    post_url = f"https://{TEST_INSTANCE}.service-now.com/api/now/table/incident"

    incident_data = {
        "contact_type": "integration",  # Should always be 'integration' - this just means the incident was created using the ServiceNow API
        "short_description": f"ApplicationException caught in process '{orchestrator_connection.process_name}'",
        "description": f"Error message:\n{error_message}\n\nFull error trace message:\n{error_trace}",
        "business_service": "",  # What should this be?
        "service_offering": "",  # What should this be?
        "assignment_group": "b54156a91ba5115068ba5398624bcb0e",  # MBU Proces & Udvikling Assignment Group - should this be a constant in Orchestrator?
        "assigned_to": "",  # Should remain empty as placeholder for future assignment?

        "category": "Fejl",
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    service_now_api_username = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).username
    service_now_api_password = orchestrator_connection.get_credential(config.SERVICE_NOW_API_PROD_USER).password

    # pylint: disable=missing-timeout
    response = requests.post(post_url, headers=headers, auth=(service_now_api_username, service_now_api_password), json=incident_data)

    print()
    print("Response Status Code:", response.status_code)
    print("Response Text:", response.text)

    # pylint: disable=no-else-return
    if response.status_code == 200:
        return response.json().get("result", {})

    else:
        print(f"Error {response.status_code}: {response.text}")

        return None
