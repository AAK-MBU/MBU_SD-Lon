"""This module contains the main process of the robot."""
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from robot_framework.config import USERNAME
from robot_framework.subprocesses.helper_functions import fetch_files
from robot_framework.subprocesses.workers import WORKER_MAP


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    # Load arguments and creds
    oc_args = json.loads(orchestrator_connection.process_arguments)
    process_type = oc_args['process'].upper()

    creds = orchestrator_connection.get_credential(USERNAME)
    username = creds.username
    password = creds.password

    # Fetch control table for workers
    control_table = fetch_files(username, password)
    process_controls = control_table.get(process_type, None)

    if not process_controls:
        raise ValueError(f"No control defined in control table for process {process_type}")

    # Find and apply worker
    worker_type = process_controls["worker_type"]
    worker = WORKER_MAP.get(worker_type, None)
    if not worker:
        raise ValueError(f"No worker defined for process {worker_type}")

    orchestrator_connection.log_trace(f"Handling process with {worker_type = }")

    worker(
        orchestrator_connection=orchestrator_connection,
        process_type=process_type,
        process_controls=process_controls,
        queue_element=queue_element
    )

    orchestrator_connection.log_trace("Process finished")
