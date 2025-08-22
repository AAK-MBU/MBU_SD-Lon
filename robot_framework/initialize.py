"""This module defines any initial processes to run when the robot starts."""

import json
from datetime import datetime

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from robot_framework.sql_scripts.kvalitetskontroller import PROCESS_PROCEDURE_DICT
from robot_framework.config import QUEUE_NAME
from robot_framework.subprocesses.helper_functions import format_item


def initialize(orchestrator_connection: OrchestratorConnection) -> None:
    """Do all custom startup initializations of the robot."""
    orchestrator_connection.log_trace("Initializing.")

    # Queue population here.
    get_items(orchestrator_connection)


def get_items(orchestrator_connection: OrchestratorConnection):
    """
    Function to retrieve items for robot.
    Uses stored procedures in SQL database
    """
    # Unpack from connection
    oc_args = json.loads(orchestrator_connection.process_arguments)
    process = oc_args.get("process", None).upper()

    if not process:
        raise ValueError(f"No process defined in process arguments: {oc_args}")

    # Set variables for function call
    process_procedure = PROCESS_PROCEDURE_DICT.get(
        process,
        None
    )
    if not process_procedure:
        raise ValueError(f"Process procedure for {process} not defined in dictionary")

    control_procedure = process_procedure.get(
        "procedure",
        ValueError(f"No stored procedure for {process_procedure} in dictionary")
    )
    procedure_params = process_procedure.get(
        "parameters",
        ValueError(f"No parameters for {process_procedure} in dictionary")
    )

    orchestrator_connection.log_trace(f"Running {process = }, procedure {control_procedure.__name__}, {procedure_params = }")

    # Get items for process
    items = control_procedure(**procedure_params, orchestrator_connection=orchestrator_connection)

    # Set dynamic queuename in connection
    orchestrator_connection.queue_name = f"{QUEUE_NAME}.{process}"

    if items:
        # Populate queue
        orchestrator_connection.bulk_create_queue_elements(
            queue_name=orchestrator_connection.queue_name,
            references=[
                f"{process}_{datetime.now().strftime('%d%m%y')}_{i+1}" for i in range(len(items))
            ],
            data = [json.dumps(format_item(item), ensure_ascii=False) for item in items],
            created_by="SD-lon_robot"
        )
        orchestrator_connection.log_trace(f"Populated queue with {len(items)} items.")

    else:
        orchestrator_connection.log_trace("No items found. Queue not populated")
