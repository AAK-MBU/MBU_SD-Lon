"""This module contains the main process of the robot."""
import json

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

from robot_framework.subprocesses.workers import WORKER_MAP


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")

    # Load arguments and creds
    oc_args = json.loads(orchestrator_connection.process_arguments)
    process_type = oc_args['process'].upper()
    notification_type = oc_args['notification_type']
    notification_receiver = oc_args['notification_receiver']

    if notification_receiver == "AF":
        notification_receiver = json.loads(queue_element.data)["AF_email"]

    # Find and apply worker
    worker = WORKER_MAP.get(notification_type, None)
    if not worker:
        raise ValueError(f"No worker defined for process {notification_type}")

    orchestrator_connection.log_trace(f"Handling process with {notification_type = }")

    worker(
        orchestrator_connection=orchestrator_connection,
        process_type=process_type,
        notification_receiver=notification_receiver,
        queue_element=queue_element
    )

    orchestrator_connection.log_trace("Process finished")
