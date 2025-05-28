"""This module defines any finalizing processes to run when the robot starts."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection


def finalize(orchestrator_connection: OrchestratorConnection) -> None:
    """Do all custom startup initializations of the robot."""
    orchestrator_connection.log_trace("Finalizing.")
