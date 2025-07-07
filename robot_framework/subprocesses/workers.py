"""Module to contain different workers"""
import json
import re

from itk_dev_shared_components.smtp.smtp_util import send_email
from OpenOrchestrator.database.queues import QueueElement
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

# from robot_framework.worker_data.KV2_data import PAIRS as KV2_PAIRS
from robot_framework.worker_data.KV2_data import tillaeg_pairs
from robot_framework.subprocesses.helper_functions import find_pair_info  # , find_match_ovk


def send_mail(orchestrator_connection: OrchestratorConnection, process_type: str, process_controls: dict, queue_element: QueueElement):
    """Function to send email to inputted receiver"""
    receiver = process_controls["worker_data"]
    email_body = construct_worker_text(process_type=process_type, queue_element=queue_element)
    email_subject = process_controls["process_description"]

    send_email(
        receiver=receiver,
        sender=orchestrator_connection.get_constant("e-mail_noreply").value,
        subject=email_subject,
        body=email_body,
        smtp_server=orchestrator_connection.get_constant('smtp_server').value,
        smtp_port=orchestrator_connection.get_constant('smtp_port').value,
        html_body=True,
    )

    orchestrator_connection.log_trace(f"E-mail sent to {receiver}")


def construct_worker_text(process_type: str, queue_element: QueueElement):
    """Function to construct text for different the processes"""
    element_data = json.loads(queue_element.data)
    text = ""

    person_id = element_data.get("Tjenestenummer", None)
    person_name = element_data.get("Navn", None)
    overenskomst = element_data.get("Overenskomst", None)
    afdeling = element_data.get("Afdeling", None)
    sd_inst_kode = element_data.get("Institutionskode", None)
    enhedsnavn = element_data.get("Enhedsnavn", None)

    if process_type == "KV2":
        # Get element info
        # Initialize found pair
        found_number = int(element_data["Tillægsnummer"])
        found_name = element_data["Tillægsnavn"]
        found_type = re.search(pattern=r"([A|B])-(?!.*-)", string=found_name).group(1)
        # Initialize supposed match
        match_number = None
        match_name = None
        match_type = None
        # Find supposed match
        for pair in tillaeg_pairs:
            match_set = find_pair_info(pair, found_number)
            if match_set:
                match_number, match_name = match_set
                match_type = re.search(pattern=r"([A|B])-(?!.*-)", string=match_name).group(1)

        # Construct message
        text = (
            "<h4>Følgende ansættelse mangler et tillægsnummer, "
            + f"da denne er registreret med et {found_type}-tillægsnummer, men mangler et {match_type}-tillægsnummer:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Overenskomst: {overenskomst}</p>"
            + f"<p>Afdeling: {afdeling} ({enhedsnavn})</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Fundet tillæg: {found_number}-{found_name}</p>"
            + f"<p>Manglende tillæg: {match_number}-{match_name}</p>"
            + "Ved rettelse af denne fejl skal lønsammensætningen kontrolleres. Ved spørgsmål, kontakt da Personale."
        )

    if process_type == "KV3" or process_type == "KV3-DEV":

        afdtype_txt = element_data["afdtype_txt"]
        # exp_ovk = find_match_ovk(overenskomst)

        text = (
            "<h4>Følgende ansættelse er oprettet med en forkert SD overenskomst:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Afdeling: {afdeling} ({enhedsnavn})</p>"
            + f"<p>Afdelingstype: {afdtype_txt}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            # + f"<p>Forventet overenskomst: {exp_ovk}</p>"
        )

    if process_type == "KV4":

        text = (
            "<h4>Følgende leder har ikke fået fastlåst sin anciennitetsdato:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Afdeling: {afdeling} ({enhedsnavn})</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
        )

    return text


WORKER_MAP = {
    "Send mail": send_mail,
}
