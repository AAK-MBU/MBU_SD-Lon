"""Module to contain different workers"""
import json
import re

from itk_dev_shared_components.smtp.smtp_util import send_email
from OpenOrchestrator.database.queues import QueueElement
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection

from robot_framework.worker_data.kv2_data import tillaeg_pairs
from robot_framework.subprocesses.helper_functions import find_pair_info  # , find_match_ovk


def send_mail(orchestrator_connection: OrchestratorConnection, process_type: str, notification_receiver: str, queue_element: QueueElement):
    """Function to send email to inputted receiver"""
    receiver = notification_receiver
    email_body, email_subject = construct_worker_text(process_type=process_type, queue_element=queue_element)

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
    subject = ""

    person_id = element_data.get("Tjenestenummer", None)
    person_name = element_data.get("Navn", None)
    overenskomst = element_data.get("Overenskomst", None)
    afdeling = element_data.get("Afdeling", None)
    sd_inst_kode = element_data.get("Institutionskode", None)
    enhedsnavn = element_data.get("Enhedsnavn", None)

    if process_type == "KV1":
        # Inspirationsansættelser
        instruction_link = "https://intranet.aarhuskommune.dk/documents/146889"

        text = (
            "<p>Der er ved en fejl blevet oprettet en inspirationsansættelse på en XA enhed i jeres Administrative Fællesskab. <br>"
            + "Inspirationsansættelser skal udelukkende oprettes på XC enheder. <br>"
            + "Du skal derfor slette ansættelsen på XA enheden og oprette ansættelsen på ny på den korrekte XC enhed"
            + "- se nedenstående.</p>"
            + "-"*100
            + "<h4>Følgende inspirationsansættelse er registreret på en XA SD-institutionskode:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Afdeling: {afdeling}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            + f'<p>Du kan finde vejledningen til "Inspirationsansættelser" på <a href={instruction_link}>dette link</a> (AARHUSINTRA)<p>'
            + "-"*100
        )

        subject = "Inspirationsansættelse på XA institution"

        return text, subject

    if process_type == "KV2":
        # Manglende tillægsnummer
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

        subject = "Manglende tillægsnummer i ansættelse"

    if process_type in ("KV3", "KV3-DEV"):
        # Forker overenskomst skole/dagtilbud

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

        subject = "Fejl i SD-overenskomst"

    if process_type == "KV4":
        # Manglende låst anciennitetsdato
        instruction_link = "https://aarhuskommune.sharepoint.com/sites/IntranetDocumentSite/Intranetdokumentbibliotek/Forms/Seneste%20dokumenter.aspx?id=%2Fsites%2FIntranetDocumentSite%2FIntranetdokumentbibliotek%2FL%C3%A5st%20p%C3%A5%20grundl%C3%B8nstrin%2Epdf&parent=%2Fsites%2FIntranetDocumentSite%2FIntranetdokumentbibliotek&p=true&ga=1"

        text = (
            "<h4>Følgende leder har ikke fået fastlåst sin anciennitetsdato til dato 31.12.9999:</h4>"
            + f"<p>Tjenestenummer: {person_id}</p>"
            + f"<p>Navn: {person_name}</p>"
            + f"<p>Afdeling: {afdeling}</p>"
            + f"<p>SD institutionskode: {sd_inst_kode}</p>"
            + f"<p>Registreret overenskomst: {overenskomst}</p>"
            + f"<p>Du kan finde vejledningen <a href={instruction_link}>her</a>"
            + "<p>Bliver datoen ikke rettet til 31.12.9999, vil lederens grundlønstrin stige med et løntrin hvert år. <br>"
            + "OBS hvis der er tilknyttet et grundlønstillæg til stillingen skal du huske at oprette dette manuelt (måske er tillægget allerede oprettet, se lønsammensætning)."
        )

        subject = "Manglende låst anciennitet på leder"

    return text, subject


WORKER_MAP = {
    "Send mail": send_mail,
}
