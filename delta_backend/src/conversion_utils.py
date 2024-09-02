"""Utils for converting FHIR Immunization Resource JSON to flat JSON with CSV fields as keys"""

from typing import Literal
from datetime import datetime, timezone
from constants import Urls


def convert_fhir_date_to_csv_date(fhir_date: str) -> str:
    """Converts a date in FHIR format, to a date in CSV format"""
    return fhir_date.replace("-", "")


def convert_to_datetime(date_time_string: str) -> datetime:
    """Converts a FHIR date time string to datetime format"""
    # Full date only
    if "T" not in date_time_string:
        return datetime.strptime(date_time_string, "%Y-%m-%d")

    # Full date, time with milliseconds, timezone
    if "." not in date_time_string:
        return datetime.strptime(date_time_string, "%Y-%m-%dT%H:%M:%S%z")

    # Full date, time without milliseconds, timezone
    return datetime.strptime(date_time_string, "%Y-%m-%dT%H:%M:%S.%f%z")


def convert_fhir_date_time_to_csv_date_time(fhir_date_time: str) -> str:
    """Converts a date-time in FHIR format, to a date-time in CSV format"""
    # If only date exists return it with dashes removed
    if "T" not in fhir_date_time:
        return fhir_date_time.replace("-", "")

    # If timezone is UTC return it with milliseconds, dashes and semicolons removed, and timezone replaced with "00"
    if fhir_date_time[-5:] == "00:00":
        return fhir_date_time.split(".")[0].replace("-", "").replace(":", "") + "00"

    # If timezone is BST return it with milliseconds, dashes and semicolons removed, and timezone replaced with "01"
    if fhir_date_time[-5:] == "01:00":
        return fhir_date_time.split(".")[0].replace("-", "").replace(":", "") + "01"

    # If timezone is not UTC or BST, convert it to UTC and return it with milliseconds, dashes and semicolons removed,
    # and timezone replaced with "00"
    date_time_utc = convert_to_datetime(fhir_date_time).astimezone(timezone.utc)
    return date_time_utc.strftime("%Y%m%dT%H%M%S") + "00"


def get_contained_resource(imms: dict, resource: Literal["Patient", "Practitioner"]) -> dict:
    """
    Gets the requested contained resource from the FHIR Immunization Resource JSON data.
    If resource isn't found, defaults to empty dictionary.
    """
    resource = [x for x in imms.get("contained", []) if x.get("resourceType") == resource]
    return resource[0] if resource else {}


def get_contained_patient(imms: dict) -> dict:
    """
    Gets the contained patient from the FHIR Immunization Resource JSON data.
    If resource isn't found, defaults to empty dictionary.
    """
    return get_contained_resource(imms, "Patient")


def get_contained_practitioner(imms: dict):
    """
    Gets the contained practitioner from the FHIR Immunization Resource JSON data.
    If resource isn't found, defaults to empty dictionary.
    """
    return get_contained_resource(imms, "Practitioner")


def get_performer_instance_containing_site_code(imms: dict) -> dict:
    """
    Gets the instance of performer which contains the SITE_CODE.
    Defaults to empty dictionary if no such instance is found.
    """
    # If there is no performer, return an empty dicitonary
    if len((performers := imms["performer"])) == 0:
        return {}

    # If there is only one instance of performer, select that performer
    if len(performers) == 1:
        return performers[0]

    # Remove records where the relevant value and system do not exist
    performers = [
        x
        for x in performers
        if x.get("actor", {}).get("identifier", {}).get("value") is not None
        and x.get("actor", {}).get("identifier", {}).get("system") is not None
    ]

    performers_with_ods = [
        x for x in performers if x.get("actor", {}).get("identifier", {}).get("system") == Urls.ods_organization_code
    ]

    # Select the first organization with ods
    if orgs_with_ods := [x for x in performers_with_ods if x.get("type") == "organization"]:
        return orgs_with_ods[0]

    # Select the first performer with ods
    if performers_with_ods:
        return performers_with_ods[0]

    # Select the first organization
    if organizations := [x for x in performers if x.get("type") == "organization"]:
        return organizations[0]

    # Select the first performer
    return performers[0]


def get_vaccination_procedure(imms: dict) -> dict:
    """
    Gets the instance of extension which contains the vaccination procedure.
    Defaults to empty dictionary if no such instance is found
    """
    vaccination_procedures = [x for x in imms["extension"] if x.get("url") == Urls.vaccination_procedure]
    return vaccination_procedures[0] if vaccination_procedures else {}


def get_first_snomed_instance_from_coding(coding: dict) -> dict:
    """
    Gets the first snomed instance from a coding element. Defaults to empty dictionary if no such instance is found.
    """
    snomeds = [x for x in coding if x.get("system") == Urls.snomed]
    return snomeds[0] if snomeds else {}


def is_current(instance: dict, occurrence_date_time: str) -> bool:
    """
    Determine if an instance is current. An instance is defined to be current if the period is absent, or the date of
    vaccination (occurrence_date_time) falls withing the period date range.
    """

    if instance.get("period") is None:
        return True

    if period_start := instance["period"].get("start"):
        if convert_to_datetime(period_start) > convert_to_datetime(occurrence_date_time):
            return False

    if period_end := instance["period"].get("end"):
        if convert_to_datetime(period_end) < convert_to_datetime(occurrence_date_time):
            return False

    return True


def get_name_instance(imms: dict, patient_or_practitioner: Literal["Patient", "Practitioner"]) -> dict:
    """
    Gets the relevant name instance from a patient or practitioner.
    Defaults to empty dictionary if no such instance exists.
    """
    names = get_contained_resource(imms, patient_or_practitioner).get("name", [])

    # Extract only name instances which have given and family elements
    names = [x for x in names if x.get("given") is not None and x.get("family") is not None]

    # If there is no name, default to empty dictionary
    if len(names) == 0:
        return {}

    # If there is only one name, select it
    if len(names) == 1:
        return names[0]

    current_names = [x for x in names if is_current(x, imms.get("occurrenceDateTime"))]

    # Select the first name which is current at date of vaccination and use=official
    if current_official_names := [x for x in current_names if x.get("use") == "official"]:
        return current_official_names[0]

    # Select first name which is current at date of vaccination and use!=old
    if current_non_old_names := [x for x in current_names if x.get("use") != "old"]:
        return current_non_old_names[0]

    # Select first name instance
    return names[0]


def get_address_instance(imms: dict, patient_or_practitioner: Literal["Patient", "Practitioner"]) -> dict:
    """Get the relevant address instance from a list of addresses. Defaults to empty dictionary."""
    addresses = get_contained_resource(imms, patient_or_practitioner).get("address", [])

    # If there is no name, default to empty dictionary
    if len(addresses) == 0:
        return {}

    # If there is only one address, select it
    if len(addresses) == 1:
        return addresses[0]

    # Extract only address instances which have a postalCode and are current
    addresses = [
        x for x in addresses if x.get("postalCode") is not None and is_current(x, imms.get("occurrenceDateTime"))
    ]

    # Select the first address with use=home and type!=postal
    if home_non_postal_address := [x for x in addresses if x.get("use") == "home" and x.get("type") != "postal"]:
        return home_non_postal_address[0]

    # Select first address with use!=old and type!=postal
    if non_old_non_postal_address := [x for x in addresses if x.get("use") != "old" and x.get("type") != "postal"]:
        return non_old_non_postal_address[0]

    # Select first name instance
    return [x for x in addresses if x.get("use") != "old"][0]


def get_nhs_number_patient_identifier_instance(imms: dict) -> dict:
    """
    Get the patient identifier instance which contains the nhs number.
    Defaults to empty dictionary if no such instance is found.
    """
    patient_identifiers = get_contained_patient(imms).get("identifier")
    nhs_number_identifiers = [x for x in patient_identifiers if x.get("system") == Urls.nhs_number]
    return nhs_number_identifiers[0] if nhs_number_identifiers else {}
