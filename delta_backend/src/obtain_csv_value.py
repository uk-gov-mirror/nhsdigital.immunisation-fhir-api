"""
File of fucntions for obtaining CSV values for the flat JSON, from a FHIR Immunization Resource.
Each function takes the FHIR Immunization JSON dictionary as it's only argument, 
and returns the required value if found, or the appropiate default value otherwise.
"""

import re
from typing import Union
from constants import Urls, GENDER_CODE_MAPPINGS, SNOMED_REGEX
import conversion_utils as utils


def nhs_number(imms: dict) -> Union[None, str]:
    """Get NHS_NUMBER from FHIR Immunization Resource JSON data"""
    nhs_number_patient_identifier_instance = utils.get_nhs_number_patient_identifier_instance(imms)
    return nhs_number_patient_identifier_instance.get("value") if nhs_number_patient_identifier_instance else None


def person_forename(imms: dict) -> Union[None, str]:
    """Get PERSON_FORENAME from FHIR Immunization Resource JSON data"""
    if not (person_given_name := utils.get_name_instance(imms, "patient").get("given")):
        return None
    return " ".join(person_given_name)


def person_surname(imms: dict) -> Union[None, str]:
    """Get PERSON_SURNAME from FHIR Immunization Resource JSON data"""
    return utils.get_name_instance(imms, "patient").get("family")


def person_dob(imms: dict) -> Union[None, str]:
    """Get PERSON_DOB from FHIR Immunization Resource JSON data"""
    if not (fhir_person_dob := utils.get_contained_patient(imms).get("birthDate")):
        return None
    return utils.convert_fhir_date_to_csv_date(fhir_person_dob)


def person_gender_code(imms: dict) -> Union[None, str]:
    """Get PERSON_GENDER_CODE from FHIR Immunization Resource JSON data"""
    return GENDER_CODE_MAPPINGS.get(utils.get_contained_patient(imms).get("gender"))


def person_postcode(imms: dict) -> str:
    """Get PERSON_POSTCODE from FHIR Immunization Resource JSON data"""
    return utils.get_address_instance(imms, "patient").get("postalCode", "ZZ99 3CZ")


def date_and_time(imms: dict) -> str:
    """Get DATE_AND_TIME from FHIR Immunization Resource JSON data"""
    return utils.convert_fhir_date_time_to_csv_date_time(imms["occurrenceDateTime"])


def site_code(imms: dict) -> Union[None, str]:
    """Get SITE_CODE from FHIR Immunization Resource JSON data"""
    return utils.get_performer_instance_containing_site_code(imms).get("actor", {}).get("identifier", {}).get("value")


def site_code_type_uri(imms: dict) -> Union[None, str]:
    """Get SITE_CODE_TYPE_URI from FHIR Immunization Resource JSON data"""
    return utils.get_performer_instance_containing_site_code(imms).get("actor", {}).get("identifier", {}).get("system")


def unique_id(imms: dict) -> str:
    """Get UNIQUE_ID from FHIR Immunization Resource JSON data"""
    return imms["identifier"][0]["value"]


def unique_id_uri(imms: dict) -> str:
    """Get UNIQUE_ID from FHIR Immunization Resource JSON data"""
    return imms["identifier"][0]["system"]


def performing_professional_forename(imms: dict) -> str:
    """Get PERFORMING_PROFESSIONAL_FORENAME from FHIR Immunization Resource JSON data"""
    if not (performing_professional_given_name := utils.get_name_instance(imms, "practitioner").get("given")):
        return None
    return " ".join(performing_professional_given_name)


def performing_professional_surname(imms: dict) -> str:
    """Get PERSON_SURNAME from FHIR Immunization Resource JSON data"""
    return utils.get_name_instance(imms, "practitioner").get("family")


def primary_source(imms: dict) -> str:
    """Get PRIMARY_SOURCE from FHIR Immunization Resource JSON data"""
    return imms.get("primarySource")


def vaccination_procedure_code(imms: dict) -> str:
    """Get VACCINATION_PROCEDURE_CODE from FHIR Immunization Resource JSON data"""
    vaccination_procedure_codeable_concept = utils.get_vaccination_procedure(imms)["valueCodeableConcept"]
    return utils.get_first_snomed_instance_from_coding(vaccination_procedure_codeable_concept["coding"])["code"]


def vaccination_procedure_term(imms: dict) -> str:
    """Get VACCINATION_PROCEDURE_TERM from FHIR Immunization Resource JSON data"""
    vaccination_procedure_codeable_concept = utils.get_vaccination_procedure(imms)["valueCodeableConcept"]

    # Select the text element
    if (text := vaccination_procedure_codeable_concept.get("text")) is not None:
        return text

    first_snomed_coding_element = utils.get_first_snomed_instance_from_coding(
        vaccination_procedure_codeable_concept["coding"]
    )

    # Select extension value string
    if "something":
        return first_snomed_coding_element

    return first_snomed_coding_element.get("display")


def dose_sequence(imms: dict) -> str:
    """Get DOSE_SEQUENCE from FHIR Immunization Resource JSON data"""
    dose_number = imms.get("protocolApplied", [])[0].get("doseNumberPositiveInt")
    if isinstance(dose_number, int) and dose_number in range(0, 10):
        return dose_number


def vaccine_product_code(imms: dict) -> str:
    """Get VACCINE_PRODUCT_CODE from FHIR Immunization Resource JSON data"""
    if not (vaccine_code_coding := imms.get("vaccineCode", {}).get("coding")):
        return None

    if not (snomed_coding_instances := [x for x in vaccine_code_coding if x.get("system") == Urls.snomed]):
        return None

    snomed_codes = [x for x in snomed_coding_instances if re.compile(SNOMED_REGEX).fullmatch(x.get("code", ""))]

    return snomed_codes[0] if snomed_codes else None


def vaccine_product_term(imms: dict) -> str:
    """Get VACCINE_PRODUCT_TERM from FHIR Immunization Resource JSON data"""
    if not (vaccine_code := imms.get("vaccineCode")):
        return None

    # Select vaccineCode.text
    if text := vaccine_code.get("text"):
        return text

    # Select coding.extension.valueString in Extension UKCore-CodingSCTDescDisplay from first coding instance
    # where system=Urls.snomed
    if "something":
        pass

    # Select display from first coding instance where system=Urls.snomed
    if first_snomed_instance := utils.get_first_snomed_instance_from_coding(vaccine_code.get("coding", [])):
        return first_snomed_instance.get("display")


def vaccine_manufacturer(imms: dict) -> str:
    """Get VACCINE_MANUFACTURER from FHIR Immunization Resource JSON data"""
    return imms.get("manufacturer", {}).get("display")


def batch_number(imms: dict) -> str:
    """Get BATCH_NUMBER from FHIR Immunization Resource JSON data"""
    return imms.get("lotNumber")


def expiry_date(imms: dict) -> str:
    """Get EXPIRY_DATE from FHIR Immunization Resource JSON data"""
    return utils.convert_fhir_date_to_csv_date(imms["expirationDate"])


def site_of_vaccination_code(imms: dict) -> str:
    """Get SITE_OF_VACCINATION_CODE from FHIR Immunization Resource JSON data"""
    return utils.get_first_snomed_instance_from_coding(imms.get("site", {}).get("coding", [])).get("code")


def site_of_vaccination_term(imms: dict) -> str:
    """Get SITE_OF_VACCINATION_TERM from FHIR Immunization Resource JSON data"""
    # If site does not exist return None
    if (site := imms.get("site")) is None:
        return None

    # If text exists return text
    if (text := site.get("text")) is not None:
        return text

    first_snomed = utils.get_first_snomed_instance_from_coding(imms.get("site", {}).get("coding", []))

    if "something":
        pass

    return first_snomed.get("display")


def route_of_vaccination_code(imms: dict) -> str:
    """Get ROUTE_OF_VACCINATION_CODE from FHIR Immunization Resource JSON data"""
    return utils.get_first_snomed_instance_from_coding(imms.get("route", {}).get("coding", [])).get("code")


def route_of_vaccination_term(imms: dict) -> str:
    """Get ROUTE_OF_VACCINATION_TERM from FHIR Immunization Resource JSON data"""
    # If route does not exist return None
    if (route := imms.get("route")) is None:
        return None

    # If text exists return text
    if (text := route.get("text")) is not None:
        return text

    first_snomed = utils.get_first_snomed_instance_from_coding(imms.get("route", {}).get("coding", []))

    if "something":
        pass

    return first_snomed.get("display")


def dose_amount(imms: dict) -> str:
    """Get DOSE_AMOUNT from FHIR Immunization Resource JSON data"""
    return imms.get("doseQuantity", {}).get("value")


def dose_unit_code(imms: dict) -> str:
    """Get DOSE_UNIT_CODE from FHIR Immunization Resource JSON data"""
    if imms.get("doseQuantity", {}).get("system") != Urls.snomed:
        return None

    return imms.get("doseQuantity", {}).get("code")


def dose_unit_term(imms: dict) -> str:
    """Get DOSE_UNIT_TERM from FHIR Immunization Resource JSON data"""
    return imms.get("doseQuantity", {}).get("unit")


def indication_code(imms: dict) -> Union[None, str]:
    """Get INDICATION_CODE from FHIR Immunization Resource JSON data"""
    if not (reason_codes := imms.get("reasonCode")):
        return None

    for reason_code in reason_codes:
        snomed_coding_instances = [x for x in reason_code.get("coding", []) if x.get("system") == Urls.snomed]
        if snomed_codes := [x for x in snomed_coding_instances if x.get("code") is not None]:
            return snomed_codes[0]


def location_code(imms: dict) -> str:
    """Get LOCATION_CODE from FHIR Immunization Resource JSON data"""
    return imms.get("location", {}).get("identifier", {}).get("code", "X99999")


def location_code_type_uri(imms: dict) -> str:
    """Get LOCATION_CODE_TYPE_URI from FHIR Immunization Resource JSON data"""
    return imms.get("location", {}).get("identifier", {}).get("system", Urls.ods_organization_code)
