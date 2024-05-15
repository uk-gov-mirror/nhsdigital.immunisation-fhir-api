"""Utils for backend folder"""

from typing import Union
from src.mappings import vaccine_type_mappings


def disease_codes_to_vaccine_type(disease_codes: list) -> Union[str, None]:
    """
    Takes a list of disease codes and returns the corresponding vaccine type if found,
    otherwise raises a value error
    """
    try:
        return next(x[1] for x in vaccine_type_mappings if x[0] == sorted(disease_codes))
    except Exception as e:
        raise ValueError(f"{disease_codes} is not a valid combination of disease codes for this service") from e


def get_vaccine_type(immunization: dict):
    """
    Take a FHIR immunization resource and returns the vaccine type based on the combination of target diseases.
    If combination of disease types does not map to a valid vaccine type, a value error is raised.

    NOTE: The immunization dict given as an argument must first have passed all pre-validation and 
    FHIR validation checks. If this is not the case then run pre-validation and FHIR validation on the dict before
    calling this function.
    """
    target_diseases = []
    try:
        target_disease_list = immunization["protocolApplied"][0]["targetDisease"]
        for element in target_disease_list:
            code = [x.get("code") for x in element["coding"] if x.get("system") == "http://snomed.info/sct"][0]
            target_diseases.append(code)
    except (KeyError, IndexError, AttributeError) as error:
        raise ValueError("No target disease codes found") from error
    return disease_codes_to_vaccine_type(target_diseases)


def has_valid_vaccine_type(immunization: dict):
    """Returns  vaccine type if combination of disease codes is valid, otherwise returns False"""
    try:
        return get_vaccine_type(immunization)
    except ValueError:
        return False
