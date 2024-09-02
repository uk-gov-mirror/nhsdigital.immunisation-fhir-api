"""Convert FHIR Immunization Resource JSON to flat JSON contianing all the CSV fields and their corresponding values"""

from typing import Literal
import obtain_csv_value
from constants import CSV_FIELDS


def convert_to_flat_json(imms: dict, operation: Literal["NEW", "UPDATE", "DELETE"]):
    """
    Takes a FHIR Immunization Resource and returns a flat JSON dictionary contianing all the CSV fields as keys,
    with their corresponding values
    """
    flat_dict = {}
    flat_dict["ACTION_FLAG"] = operation
    for csv_field in CSV_FIELDS.pop("ACTION_FLAG"):
        flat_dict[csv_field] = getattr(obtain_csv_value, csv_field)(imms)
    return flat_dict
