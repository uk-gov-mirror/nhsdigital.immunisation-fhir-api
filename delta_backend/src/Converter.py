# Main validation engine

import ExceptionMessages
from FHIRParser import FHIRParser
from SchemaParser import SchemaParser
from ConversionChecker import ConversionChecker
import ConversionLayout
from datetime import datetime
from extractor import extract_person_names,extract_practitioner_names,extract_site_code,get_patient,get_valid_address,get_valid_names

# Converter variables
FHIRData = ""
SchemaFile = {}
imms = []
Converted = {}
ErrorRecords = []


# Converter
class Converter:

    def __init__(self, fhir_data):
        self.FHIRData = fhir_data  # Store JSON data directly
        self.SchemaFile = ConversionLayout.ConvertLayout

    # create a FHIR  parser - uses fhir json data from delta
    def _getFHIRParser(self, fhir_data):
        fhirParser = FHIRParser()
        fhirParser.parseFHIRData(fhir_data)
        return fhirParser

    # create a schema parser
    def _getSchemaParser(self, schemafile):
        schemaParser = SchemaParser()
        schemaParser.parseSchema(schemafile)
        return schemaParser

    # Convert data against converter schema
    def _convertData(self, ConversionValidate, expression, dataParser, json_data):

        FHIRFieldName = expression["fieldNameFHIR"]
        FlatFieldName = expression["fieldNameFlat"]

        expressionType = expression["expression"]["expressionType"]
        expressionRule = expression["expression"]["expressionRule"]

        try:
            conversionValues = dataParser.getKeyValue(FHIRFieldName)
        except Exception as e:
            message = "Data get value Unexpected exception [%s]: %s" % (e.__class__.__name__, e)
            p = {"code": ExceptionMessages.PARSING_ERROR, "message": message}
            ErrorRecords.append(p)
            return p

        for conversionValue in conversionValues:
            convertedData = ConversionValidate.convertData(
                expressionType, expressionRule, FHIRFieldName, conversionValue
            )
            if FHIRFieldName == "contained|#:":
                convertedData= self.extract_patient_details(json_data, FlatFieldName)
            if convertedData is not None:
                Converted[FlatFieldName] = convertedData

    # run the conversion against the data
    def runConversion(self, json_data, summarise=False, report_unexpected_exception=True):
        try:
            dataParser = self._getFHIRParser(self.FHIRData)
        except Exception as e:
            if report_unexpected_exception:
                message = "FHIR Parser Unexpected exception [%s]: %s" % (e.__class__.__name__, e)
                p = {"code": 0, "message": message}
                ErrorRecords.append(p)
                return p

        try:
            schemaParser = self._getSchemaParser(self.SchemaFile)
        except Exception as e:
            if report_unexpected_exception:
                message = "Schema Parser Unexpected exception [%s]: %s" % (e.__class__.__name__, e)
                p = {"code": 0, "message": message}
                ErrorRecords.append(p)
                return p

        try:
            ConversionValidate = ConversionChecker(dataParser, summarise, report_unexpected_exception)
        except Exception as e:
            if report_unexpected_exception:
                message = "Expression Checker Unexpected exception [%s]: %s" % (e.__class__.__name__, e)
                p = {"code": 0, "message": message}
                ErrorRecords.append(p)
                return p

        # get list of expressions
        try:
            conversions = schemaParser.getConversions()
        except Exception as e:
            if report_unexpected_exception:
                message = "Expression Getter Unexpected exception [%s]: %s" % (e.__class__.__name__, e)
                p = {"code": 0, "message": message}
                ErrorRecords.append(p)
                return p

        for conversion in conversions:
            rows = self._convertData(ConversionValidate, conversion, dataParser, json_data)

        imms.append(Converted)
        return imms

    def getErrorRecords(self):
        return ErrorRecords

    def extract_patient_details(self, json_data, FlatFieldName):
        occurrence_time = datetime.strptime(json_data.get("occurrenceDateTime", ""), "%Y-%m-%dT%H:%M:%S%z")
        patient = get_patient(json_data)
        if not patient:
            return None
        
        person_forename, person_surname = extract_person_names(patient, occurrence_time)
        postal_code = get_valid_address(patient, occurrence_time)
        site_code, site_code_type_uri = extract_site_code(json_data)
        performing_professional_forename, performing_professional_surname = extract_practitioner_names(json_data, occurrence_time)
        
        field_map = {
            "PERSON_FORENAME": person_forename,
            "PERSON_SURNAME": person_surname,
            "PERSON_POSTCODE": postal_code,
            "SITE_CODE": site_code,
            "SITE_CODE_TYPE_URI": site_code_type_uri,
            "PERFORMING_PROFESSIONAL_FORENAME": performing_professional_forename,
            "PERFORMING_PROFESSIONAL_SURNAME": performing_professional_surname
        }
        
        return field_map.get(FlatFieldName)
         