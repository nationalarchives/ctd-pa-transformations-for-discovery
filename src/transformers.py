import re
import copy
from typing import Any, Optional, Iterable, List, Set
import xml.etree.ElementTree as ET
import json
from datetime import datetime, time, timedelta
import time as pytime
import re
import os
import logging
import sys

from src.utils import progress_context, _fmt_duration


def convert_to_json(xml_path: str, output_dir: str, remove_empty_fields: bool = True, 
                    progress_verbose: bool = False):
    # this function relies on module-level `tree` and `root` set by the caller
    # (the main runner sets these globals before calling convert_to_json)
    global tree, root

    record_level_mapping = {
        'FONDS': 1,
        'SUB-FONDS': 2,
        'SUB-SUB-FONDS': 3,
        'SUB-SUB-SUB-FONDS': 4,
        'SUB-SUB-SUB-SUB-FONDS': 5,
        'SERIES': 6,
        'SUB-SERIES': 7,
        'SUB-SUB-SERIES': 8,
        'FILE': 9,
        'ITEM': 10
    }

    parsed_tree = ET.parse(xml_path)
    globals()["tree"] = parsed_tree
    globals()["root"] = parsed_tree.getroot()

    for record_type in root.iter('record_type'):
        neutral_value = record_type.find("./value[@lang='neutral']")
        if neutral_value is not None:
            key = neutral_value.text.strip()
            if key in record_level_mapping:
                record_type.find("./value[@lang='neutral']").text = str(record_level_mapping[key])
                #print()


    for client_filepath in root.iter('client_filepath'):
            if client_filepath is not None:
                client_filepath.text = "Original filepath:" + client_filepath.text.strip()


    for start_date in root.iter('dating.date.start'):
        date_unconverted = start_date.text

        if date_unconverted:
            try:
                unconverted_date_pattern = r"(\d{4})-(\d{2})-(\d{2})"
                converted_date_pattern = r"\1\2\3"
                date_unconverted = re.sub(unconverted_date_pattern, converted_date_pattern, date_unconverted)
                start_date.text = date_unconverted
                #print(date_unconverted)

            except ValueError as e:
                print(f"Error converting date '{date_unconverted}' : {e}")

        #else:
            #print ("Date is None or empty")


    for end_date in root.iter('dating.date.end'):
        date_unconverted = end_date.text


        if date_unconverted:
            try:
                unconverted_date_pattern = r"(\d{4})-(\d{2})-(\d{2})"
                converted_date_pattern = r"\1\2\3"
                date_unconverted = re.sub(unconverted_date_pattern, converted_date_pattern, date_unconverted)
                end_date.text = date_unconverted
                #print(date_unconverted)

            except ValueError as e:
                print(f"Error converting date '{date_unconverted}' : {e}")

        #else:
        #   print ("Date is None or empty")


    for language in root.iter('inscription.language'):
        if language.text is not None:
            languages = [language_item.strip() for language_item in language.text.split(';')]
            if len(languages) > 1:
                sorted_languages = ', '.join(sorted(languages[:-1])) + ' and ' + languages[-1]
                language.text = sorted_languages

    ######################## creating one JSON file export for testing ###############################################
    #records = []

    #for record in root.iter('record'):

    ##################################################################################################################
    # Create dictionary for parentId resolution
    object_number_dict = {}
    for record in root.iter('record'):
        object_number_elem = record.find("object_number")
        if object_number_elem is not None and object_number_elem.text:
            object_number = object_number_elem.text

            # Find the CALM RecordID for this record
            calm_id_elem = record.find("Alternative_number/[alternative_number.type='CALM RecordID']")
            if calm_id_elem is not None:
                alt_number_elem = calm_id_elem.find('alternative_number')
                if alt_number_elem is not None and alt_number_elem.text:
                    object_number_dict[object_number] = alt_number_elem.text

    # diagnostic counters: how many record nodes processed and unique files written
    _records_processed = 0
    _written_paths = set()

    records = {}
    print(f"length of root records: {len(list(root.iter('record')))}")
    _total_records = len(list(root.iter('record')))
    for i, record in enumerate (root.iter('record')):

    ######################## Find_CALM_Record_ID_Element ###########################################################

        Find_CALM_Record_ID_Element = record.find("Alternative_number/[alternative_number.type='CALM RecordID']")
        if Find_CALM_Record_ID_Element is not None:
            iaid = Find_CALM_Record_ID_Element.find('alternative_number').text
        else:
            iaid = None

    ###############################################################################################################

        #replicaId -- not used

        citableReference = record.find("object_number")
        citableReference = citableReference.text if citableReference is not None else None

        #accumulationDates #-- not used

    #################################### parentId ####################################################################

        #################################### parentId ####################################################################

        parentId = "A13530124"  # Fond level value

        # Use the lookup dictionary for parentId resolution
        if part_of_reference and part_of_reference in object_number_dict:
            parentId = object_number_dict[part_of_reference]

    #####################################################################################################################

        accruals = record.find("accruals")
        accruals = accruals.text if accruals is not None else None

        #accessConditions = record.find("access_category.notes") #should apply only to level 1-8    # not used in this form anymore anymore
        #accessConditions =  accessConditions.text if accessConditions is not None else None

        administrativeBackground = record.find("admin_history")
        administrativeBackground = administrativeBackground.text if administrativeBackground is not None else None

        #appraisalInformation = record.find("disposal.notes")                                        # not used used anymore
        #appraisalInformation = appraisalInformation.text if appraisalInformation is not None else None

    ############################# arrangement###########################################################################

        arrangement_system = record.find("system_of_arrangement") #values of system_of_arrangement and client_filepath need to be aggregted in arrangement JSON field
        arrangement_system = arrangement_system.text if arrangement_system is not None else ''
        client_filepath = record.find("client_filepath")
        client_filepath = client_filepath.text if client_filepath is not None else ''

        arrangement = arrangement_system+' '+client_filepath
        arrangement = arrangement.strip()

        if arrangement == "":
            arrangement = None

            arrangement = arrangement_system+' '+client_filepath
            arrangement = arrangement.strip()
            
            if arrangement == "":
                arrangement = None

        #batchId ---> not used

        #refIaid ---> not used

        catalogueId = record.find("catid")
        catalogueId = int(catalogueId.text) if catalogueId is not None else None

    ############ catalogueLevel and access condition #############################################################################

        catalogueLevel = record.find("record_type/value[@lang='neutral']")
        catalogueLevel = catalogueLevel.text if catalogueLevel is not None else None
        if catalogueLevel is not None:
            catalogueLevel = int(catalogueLevel)

        if catalogueLevel >= 9:
            accessConditions =  None

        if catalogueLevel <= 8:
            accessConditions =  "Open unless otherwise stated"

    #######################################################################################################

        coveringFromDate = record.find("Dating/dating.date.start")
        coveringFromDate = coveringFromDate.text if coveringFromDate is not None else None
        if coveringFromDate is not None:
            coveringFromDate = int(coveringFromDate)

        coveringToDate = record.find("Dating/dating.date.end")
        coveringToDate = coveringToDate.text if coveringToDate is not None else None
        if coveringToDate is not None:
            coveringToDate = int(coveringToDate)

        chargeType = 1

        # eDocumentId -- not used

        coveringDates = record.find("dating.notes")
        coveringDates = coveringDates.text if coveringDates is not None else None

        custodialHistory = record.find("object_history_note")
        custodialHistory = custodialHistory.text if custodialHistory is not None else None

    ################### heldBy #######################################################

        heldBy_information = record.find("institution.name")
        heldBy_information = heldBy_information.text if heldBy_information is not None else None

        heldBy = []

        if heldBy_information == "The National Archives, Kew":
            heldBy = [
        {
        "xReferenceId": "A13530124",
        "xReferenceCode": "66",
        "xReferenceName": "The National Archives, Kew"
        }
    ]
        elif heldBy_information == "UK Parliament":
            heldBy = [
        {
        "xReferenceId": "A13531051",
        "xReferenceCode": "61",
        "xReferenceName": "UK Parliament"
        }
    ]

        elif heldBy_information == "British Film Institute":
            heldBy = [
        {
        "xReferenceId": "A13532152",
        "xReferenceCode": "2870",
        "xReferenceName": "British Film Institute (BFI) National Archive"
        }
    ]

    ######### ClosureCode ClosureStatus and closure Type ##################################

        if catalogueLevel >= 9:
            closureStatus = record.find("access_status/value[@lang='neutral']")
            closureStatus = closureStatus.text if closureStatus is not None else None

            if closureStatus == 'OPEN':
                closureStatus = 'O'
            elif closureStatus == 'CLOSED':
                closureStatus = 'D'

            closureCode = record.find("closed_until")
            if closureStatus == 'D':
                closureCode = closureCode.text
                closureCode = datetime.strptime(closureCode, "%Y-%m-%d")
                closureCode = closureCode.strftime("%Y")
            else:
                closureCode = None

            closureType = None

            if closureStatus == 'D':
                closureType = 'U'
            else:
                closureType = None
                
                if closureStatus == 'D':
                    closureType = 'U'
                else:
                    closureType = None

                if closureStatus == 'D' and heldBy_information == "UK Parliament":
                    closureStatus = 'U'
                    closureCode = None
                    closureType = None

            if catalogueLevel <= 8:
                closureStatus = None
                closureCode = None
                closureType = None


        ################### recordOpeningDate #######################################################

    ################### recordOpeningDate #######################################################

        if catalogueLevel >= 9:

            recordOpeningDate = record.find("closed_until")
            recordOpeningDate = recordOpeningDate.text if recordOpeningDate is not None else None

                if record.find("access_status/value[@lang='neutral']").text == 'CLOSED' and heldBy_information == "UK Parliament":
                    recordOpeningDate = None
                    
            if catalogueLevel <= 8:
                recordOpeningDate = None

        if catalogueLevel <= 8:
            recordOpeningDate = None

    #    if recordOpeningDate is not None:
    #        closed_date_object = datetime.strptime(recordOpeningDate, "%Y-%m-%d")
    #        recordOpeningDate = closed_date_object + timedelta(days =1)
    #        #recordOpeningDate = recordOpeningDate.strftime("%Y-%m-%d")
    #        recordOpeningDate = recordOpeningDate.strftime("%d-%m-%Y")

    #    else:
    #        recordOpeningDate = None
    #############################################################################################

        # corporateNames -- not used

        ################### copiesInformation #######################################################

            copiesInformation_description = record.find("existence_of_copies")
            copiesInformation_description = copiesInformation_description.text if copiesInformation_description is not None else None

        copiesInformation = []
        if copiesInformation_description is not None:
            copiesInformation = [
        {
        "xReferenceName": None,
        "xReferenceDescription": None,
        "description": copiesInformation_description
        }
    ]
        else:
            copiesInformation = [
        {
        "xReferenceName": None,
        "xReferenceDescription": None,
        "description": None
        }
    ]
    ################### creatorName #######################################################
        # At levels 9-10 do not supply any values (even if present in the Axiell export) into the creatorName field
        #if catalogueLevel >= 9:

        #    creatorName = [
        #    {
        #    "xReferenceName": None,
        #    "preTitle": None,
        #    "title": None,
        #    "firstName": None,
        #    "surname": None,
        #    "startDate": 0,
        #    "endDate": 0
        #    }
        #]

        if catalogueLevel <= 8:
            production_elements = record.findall("Production")
            creatorName = []
        #Looping through each production element to find creator sub-elements
            if production_elements:
                for production in production_elements:
                    creator_element = production.find("creator")
                    if creator_element is not None and creator_element.text:
                        creatorName.append({
                            "xReferenceName": creator_element.text,
                            "preTitle": None,
                            "title": None,
                            "firstName": None,
                            "surname": None,
                            "startDate": 0,
                            "endDate": 0
                        })

                    if not creatorName:
                        creatorName = [{
                            "xReferenceName": None,
                            "preTitle": None,
                            "title": None,
                            "firstName": None,
                            "surname": None,
                            "startDate": 0,
                            "endDate": 0
                        }]

    ############################ digitised ##########################################################

        digitised = record.find("digitised")
        digitised = digitised.text if digitised is not None else None
        if digitised is None:
            digitised = False
        if digitised == "x":
            digitised = True

    #################################################################################################

        #dimensions -- not used

    ########################### formerReferenceDep ###################################################

        Find_Former_Ref_Department_Element = record.find("Alternative_number/[alternative_number.type='Former reference (Department)']")
        if Find_Former_Ref_Department_Element is not None:
            formerReferenceDep = Find_Former_Ref_Department_Element.find('alternative_number').text
        else:
            formerReferenceDep = None

    ########################### formerReferencePro #########################################

        Find_Former_Archival_Ref_Element = record.find("Alternative_number/[alternative_number.type='Former archival reference']")
        if Find_Former_Archival_Ref_Element is not None:
            formerReferencePro = Find_Former_Archival_Ref_Element.find('alternative_number').text
        else:
            formerReferencePro = None

    ################################# immediateSourceOfAcquisition #######################################

        immediateSourceOfAcquisition_xReferenceDescription = record.find("acquisition.notes")
        immediateSourceOfAcquisition_xReferenceDescription =  immediateSourceOfAcquisition_xReferenceDescription.text if immediateSourceOfAcquisition_xReferenceDescription is not None else None

        immediateSourceOfAcquisition = []

        if immediateSourceOfAcquisition_xReferenceDescription is not None:
            immediateSourceOfAcquisition = [
        {
        "xReferenceName": None,
        "xReferenceDescription": immediateSourceOfAcquisition_xReferenceDescription,
        "preTitle": None,
        "title": None,
        "firstName": None,
        "surname": None,
        "startDate": 0,
        "endDate": 0
        }
    ]
        else:
            immediateSourceOfAcquisition = [
        {
        "xReferenceName": None,
        "xReferenceDescription": None,
        "preTitle": None,
        "title": None,
        "firstName": None,
        "surname": None,
        "startDate": 0,
        "endDate": 0
        }
    ]

    #############################################################################################

            language = record.find("Inscription//inscription.language")
            language = language.text if language is not None else None

            legalStatus = record.find("legal_status/value[@lang='0']")
            legalStatus = legalStatus.text if legalStatus is not None else None

        #links -- not used

    ################################# existence_of_originals #######################################

            locationOfOriginals_xReferenceDescription = record.find("existence_of_originals")
            locationOfOriginals_xReferenceDescription = locationOfOriginals_xReferenceDescription.text if locationOfOriginals_xReferenceDescription is not None else None

            locationOfOriginals = []

            if locationOfOriginals_xReferenceDescription is not None:
                locationOfOriginals = [
                {
                "xReferenceName": None,
                "xReferenceDescription": locationOfOriginals_xReferenceDescription
                }
        ]
            else:
                locationOfOriginals = [
            {
            "xReferenceName": None,
            "xReferenceDescription": None
            }
        ]

        ######################################################################################################################################################################

    ################################# people - NOT USED #######################################



    #    people  = []
    #    people =  [
    #    {
    #      "preTitle": None,
    #      "title": None,
    #      "forenames": [
    #       None
    #      ],
    #      "surname": None,
    #      "dateOfBirth": None,
    #      "dateOfDeath": None
    #    }
    #  ]

            
        #    people  = []
        #    people =  [
        #    {
        #      "preTitle": None,
        #      "title": None,
        #      "forenames": [
        #       None
        #      ],
        #      "surname": None,
        #      "dateOfBirth": None,
        #      "dateOfDeath": None
        #    }
        #  ]

        ######################################## physicalDescriptionExtent and physicalDescriptionForm ########################################################################################################


            extent_descriptions = []

            for extent in record.findall('Extent'):
                value_elem = extent.find("extent.value")
                form_elem = extent.find("extent.form")

                value_text = value_elem.text.strip() if value_elem is not None and value_elem.text else ""
                form_text = form_elem.text.strip() if form_elem is not None and form_elem.text else ""

                if value_text or form_text:
                    extent_descriptions.append((value_text, form_text))

            physicalDescriptionExtent = extent_descriptions[0][0] if extent_descriptions else None

            physicalDescriptionForm = []
            if extent_descriptions:
                first_form = extent_descriptions[0][1]
                if first_form:
                    physicalDescriptionForm.append(f" {first_form}")
                for value, form in extent_descriptions[1:]:
                    physicalDescriptionForm.append(f"{value} {form}".strip())

    ################################# places - NOT USED #############################################################################################


    #    places  = []
    #    places = [
    #    {
    #      "description": None,
    #      "placeName": None,
    #      "parish": None,
    #      "town": None,
    #      "county": None,
    #      "country": None
    #    }
    #  ]

    ################################ referencePart ###########################################################

        referencePart = record.find("object_number")
        referencePart = referencePart.text if referencePart is not None else None

        referencePart_pattern = r"([^\/]+$)"
        referencePart_pattern_match = re.search(referencePart_pattern, referencePart)
        referencePart = referencePart_pattern_match.group()

    ################################ publicationNote ###########################################################

        publicationNote_string = record.find("publication_note")
        publicationNote_string = publicationNote_string.text if publicationNote_string is not None else None

        if publicationNote_string is not None:
            publicationNote = []
            publicationNote = [
            publicationNote_string
        ]

        elif publicationNote_string is None:
            publicationNote = []
            publicationNote = [
            None
        ]

    ################################ publicationNote ###########################################################


        relatedMaterial_description = record.find("related_material.free_text")
        relatedMaterial_description = relatedMaterial_description.text if relatedMaterial_description is not None else None

        if relatedMaterial_description is not None:
            relatedMaterial = []
            relatedMaterial = [
        {
        "xReferenceId": None,
        "description":  relatedMaterial_description
        }
    ]
        elif relatedMaterial_description is None:
            relatedMaterial = []
            relatedMaterial = [
        {
        "xReferenceId": None,
        "description":  None
        }
    ]

        ################################ publicationNote ###########################################################
                
                
            relatedMaterial_description = record.find("related_material.free_text")
            relatedMaterial_description = relatedMaterial_description.text if relatedMaterial_description is not None else None

            if relatedMaterial_description is not None:
                relatedMaterial = []
                relatedMaterial = [
            {
            "xReferenceId": None,
            "description":  relatedMaterial_description
            }
        ]
            elif relatedMaterial_description is None:
                relatedMaterial = []
                relatedMaterial = [
            {
            "xReferenceId": None,
            "description":  None
            }
        ]

        ################################# separatedMaterial ##############################################################


        #registryRecords -- not used and not in JSON template

        #restrictionsOnUse = record.find("copyright_note")
        #restrictionsOnUse = restrictionsOnUse.text if restrictionsOnUse is not None else None

        ###################################################################################################################

            
            #registryRecords -- not used and not in JSON template
            
            #restrictionsOnUse = record.find("copyright_note")
            #restrictionsOnUse = restrictionsOnUse.text if restrictionsOnUse is not None else None


        ##################################### scopeContent ##################################################################

            scopeContent_description = record.find("Content_description/content.description")
            scopeContent_description = scopeContent_description.text if scopeContent_description is not None else None

            if scopeContent_description is not None:
                scopeContent = []
                scopeContent = {
                "personNames": [
                {
                "firstName": None,
                "surname": None,
                #"startDate": None,
                #"endDate": None
            }
            ],
            "placeNames": [
            {
                "xReferenceName": None
            }
            ],
            "refferedToDate": None,
            "organizations": [
            {
                "xReferenceName": None
            }
            ],
            "description": scopeContent_description,
            "ephemera": None,
            "occupations": None,
            "schema": None
        }

            elif scopeContent_description is None:
                scopeContent = []
                scopeContent = {
                "personNames": [
                {
                "firstName": None,
                "surname": None,
                #"startDate": None,
                #"endDate": None
            }
            ],
            "placeNames": [
            {
                "xReferenceName": None
            }
            ],
            "refferedToDate": None,
            "organizations": [
            {
                "xReferenceName": None
            }
            ],
            "description": None,
            "ephemera": None,
            "occupations": None,
            "schema": None
        }

        ########################################################################################################

            #sortKey: generated automatically with ingest into MongoDB. Does not to have be included in JSON.

            #source ---> hard coded value "PA"

            #subjects --> will not be used

        ################################# subjects ##############################################################

            subjects = []
            subjects = [
                None
        ]

    ###################################################################################################################

        title = record.find("Title/title")
        title = title.text if title is not None else None

    ################################### unpublishedFindingAids ###########################################################

        unpublishedFindingAids_string = record.find("Finding_aids/finding_aids")
        unpublishedFindingAids_string = unpublishedFindingAids_string.text if unpublishedFindingAids_string is not None else None
        unpublishedFindingAids = [unpublishedFindingAids_string]

    ########################################## storing XML values in JSON dictionary ###########################################

        ##### Temporarly remove closure information for UK Parliament records that are closed (U status)

        # if Jenny asks send U closure status for UK Parliament records to Discovery then deactive this IF statement (Discovery can handle U status)

        if heldBy_information == "UK Parliament" and closureStatus == 'U':
            mask_closure_status = False
        else:
            mask_closure_status = False

        if mask_closure_status:
            record_data = { "record": {
                            #"$schema": "./PA_JSON_Schema.json",  # for schema validation in Visual Studio
                            "iaid": iaid,
                        #"replicaId": None,
                        "citableReference": citableReference,
                        "parentId": parentId,
                        #"accumulationDates": None,
                        "accruals": accruals,
                        "accessConditions": accessConditions,
                        "administrativeBackground": administrativeBackground,
                        #"appraisalInformation": appraisalInformation,
                        "arrangement": arrangement, #arrangement_system+' '+client_filepath,
                        #"batchId": None,
                        #"refIaid": None,
                        "catalogueId": catalogueId,
                        "catalogueLevel": catalogueLevel,
                        "coveringFromDate": coveringFromDate,
                        "coveringToDate": coveringToDate,
                        "chargeType": chargeType,
                        #"eDocumentId": None,
                        "coveringDates": coveringDates,
                        "custodialHistory": custodialHistory,
                        #"closureCode": closureCode,
                        #"closureStatus": closureStatus,
                        #"closureType": closureType,
                        #"recordOpeningDate": recordOpeningDate,
                        #"corporateNames": None,
                        "copiesInformation": copiesInformation,
                        "creatorName": creatorName,
                        "digitised": digitised,
                        #"dimensions": None,
                        "formerReferenceDep": formerReferenceDep,
                        "formerReferencePro": formerReferencePro,
                        "heldBy": heldBy,
                        #"immediateSourceOfAcquisition": immediateSourceOfAcquisition,
                        "language": language,
                        "legalStatus": legalStatus,
                        #"links": None,
                        "locationOfOriginals": locationOfOriginals,
                        #"mapDesignation": None,
                        #"mapScaleNumber": None,
                        #"note": None,
                        #"people": people,
                        #"physicalCondition": physicalCondition,
                        "physicalDescriptionExtent": physicalDescriptionExtent,
                        "physicalDescriptionForm": physicalDescriptionForm,
                        #"places": places,
                        "referencePart": referencePart,
                        "publicationNote": publicationNote,
                        "relatedMaterial": relatedMaterial,
                        "separatedMaterial": separatedMaterial,
                        #"restrictionsOnUse": restrictionsOnUse,
                        "scopeContent": scopeContent,
                        #"sortKey": None,
                        "source": "PA",
                        #"subjects": subjects,
                        "title": title,
                        "unpublishedFindingAids": unpublishedFindingAids
                        }

        #### JSON mapping for all other types of records
        else:
            record_data = { "record": {
                            #"$schema": "./PA_JSON_Schema.json",  # for schema validation in Visual Studio
                            "iaid": iaid,
                        #"replicaId": None,
                        "citableReference": citableReference,
                        "parentId": parentId,
                        #"accumulationDates": None,
                        "accruals": accruals,
                        "accessConditions": accessConditions,
                        "administrativeBackground": administrativeBackground,
                        #"appraisalInformation": appraisalInformation,
                        "arrangement": arrangement, #arrangement_system+' '+client_filepath,
                        #"batchId": None,
                        #"refIaid": None,
                        "catalogueId": catalogueId,
                        "catalogueLevel": catalogueLevel,
                        "coveringFromDate": coveringFromDate,
                        "coveringToDate": coveringToDate,
                        "chargeType": chargeType,
                        #"eDocumentId": None,
                        "coveringDates": coveringDates,
                        "custodialHistory": custodialHistory,
                        "closureCode": closureCode,
                        "closureStatus": closureStatus,
                        "closureType": closureType,
                        "recordOpeningDate": recordOpeningDate,
                        #"corporateNames": None,
                        "copiesInformation": copiesInformation,
                        "creatorName": creatorName,
                        "digitised": digitised,
                        #"dimensions": None,
                        "formerReferenceDep": formerReferenceDep,
                        "formerReferencePro": formerReferencePro,
                        "heldBy": heldBy,
                        #"immediateSourceOfAcquisition": immediateSourceOfAcquisition,
                        "language": language,
                        "legalStatus": legalStatus,
                        #"links": None,
                        "locationOfOriginals": locationOfOriginals,
                        #"mapDesignation": None,
                        #"mapScaleNumber": None,
                        #"note": None,
                        #"people": people,
                        #"physicalCondition": physicalCondition,
                        "physicalDescriptionExtent": physicalDescriptionExtent,
                        "physicalDescriptionForm": physicalDescriptionForm,
                        #"places": places,
                        "referencePart": referencePart,
                        "publicationNote": publicationNote,
                        "relatedMaterial": relatedMaterial,
                        "separatedMaterial": separatedMaterial,
                        #"restrictionsOnUse": restrictionsOnUse,
                        "scopeContent": scopeContent,
                        #"sortKey": None,
                        "source": "PA",
                        #"subjects": subjects,
                        "title": title,
                        "unpublishedFindingAids": unpublishedFindingAids
                        }
                    }


        def _clean_none(obj):
            """Recursively remove None values and empty containers.
            - dict: remove keys with None/empty; return None if dict becomes empty
            - list: keep only cleaned items that are not None/empty; return None if list becomes empty
            - other: return as-is (None returns None)
            """
            if obj is None:
                return None
            if isinstance(obj, dict):
                new = {}
                for k, v in obj.items():
                    cv = _clean_none(v)
                    if cv is None:
                        continue
                    if isinstance(cv, (list, dict)) and len(cv) == 0:
                        continue
                    new[k] = cv
                return new if new else None
            if isinstance(obj, list):
                new_list = []
                for item in obj:
                    ci = _clean_none(item)
                    if ci is None:
                        continue
                    if isinstance(ci, (list, dict)) and len(ci) == 0:
                        continue
                    new_list.append(ci)
                return new_list if new_list else None
            return obj

        # remove unnecessary fields (with null values only) if requested
        if remove_empty_fields:
            # prune None/empty fields from the record prior to writing JSON
            cleaned = _clean_none(record_data)
            if cleaned is None:
                # ensure we still write a minimal record object if everything pruned
                cleaned = {"record": {}}
            elif not isinstance(cleaned, dict) or "record" not in cleaned:
                # defensive: ensure top-level shape is preserved
                cleaned = {"record": cleaned}

            records[iaid] = cleaned
        else:
            records[iaid] = record_data

        # update diagnostics
        _records_processed += 1
        print(f"Processed [{i}/{_total_records}]: {(_records_processed/_total_records)*100:.0f}%", end='\r')
        sys.stdout.flush()


class NewlineToPTransformer():
    def __init__(self, target_columns: Optional[Iterable[str]] = None, match="\\n", replace="<p>"):
        self.target_columns = target_columns
        self.match = match
        self.replace = replace
        self.regex = re.compile(self.match)
        self._fitted = True

    def fit(self, df=None, **kwargs):
        # nothing to fit for newline replacement
        self._fitted = True
        return self

    def _transform_string(self, s: str) -> str:
        """Apply the newline -> <p> policy to a single string."""
        if not isinstance(s, str):
            return s
        # normalize windows/newline combos to \n but do not strip whitespace
        # — we want to preserve trailing newlines so they get replaced too.
        text = s.replace('\r\n', '\n').replace('\r', '\n')

        try:
            return self.regex.sub(self.replace, text)
        except Exception:
            # fallback: just replace newlines
            return re.sub(r'\n+', self.replace, text)

    def _walk_and_transform(self, obj):
        """Recursively walk dict/list and transform all string values in-place."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                obj[k] = self._walk_and_transform(v)
            return obj
        if isinstance(obj, list):
            for i, v in enumerate(obj):
                obj[i] = self._walk_and_transform(v)
            return obj
        if isinstance(obj, str):
            return self._transform_string(obj)
        return obj

    @staticmethod
    def _parse_part(part: str):
        m = re.match(r'^([^\[]+)(?:\[(\d+)\])?$', part)
        if not m:
            return part, None
        key = m.group(1)
        idx = int(m.group(2)) if m.group(2) is not None else None
        return key, idx

    def get_by_path(self, obj: Any, path: str, default: Any = None) -> Any:
        """Return value at dotted/bracket path or default if not found."""
        cur = obj
        for part in path.split('.'):
            key, idx = self._parse_part(part)
            if not isinstance(cur, dict):
                return default
            cur = cur.get(key, default)
            if cur is default:
                return default
            if idx is not None:
                if not isinstance(cur, list) or idx < 0 or idx >= len(cur):
                    return default
                cur = cur[idx]
        return cur


    def set_by_path(self, obj: Any, path: str, value: Any) -> bool:
        """Set value at dotted/bracket path. Returns True on success, False otherwise.
        Does not create intermediate dicts/lists — only sets when path exists."""
        cur = obj
        parts = path.split('.')
        for i, part in enumerate(parts):
            key, idx = self._parse_part(part)
            last = (i == len(parts) - 1)
            if not isinstance(cur, dict):
                return False
            if last:
                if idx is None:
                    cur[key] = value
                    return True
                # bracket index on final part
                lst = cur.get(key)
                if not isinstance(lst, list):
                    return False
                if idx < 0 or idx >= len(lst):
                    return False
                lst[idx] = value
                return True
            # traverse
            cur = cur.get(key)
            if cur is None:
                return False
            if idx is not None:
                if not isinstance(cur, list) or idx < 0 or idx >= len(cur):
                    return False
                cur = cur[idx]
        return False

    def transform_json(self, data: dict, target_columns: Optional[Iterable[str]] = None, json_id: Optional[int] = None, **kwargs) -> dict:
        """
        If target_columns is None, apply newline -> <p> to every string value in the JSON (like YNaming).
        If target_columns is provided, keep existing per-field behaviour.
        """
        payload = copy.deepcopy(data)

        if target_columns is None:
            # apply to all string fields and log transformations when json_id provided
            def _walk_and_transform_and_log(obj, parent_path=''):
                if isinstance(obj, dict):
                    for k, v in list(obj.items()):
                        path = f"{parent_path}.{k}" if parent_path else k
                        obj[k] = _walk_and_transform_and_log(v, path)
                    return obj
                if isinstance(obj, list):
                    for i, v in enumerate(list(obj)):
                        path = f"{parent_path}[{i}]" if parent_path else f"[{i}]"
                        obj[i] = _walk_and_transform_and_log(v, path)
                    return obj
                if isinstance(obj, str):
                    original = obj
                    new = self._transform_string(original)
                    if new != original:
                        # compute highlight ranges for diagnostics similar to per-field mode
                        try:
                            matches = list(self.regex.finditer(original))
                            orig_ranges = []
                            trans_ranges = []
                            offset = 0
                            for m in matches:
                                s = m.start()
                                e = m.end()
                                orig_ranges.append([s, e])
                                tstart = s + offset
                                tend = tstart + len(self.replace)
                                trans_ranges.append([tstart, tend])
                                offset += len(self.replace) - (e - s)
                        except Exception:
                            orig_ranges = None
                            trans_ranges = None
                        #TODO:  use logger to log transformation
                        """
                        if json_id is not None and log_transformation:
                            try:
                                log_transformation(json_id, header=parent_path, desc=f"Replaced '{self.match}' with '{self.replace}'", match=True, orig_ranges=orig_ranges, trans_ranges=trans_ranges)
                            except Exception:
                                try:
                                    log_transformation(json_id, header=parent_path, desc=f"Replaced '{self.match}' with '{self.replace}'", match=True)
                                except Exception:
                                    pass
                        """
                    return new
                return obj

            _walk_and_transform_and_log(payload, parent_path='')
            return payload

        # ...existing per-field logic when `fields` is provided...
        for field_path in target_columns:
            # existing code expects to resolve dotted paths; keep your current implementation
            current = self.get_by_path(payload, field_path)  # placeholder for your path getter
            if not isinstance(current, str):
                continue
            new = self._transform_string(current)
            if new != current:
                self.set_by_path(payload, field_path, new)   # placeholder for your path setter
            # existing logging of transformation if present
        return payload

    def transform(self, data, json_id=None, **kwargs):
        """Transform either a pandas DataFrame or a JSON dict.

        If self.fields is None the transformer applies to all string values (delegates to transform_json with fields=None).
        """
        # If data is a DataFrame, this transformer does not operate on it
        try:
            import pandas as pd
            is_df = isinstance(data, pd.DataFrame)
        except Exception:
            is_df = False

        if is_df:
            return data

        # Work on a deepcopy of the JSON dict
        obj = copy.deepcopy(data)

        # If no explicit fields were configured, apply to all string fields
        if not self.target_columns:
            # forward json_id so transform_json can log per-string transformations
            return self.transform_json(obj, target_columns=None, json_id=json_id)

        for field in self.target_columns:
            candidates = [field]
            if field.startswith('record.'):
                candidates.append(field[len('record.'):])
            else:
                candidates.append('record.' + field)

            for candidate in candidates:
                changed, orig_ranges, trans_ranges = self._transform_field(obj, candidate)
                #TODO: use logger to log transformation
                """
                if changed and json_id is not None and log_transformation:
                    try:
                        log_transformation(json_id, header=candidate, desc=f"Replaced '{self.match}' with '{self.replace}'", match=True, orig_ranges=orig_ranges, trans_ranges=trans_ranges)
                    except Exception:
                        log_transformation(json_id, header=candidate, desc=f"Replaced '{self.match}' with '{self.replace}'", match=True)
                    break
                """
        return obj

    def _transform_field(self, obj, field_path):
        parts = field_path.split('.')
        cur = obj
        for i, part in enumerate(parts):
            if '[' in part and part.endswith(']'):
                name, idx = part[:-1].split('[')
                try:
                    idx = int(idx)
                except Exception:
                    return False, None, None
                if name:
                    cur = cur.get(name, []) if isinstance(cur, dict) else None
                if isinstance(cur, list) and len(cur) > idx:
                    if i == len(parts) - 1:
                        if isinstance(cur[idx], str):
                            original = cur[idx]
                            matches = list(self.regex.finditer(original))
                            if not matches:
                                return False, None, None
                            new = self.regex.sub(self.replace, original)
                            orig_ranges = []
                            trans_ranges = []
                            offset = 0
                            for m in matches:
                                s = m.start()
                                e = m.end()
                                orig_ranges.append([s, e])
                                tstart = s + offset
                                tend = tstart + len(self.replace)
                                trans_ranges.append([tstart, tend])
                                offset += len(self.replace) - (e - s)
                            cur[idx] = new
                            return True, orig_ranges, trans_ranges
                    else:
                        cur = cur[idx]
                else:
                    return False, None, None
            else:
                if i == len(parts) - 1:
                    if isinstance(cur, dict) and part in cur and isinstance(cur[part], str):
                        original = cur[part]
                        matches = list(self.regex.finditer(original))
                        if not matches:
                            return False, None, None
                        new = self.regex.sub(self.replace, original)
                        orig_ranges = []
                        trans_ranges = []
                        offset = 0
                        for m in matches:
                            s = m.start()
                            e = m.end()
                            orig_ranges.append([s, e])
                            tstart = s + offset
                            tend = tstart + len(self.replace)
                            trans_ranges.append([tstart, tend])
                            offset += len(self.replace) - (e - s)
                        cur[part] = new
                        return True, orig_ranges, trans_ranges
                else:
                    cur = cur.get(part, None) if isinstance(cur, dict) else None
        return False, None, None


class YNamingTransformer():
    """Transformer for applying Y naming conventions."""

    def __init__(self,
                 target_columns: Optional[List[str]] = None,
                 backup_original: bool = True):
        """Initialize the transformer."""
        self.logger = logging.getLogger("pipeline.transformers.y_naming")
        # Default columns to process
        self.target_columns = target_columns
        self.backup_original = backup_original
        # Loaded definitive reference set (optional)
        self._refs = None

    def transform(self, data, json_id=None, **kwargs):
        # Delegate to transform_json; apply to all if target_columns is None
        return self.transform_json(data, target_columns=self.target_columns, json_id=json_id)

    # regex to find embedded candidate tokens: requires at least one slash
    _embedded_token_re = re.compile(r'([A-Z0-9-]+(?:/[A-Z0-9-]+)+/?)')

    def apply_if_reference(self, text: str) -> str:
        """
        If `text` is syntactically reference-like and present in the loaded definitive set,
        apply Y naming and return the transformed value. Otherwise return original text.
        """

        if not isinstance(text, str):
            return text
        # If the whole field is a canonical reference-like string, handle as before
        if self._is_reference_like(text):
            # if definitive set not loaded, we conservatively treat syntactic matches as references
            # (caller should load the set for stricter behavior)
            if self._refs is None:
                result = self._apply_y_naming(text)
                return result
            # exact membership check
            if text in self._refs:
                result = self._apply_y_naming(text)
                return result
            return text

        # Otherwise, attempt to find embedded reference-like tokens anywhere in the text
        try:
            new_text = self._replace_embedded_references(text)
            if new_text != text:
                self.logger.debug(f"Applied Y naming to embedded references in: '{text}' -> '{new_text}'")
            return new_text
        except Exception as e:
            self.logger.warning(f"Error processing embedded references in '{text}': {e}")
            # On any unexpected error, fall back to original text
            return text


    def _replace_embedded_references(self, text: str) -> str:
        """Find embedded reference-like tokens in `text` and replace each with its Y-named equivalent.

        Behavior:
        - Finds candidate tokens matching `_embedded_token_re` (requires at least one slash).
        - For each token: if `is_reference_like(token)` and either no `_refs` loaded or the token (normalized) is in `_refs`,
          replace the token with `_apply_y_naming(token)`; otherwise leave it unchanged.
        - Preserves surrounding text and punctuation.
        """

        if not isinstance(text, str):
            return text

        def repl(m: re.Match) -> str:
            token = m.group(1)
            # quick syntactic check on the token itself
            if not self._is_reference_like(token):
                return token
            # membership check if definitive set loaded
            if self._refs is not None:
                if token in self._refs:
                    return self._apply_y_naming(token)
                else:
                    return token
            # no definitive set loaded: apply algorithmic transform
            return self._apply_y_naming(token)

        # Use sub with function to handle multiple occurrences and preserve other text
        return self._embedded_token_re.sub(repl, text)

    # ----- JSON-dict transform API for pipeline runtime -----
    def transform_json(self, data: dict, target_columns: Optional[List[str]] = None, json_id: Optional[int] = None) -> dict:
        """Apply Y-naming to a JSON dict for the given field paths and log changes.

        fields: list of dotted field paths like 'record.title' or 'record.relatedMaterial[0].description'
        If fields is None, apply to ALL string values in the JSON recursively.
        """
        obj = copy.deepcopy(data)

        # Important: treat fields=None as the signal to apply to ALL string values.
        # If caller passes an explicit list (possibly empty), only those fields are processed.
        if target_columns is None:
            # Apply to all string values recursively
            self._transform_all_strings_json(obj, json_id)
            return obj

        # Explicit list of target columns: transform only those paths (string or nested containers)
        for field in target_columns:
            candidates = [field]
            if field.startswith('record.'):
                candidates.append(field[len('record.'):])
            else:
                candidates.append('record.' + field)

            for candidate in candidates:
                if self._transform_target_path(obj, candidate):
                    break  # stop after first matching candidate variant
        return obj

    def _transform_target_path(self, obj: dict, path: str) -> bool:
        """Apply transformation at a dotted path.
        If final value is:
          - string: transform directly
          - dict/list: recurse inside it (all strings)
        Returns True if any change occurred.
        """
        parts = path.split('.')
        cur = obj
        for i, part in enumerate(parts):
            last = (i == len(parts) - 1)
            if not isinstance(cur, dict) or part not in cur:
                return False
            val = cur[part]
            if last:
                if isinstance(val, str):
                    new_val = self.apply_if_reference(val)
                    if new_val != val:
                        cur[part] = new_val
                        return True
                    return False
                if isinstance(val, (dict, list)):
                    # snapshot before
                    before = json.dumps(val, ensure_ascii=False, sort_keys=True) if isinstance(val, dict) else str(val)
                    self._transform_all_strings_json(val, json_id=None)
                    after = json.dumps(val, ensure_ascii=False, sort_keys=True) if isinstance(val, dict) else str(val)
                    return before != after
                return False
            else:
                cur = val
        return False

    def _transform_field_json(self, obj, field_path):
        parts = field_path.split('.')
        cur = obj
        for i, part in enumerate(parts):
            if '[' in part and part.endswith(']'):
                name, idx = part[:-1].split('[')
                try:
                    idx = int(idx)
                except Exception:
                    return False
                if name:
                    cur = cur.get(name, []) if isinstance(cur, dict) else None
                if isinstance(cur, list) and len(cur) > idx:
                    if i == len(parts) - 1:
                        if isinstance(cur[idx], str):
                            original = cur[idx]
                            new = self.apply_if_reference(original)
                            cur[idx] = new
                            return original != new
                    else:
                        cur = cur[idx]
                else:
                    return False
            else:
                if i == len(parts) - 1:
                    if isinstance(cur, dict) and part in cur and isinstance(cur[part], str):
                        original = cur[part]
                        new = self.apply_if_reference(original)
                        cur[part] = new
                        return original != new
                else:
                    cur = cur.get(part, None) if isinstance(cur, dict) else None
        return False

    def _compute_ranges_for_pair(self, original: Optional[str], new: Optional[str]):
        """Compute simple changed ranges between two strings.

        Returns (orig_ranges, trans_ranges) where each is either None or a list of [start,end]
        pairs (end is exclusive). If inputs are not strings, returns (None, None).
        This is a lightweight implementation sufficient for diagnostics highlighting.
        """
        if not isinstance(original, str) or not isinstance(new, str):
            return None, None

        # Fast path: identical
        if original == new:
            return [], []

        lo = len(original)
        ln = len(new)
        # longest common prefix
        i = 0
        m = min(lo, ln)
        while i < m and original[i] == new[i]:
            i += 1
        # longest common suffix after the prefix
        j = 0
        while j < (m - i) and original[lo - 1 - j] == new[ln - 1 - j]:
            j += 1

        orig_start = i
        orig_end = lo - j
        new_start = i
        new_end = ln - j

        orig_ranges = [] if orig_start >= orig_end else [[orig_start, orig_end]]
        trans_ranges = [] if new_start >= new_end else [[new_start, new_end]]
        return orig_ranges, trans_ranges

    def _apply_y_naming(self, text: str) -> str:
        """
        Apply Y naming conventions to a reference string.

        Rules:
        1. Add prefix 'Y' to the reference (before the letter code)
        2. If adding 'Y' makes the letter code exceed 4 characters, drop the last letter to keep it at 4
        3. Special case: 'PARL' → 'YUKP' (ignore other rules)
        4. If reference already starts with 'Y', don't add another Y (prevent double-application)
        """

        if not isinstance(text, str) or not text.strip():
            return text

        ref = text.strip()
        if not ref:
            return text

        # Split by slash to get the prefix (letter code)
        parts = ref.split('/')
        if len(parts) == 0:
            return text

        prefix = parts[0].strip().upper()
        suffix = '/' + '/'.join(parts[1:]) if len(parts) > 1 else ''

        # Apply naming rules
        # Only apply Y-prefixing for prefixes that are purely alphabetic.
        # This avoids incorrectly modifying references whose first token is numeric
        # or alphanumeric (e.g. '3' or 'PLAN1852'). If the prefix contains any
        # digits or non-alpha characters we conservatively leave the reference
        # unchanged.
        if not parts[0].strip().isalpha():
            return text

        if prefix == 'PARL':
            # Special case: PARL → YUKP
            new_prefix = 'YUKP'
        elif prefix.startswith('Y'):
            # Already has Y prefix - don't add another Y to prevent double-application
            new_prefix = prefix
        else:
            # Add Y prefix to the letter code
            temp_prefix = 'Y' + prefix
            # CRITICAL 5-LETTER RULE: If adding Y makes it exceed 4 characters, trim the last character
            if len(temp_prefix) > 4:
                new_prefix = temp_prefix[:4]  # Keep only first 4 characters (Y + first 3 of original)
            else:
                new_prefix = temp_prefix

        result = new_prefix + suffix
        return result

    def _transform_all_strings_json(self, obj, json_id):
        """Recursively apply Y-naming to all string values in the JSON object."""
        def _recurse_and_transform(current_obj, path=""):
            if isinstance(current_obj, dict):
                for key, value in current_obj.items():
                    current_path = f"{path}.{key}" if path else key
                    if isinstance(value, str):
                        original = value
                        new = self.apply_if_reference(original)
                        if new != original:
                            current_obj[key] = new
                    else:
                        _recurse_and_transform(value, current_path)
            elif isinstance(current_obj, list):
                for i, item in enumerate(current_obj):
                    current_path = f"{path}[{i}]"
                    if isinstance(item, str):
                        original = item
                        new = self.apply_if_reference(original)
                        if new != original:
                            current_obj[i] = new
                    else:
                        _recurse_and_transform(item, current_path)
        _recurse_and_transform(obj)

    def _is_reference_like(self, s: str) -> bool:
        """Quick syntactic check: at least one slash, at most 9 slashes, tokens uppercase alnum/hyphen.

        Accept a trailing slash (e.g. 'ABC/') by allowing an empty final token, but reject empty
        tokens elsewhere (so 'ABC//DEF' and '///' are rejected).
        """
        if not isinstance(s, str):
            return False
        orig = s
        s = s.strip()
        if len(s) < 2 or len(s) > 250:
            return False

        # explicit exclusion: any token starting with "APT/" should be rejected (case-insensitive)
        # We use a word boundary before APT to avoid matching inside longer tokens like CAPT/.
        if re.search(r'(?i)\bAPT/', orig):
            return False

        # check that the token contains 1 to 9 slashes
        slash_count = s.count('/')
        if slash_count < 1 or slash_count > 9:
            return False

        raw_toks = s.split('/')
        toks = [t.strip() for t in raw_toks]
        if len(toks) < 2 or len(toks) > 10:
            return False

        # Reject if any token had leading/trailing whitespace originally (e.g. ' DEF')
        for raw, tok in zip(raw_toks, toks):
            if raw != tok:
                return False
            if tok == '':
                return False
            if not re.match(r'^[A-Za-z0-9-]+$', tok):
                return False

        # Additional rule: first (prefix) token must be purely alphabetic (no digits or hyphens)
        # This enforces rejection of examples like 'XYZ-12/ABC-3' and 'A1B2C3/456'.
        if not re.match(r'^[A-Za-z]+$', toks[0]):
            return False
        # Prefix must be at least 1 alphabetic character (reject empty like '/1').
        if len(toks[0]) > 1 or toks[0] == 'S':
            return True
        else:
            return False

        return True


    