package org.endeavourhealth.patientfhirextractor.constants;

public interface ResourceConstants {

    public static final String SYSTEM_ID = "https://fhir.hl7.org.uk/Id/dds";

    //Patient
     String META_SYSTEM="https://fhir.nhs.uk/Id/ODS-Code";
    String IDENTIFIER_URL = "https://fhir.hl7.org.uk/STU3/StructureDefinition/Extension-CareConnect-NHSNumberVerificationStatus-1";
    String CODING_SYSTEM = "https://fhir.hl7.org.uk/STU3/CodeSystem/CareConnect-NHSNumberVerificationStatus-1";
    String VALUE_CODEABLE_CONCEPT = "valueCodeableConcept";
    String NHS_NUMBER_IDENTIFIER = "https://fhir.nhs.uk/Id/nhs-number";

    //MESSAGE_HEADER
    public static final String EVENT_SYSTEM="http://fhir.patientsknowbest.com/codesystem/message-event";
    public static final String EVENT_CODE="create-or-update-patient";
    public static final String URL="pkb/$process-message";
}
