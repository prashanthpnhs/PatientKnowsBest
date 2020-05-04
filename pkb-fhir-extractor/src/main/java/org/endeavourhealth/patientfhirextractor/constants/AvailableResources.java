package org.endeavourhealth.patientfhirextractor.constants;

public enum AvailableResources {

    PATIENT("Patient"),
    ORGANIZATION("Organization");

    private String resourceName;

    AvailableResources(String resourceName){
        this.resourceName = resourceName;
    }
}
