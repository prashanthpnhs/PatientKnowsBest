package org.endeavourhealth.patientfhirextractor.controller;

import ca.uhn.fhir.context.FhirContext;
import org.endeavourhealth.patientfhirextractor.configuration.ExporterProperties;
import org.endeavourhealth.patientfhirextractor.data.PatientEntity;
import org.endeavourhealth.patientfhirextractor.repository.PatientRepository;
import org.endeavourhealth.patientfhirextractor.resource.MessageHeader;
import org.endeavourhealth.patientfhirextractor.resource.Patient;
import org.endeavourhealth.patientfhirextractor.service.PatientService;
import org.hl7.fhir.dstu3.model.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@RestController
@RequestMapping("/api")
public class PatientRecordController {
    Logger logger = LoggerFactory.getLogger(PatientRecordController.class);

    @Autowired
    PatientRepository patientRepository;

    @Autowired
    ExporterProperties exporterProperties;

    @Autowired
    PatientService patientService;

    Patient patient;
    MessageHeader messageHeader;

    public void publishPatients() throws Exception {
        logger.info("Entering publishPatients() method");
        getUserDetails();
        logger.info("End of publishPatients() method");
    }

    public void getUserDetails() throws Exception {
        logger.info("Entering getUserDetails() method");

        List<PatientEntity> patientEntities = patientService.processPatients();
        patient = new Patient();
        messageHeader = new MessageHeader();
        Bundle bundle = null;
        FileWriter file = new FileWriter(exporterProperties.getOutputFHIR());
        try {
            for (PatientEntity patientItem : patientEntities) {
                bundle = new Bundle();
                bundle.setType(Bundle.BundleType.MESSAGE);

                bundle.addEntry().setResource(messageHeader.getMessageHeader());
                org.hl7.fhir.dstu3.model.Patient patientResource = patient.getPatientResource(patientItem);
                bundle.addEntry().setResource(patientResource);

                //TODO : POST or PUT API will be fired but now its written to a file for testing purpose.
                FhirContext ctx = FhirContext.forDstu3();
                String json = ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle);
                file.write(json);
            }
            file.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            file.close();
        }
    }

    public void postBundle(Bundle bundle)
            throws IOException {
        FhirContext ctx = FhirContext.forDstu3();
        String json = ctx.newJsonParser().setPrettyPrint(true).encodeResourceToString(bundle);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
        HttpEntity<String> request =
                new HttpEntity<>(json, headers);
    }


}
