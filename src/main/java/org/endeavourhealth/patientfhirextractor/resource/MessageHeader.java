package org.endeavourhealth.patientfhirextractor.resource;

import org.endeavourhealth.patientfhirextractor.constants.ResourceConstants;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.UriType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

public class MessageHeader {

    private static final Logger LOG = LoggerFactory.getLogger(MessageHeader.class);

    public org.hl7.fhir.dstu3.model.MessageHeader getMessageHeader() {
        LOG.info("Entering getMessageHeader() method");
        org.hl7.fhir.dstu3.model.MessageHeader messageHeader = new org.hl7.fhir.dstu3.model.MessageHeader();

        UUID uuid = UUID.randomUUID();

        messageHeader.setId(uuid.toString());

        Coding coding = new Coding();
        coding.setSystem(ResourceConstants.EVENT_SYSTEM);
        coding.setCode(ResourceConstants.EVENT_CODE);
        messageHeader.setEvent(coding);

        //TODO: Need to confirm source response from PKB.
        org.hl7.fhir.dstu3.model.MessageHeader.MessageSourceComponent messageSourceComponent = new org.hl7.fhir.dstu3.model.MessageHeader.MessageSourceComponent();
        messageSourceComponent.setName("Acme Central Patient Registry");
        UriType type = new UriType();
        type.setValue("https://data.developer.nhs.uk/ccri-fhir/STU3/Patient");
        messageSourceComponent.setEndpointElement(type);
        messageHeader.setSource(messageSourceComponent);

        //TODO: Need to confirm PKB destination endpoint
        org.hl7.fhir.dstu3.model.MessageHeader.MessageDestinationComponent messageDestinationComponent = new org.hl7.fhir.dstu3.model.MessageHeader.MessageDestinationComponent();
        messageDestinationComponent.setName("PKB Message Gateway");
        messageHeader.setDestination(Arrays.asList(messageDestinationComponent));

        messageHeader.setTimestamp(new Date());

        LOG.info("End getMessageHeader() method");
        return messageHeader;
    }

}


