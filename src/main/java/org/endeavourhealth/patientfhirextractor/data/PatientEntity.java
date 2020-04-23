package org.endeavourhealth.patientfhirextractor.data;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.text.SimpleDateFormat;
import java.util.Date;


@AllArgsConstructor
@NoArgsConstructor
@Entity
@Data
@Table(name = "patient", schema = "subscriber_pi")
public class PatientEntity {

    private static final Logger LOG = LoggerFactory.getLogger(PatientEntity.class);

    @Id
    private long id;

    private String gender;
    private String lastname;
    private String title;
    private String code;
    private String firstname;
    private String nhsNumber;
    private String dob;
    private String dod;
    private String telecom;
    private String adduse;
    private String add1;
    private String add2;
    private String add3;
    private String add4;
    private String postcode;
    private String city;
    private String otheraddresses;
    private String orglocation;
    private String startdate;


}
