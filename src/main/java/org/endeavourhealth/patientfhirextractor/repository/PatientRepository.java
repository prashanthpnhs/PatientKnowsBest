package org.endeavourhealth.patientfhirextractor.repository;


import org.endeavourhealth.patientfhirextractor.data.PatientEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PatientRepository extends JpaRepository<PatientEntity, Long> {

}

