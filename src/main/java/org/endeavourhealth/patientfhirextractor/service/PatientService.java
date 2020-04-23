package org.endeavourhealth.patientfhirextractor.service;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.patientfhirextractor.configuration.ExporterProperties;
import org.endeavourhealth.patientfhirextractor.data.PatientEntity;
import org.endeavourhealth.patientfhirextractor.repository.PatientRepository;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import java.util.List;

@Component("patientService")
public class PatientService {
    Logger logger = LoggerFactory.getLogger(PatientService.class);

    @Autowired
    private PatientRepository patientRepository;

    @Autowired
    private ExporterProperties exporterProperties;

    @Autowired
    @PersistenceUnit
    private EntityManagerFactory entityManagerFactory;

    public List<PatientEntity> processPatients() {
        List<String> patientIds = getPatientIds();
        List<PatientEntity> patientEntities = null;
        if (patientIds.size() > 0) {
            patientEntities = getPatientFull(patientIds);
        }
        return patientEntities;
    }


    private List<String> getPatientIds() {
        String sql = "select * from pkb_extracts.pkbpatients";
        Session session = entityManagerFactory.unwrap(SessionFactory.class).openSession();
        List<String> patientIds = session.createSQLQuery(sql).list();
        session.close();
        return patientIds;
    }

    private List<PatientEntity> getPatientFull(List patientIds) {
        String dbSchema = exporterProperties.getDbschema();
        String sql = "SELECT p.id," +
                "coalesce(p.organization_id,'') as orglocation," +
                "coalesce(p.date_of_birth,'') as dob," +
                "p.date_of_death as dod," +
                "coalesce(o.ods_code,'') as code," +
                "coalesce(c.name,'') as gender," +
                "coalesce(p.nhs_number,'') as nhs_number," +
                "coalesce(p.last_name,'') as lastname," +
                "coalesce(p.first_names,'') as firstname," +
                "coalesce(p.title,'') as title," +
                "coalesce(a.address_line_1,'') as add1," +
                "coalesce(a.address_line_2,'') as add2," +
                "coalesce(a.address_line_3,'') as add3," +
                "coalesce(a.address_line_4,'') as add4," +
                "coalesce(a.city,'') as city," +
                "coalesce(a.postcode,'') as postcode," +
                "coalesce(e.date_registered,'') as startdate," +
                "'HOME' as adduse," +
                "'' as telecom," +
                "'' as otheraddresses " +
                "FROM " + dbSchema + "." + "patient p " +
                "join " + dbSchema + "." +"patient_address a on a.id = p.current_address_id " +
                "join " + dbSchema + "." + "concept c on c.dbid = p.gender_concept_id " +
                "join " + dbSchema + "." +"episode_of_care e on e.patient_id = p.id " +
                "join " + dbSchema + "." +"organization o on o.id = p.organization_id " +
                "join " + dbSchema + "." +"concept c2 on c2.dbid = e.registration_type_concept_id " +
                "where c2.code = 'R' " +
                "and p.date_of_death IS NULL " +
                "and e.date_registered <= now() " +
                "and (e.date_registered_end > now() or e.date_registered_end IS NULL) and p.id in (" + StringUtils.join(patientIds, ',') + ") ";
        List<PatientEntity> patients = null;

        Session session = entityManagerFactory.unwrap(SessionFactory.class).openSession();
        patients = session.createSQLQuery(sql).addEntity(PatientEntity.class).list();
        session.close();

        return patients;
    }

    public void executeProcedures() {
        Session session = null;
        try {

            session = entityManagerFactory.unwrap(SessionFactory.class).openSession();
            Transaction txn = session.beginTransaction();
            session.createSQLQuery("call pkb_extracts.initialiseTablesPKB()").executeUpdate();

            session.createSQLQuery("call pkb_extracts.createCohortforPKB()").executeUpdate();
            session.createSQLQuery("call pkb_extracts.extractsPatientsForPKB()").executeUpdate();
            session.createSQLQuery("call pkb_extracts.extractsDeletionsForPKB()").executeUpdate();
            session.createSQLQuery("call pkb_extracts.finaliseExtractForPKB()").executeUpdate();
            txn.commit();

        } catch (Exception ex) {
            logger.error("", ex.getCause());
        } finally {
            session.close();
        }
    }

}
