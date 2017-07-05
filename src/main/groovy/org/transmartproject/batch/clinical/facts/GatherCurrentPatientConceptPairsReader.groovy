package org.transmartproject.batch.clinical.facts

import groovy.util.logging.Slf4j
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.database.JdbcCursorItemReader
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.RowMapper
import org.springframework.stereotype.Component
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.concept.ConceptPath
import org.transmartproject.batch.concept.ConceptTree
import org.transmartproject.batch.patient.Patient
import org.transmartproject.batch.patient.PatientSet

import javax.annotation.PostConstruct
import javax.sql.DataSource
import java.sql.ResultSet
import java.sql.SQLException

/**
 * Gets the unique concept patient pairs from the observations (for the study) from database.
 */
@Slf4j
@Component
@JobScope
class GatherCurrentPatientConceptPairsReader implements ItemStreamReader<PatientConceptPair> {

    @Delegate
    JdbcCursorItemReader<PatientConceptPair> delegate

    @Autowired
    DataSource dataSource

    @Autowired
    ConceptTree conceptTree

    @Autowired
    PatientSet patientSet

    @PostConstruct
    void init() {
        delegate = new JdbcCursorItemReader<>(
                driverSupportsAbsolute: true,
                dataSource: dataSource,
                sql: sql,
                rowMapper: this.&mapRow as RowMapper<Patient>)

        delegate.afterPropertiesSet()
    }

    @Value("#{jobParameters['STUDY_ID']}")
    String studyId

    private String getSql() {
        """
                SELECT
                    p.sourcesystem_cd, c.concept_path
                FROM ${Tables.OBSERVATION_FACT} o
                INNER JOIN ${Tables.PATIENT_DIMENSION} p on p.patient_num = o.patient_num
                INNER JOIN ${Tables.CONCEPT_DIMENSION} c on c.concept_cd = o.concept_cd
                WHERE o.sourcesystem_cd = '${studyId}'"""
    }

    @SuppressWarnings('UnusedPrivateMethodParameter')
    private PatientConceptPair mapRow(ResultSet rs, int rowNum) throws SQLException {
        String subjectId = rs.getString(1)["$studyId:".length()..-1]
        String conceptPath = rs.getString(2)
        new PatientConceptPair(
                patient: patientSet[subjectId],
                conceptNode: conceptTree[new ConceptPath(conceptPath)]
        )
    }

}

