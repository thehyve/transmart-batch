package org.transmartproject.batch.clinical

import org.hamcrest.Matcher
import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.beans.PersistentContext
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.junit.JobRunningTestTrait
import org.transmartproject.batch.junit.RunJobRule
import org.transmartproject.batch.support.TableLists
import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*


/**
 *
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataPartialUploadTests implements JobRunningTestTrait {

    public static final String STUDY_ID = 'GSE8581'
    public static final String PARENT_FOLDER = '\\Public Studies\\'
    public static final String STUDY_BASE_FOLDER = "${PARENT_FOLDER}${STUDY_ID}\\"

    static pathToReupload = '\\Public Studies\\GSE8581\\Endpoints\\'
    static topNode = '\\Public Studies\\GSE8581\\'
    static incrementalPath = '\\Public Studies\\GSE8581\\Endpoints\\Endpoints'

    private static final BigDecimal DELTA = 0.005


    @ClassRule
    public final static TestRule RUN_JOB_RULE = new RuleChain([
            new RunJobRule("${STUDY_ID}", 'clinical', ['-d', 'TOP_NODE=' + pathToReupload,
                                                       '-d', 'APPEND_FACTS=Y', '-n']),
            new RunJobRule("${STUDY_ID}", 'clinical', ['-n']),
    ])

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testPatientLinkRemains() {
        def q = """
            SELECT *
            FROM ${Tables.PATIENT_DIMENSION}
            WHERE sourcesystem_cd = :ss"""
        def p = [ss: 'GSE8581:GSE8581GSM211865']

        List<Map<String, Object>> r = queryForList q, p
        assert !r.empty
    }

    @Test
    void testI2b2AndConceptDimensionMatch() {
        long numI2b2 = rowCounter.count(
                Tables.I2B2,
                'sourcesystem_cd = :ss',
                ss: STUDY_ID)

        assertThat numI2b2, is(greaterThan(0L))

        def q
        def numJoined

        // they should match through the concept path
        q = """
            SELECT COUNT(*)
            FROM ${Tables.I2B2} I
            INNER JOIN ${Tables.CONCEPT_DIMENSION} D
                ON (I.c_fullname = D.concept_path)
            WHERE I.sourcesystem_cd = :study"""
        numJoined = jdbcTemplate.queryForObject(q, [study: STUDY_ID], Long)

        assertThat numJoined, is(equalTo(numI2b2))

        // they should also match through the "basecode"
        q = """
            SELECT COUNT(*)
            FROM ${Tables.I2B2} I
            INNER JOIN ${Tables.CONCEPT_DIMENSION} D
                ON (I.c_basecode = D.concept_cd)
            WHERE I.sourcesystem_cd = :study"""
        numJoined = jdbcTemplate.queryForObject(q, [study: STUDY_ID], Long)

        assertThat numJoined, is(equalTo(numI2b2))
    }

    @Test
    void testFactValuesForOriginalPatient() {
        /* GSE8581GSM211865 Homo sapiens caucasian male 69 year 67 inch 71
            lung D non-small cell squamous cell carcinoma 2.13 */

        // Test a numerical and a categorical concept
        def expected = [
                'Endpoints\\FEV1\\': 2.13,
                'Subjects\\Organism\\Homo sapiens\\': 'Homo sapiens',
        ]

        def r = factsForPatient('GSE8581GSM211865')

        assertThat r, hasItems(
                expected.collect { pathEnding, value ->
                    allOf(
                            hasEntry(is('concept_path'), endsWith(pathEnding)),
                            value instanceof String ?
                                    hasEntry(is('tval_char'), is(value)) :
                                    hasEntry(is('nval_num'), closeTo(value, DELTA))
                    )
                } as Matcher[]
        )
    }

    @Test
    void testFactValuesForAddedStudyPatient() {
        /* GSE8581GSM211865 Homo sapiens caucasian male 69 year 67 inch 71
            lung D non-small cell squamous cell carcinoma 2.13 */

        // Test a numerical and a categorical concept
        def expected = [
                'Endpoints\\Endpoints\\FEV1\\': 2.13,
                'Endpoints\\Subjects\\Organism\\Homo sapiens\\': 'Homo sapiens',
        ]

        def r = factsForPatient('GSE8581GSM211865')

        assertThat r, hasItems(
                expected.collect { pathEnding, value ->
                    allOf(
                            hasEntry(is('concept_path'), endsWith(pathEnding)),
                            value instanceof String ?
                                    hasEntry(is('tval_char'), is(value)) :
                                    hasEntry(is('nval_num'), closeTo(value, DELTA))
                    )
                } as Matcher[]
        )
    }

    // maps have keys concept_path, tval_char and nval_num
    private List<Map<String, ?>> factsForPatient(String patient) {
        def q = """
            SELECT C.concept_path, O.tval_char, O.nval_num
            FROM
                ${Tables.OBSERVATION_FACT} O
                INNER JOIN ${Tables.CONCEPT_DIMENSION} C
                    ON (O.concept_cd = C.concept_cd)
            WHERE patient_num = (
                SELECT patient_num
                FROM ${Tables.PATIENT_DIMENSION}
                WHERE sourcesystem_cd = :patient)"""

        queryForList q, [patient: "GSE8581:$patient"]
    }

    @Test
    void testPatientsLinkedToConcepts(){
        def r1 = patientsLinkedToConcept("\\Public Studies\\GSE8581\\Endpoints\\Diagnosis\\NSC-Mixed\\")
        def r2 = patientsLinkedToConcept("\\Public Studies\\GSE8581\\Subjects\\Lung Disease\\control\\")
        def r3 = patientsLinkedToConcept("\\Public Studies\\GSE8581\\Endpoints\\Subjects\\Ethnicity\\Caucasian\\")
        def r4 = patientsLinkedToConcept("\\Public Studies\\GSE8581\\Endpoints\\Endpoints\\Diagnosis\\hematoma\\")
        assert !r1.empty
        assert !r2.empty
        assert !r3.empty
        assert !r4.empty
    }


    private List<Map<String, ?>> patientsLinkedToConcept(String conceptPath) {
        def q = """
                SELECT patient_num
                FROM ${Tables.OBSERVATION_FACT}
                WHERE concept_cd = (SELECT concept_cd
                        FROM ${Tables.CONCEPT_DIMENSION}
                        WHERE concept_path = :conceptpath)"""

        queryForList q, [conceptpath: conceptPath]
    }

    @Test
    void testSingleConceptCountRecord() {
        long conceptCountRecords = rowCounter.count(
                Tables.CONCEPT_COUNTS,
                'concept_path = :conceptPath',
                conceptPath: '\\Public Studies\\GSE8581\\Subjects\\Lung Disease\\control\\')

        assertThat conceptCountRecords, equalTo(1L)
    }

    @SuppressWarnings('UnusedPrivateMethod')
    private getAllStudyConceptPaths(String studyId) {
        def q = "SELECT * FROM i2b2demodata.concept_dimension WHERE concept_path LIKE " +
                "'\\\\Public Studies\\\\${studyId}\\\\%'"
        queryForList(q, [:])*.concept_path
    }

}
