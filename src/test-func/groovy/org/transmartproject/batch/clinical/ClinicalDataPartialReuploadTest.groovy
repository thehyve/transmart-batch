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
import static org.transmartproject.batch.matchers.IsInteger.isIntegerNumber

/**
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataPartialReuploadTests implements JobRunningTestTrait {
    public static final String STUDY_ID1 = 'CLUC_first_half_patientset'
    public static final String STUDY_ID2 = 'CLUC_second_half_patientset'
    public static final String PARENT_FOLDER = '\\Public Studies\\'
    public static final String STUDY_ID = 'GSE8581'
    public static final String STUDY_BASE_FOLDER = "${PARENT_FOLDER}${STUDY_ID}\\"

    static topNode = '\\Public Studies\\GSE8581\\CLUC\\'

    private static final BigDecimal DELTA = 0.005

    public static final NUMBER_OF_PATIENTS = 77L
    public static final NUMBER_OF_PATIENTS_GSE8581 = 58L

    @ClassRule
    public final static TestRule RUN_JOB_RULE = new RuleChain([
            new RunJobRule("${STUDY_ID2}", 'clinical', ['-d', 'TOP_NODE=' + topNode, '-d', 'APPEND_FACTS=Y', '-n']),
            new RunJobRule("${STUDY_ID1}", 'clinical', ['-d', 'TOP_NODE=' + topNode, '-d', 'APPEND_FACTS=Y', '-n']),
            new RunJobRule("${STUDY_ID}", 'clinical'),
    ])


    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testFirstPatientConceptLinksRemain() {
        def testConcepts = ["\\Public Studies\\GSE8581\\CLUC\\Characteristics\\Cell type\\Epithelial-like cells\\": 4,
                            "\\Public Studies\\GSE8581\\Endpoints\\Diagnosis\\NSC-Mixed\\"                        : 1,
                            "\\Public Studies\\GSE8581\\Subjects\\Sex\\female\\"                                  : 30]
        def res = [:]
        testConcepts.each { key, value ->
            res[key] = patientsLinkedToConcept(key).size()
        }
        assertThat(res, is(testConcepts))
    }

    @Test
    void testFactValuesForAddedStudyPatient() {
        def expected_caco2 = [
                "\\Public Studies\\GSE8581\\CLUC\\Characteristics\\Gender\\Male\\": "Male",
                "\\Public Studies\\GSE8581\\CLUC\\Characteristics\\Age\\"         : 72,
        ]
        def expected_rko = [
                "\\Public Studies\\GSE8581\\CLUC\\Characteristics\\Age\\"                   : 82,
                "\\Public Studies\\GSE8581\\CLUC\\Characteristics\\Organism\\Homo sapiens\\": 'Homo sapiens',
        ]
        def r_caco2 = factsForPatient('GSE8581:CACO2')

        assertThat r_caco2, hasItems(
                expected_caco2.collect { pathEnding, value ->
                    allOf(
                            hasEntry(is('concept_path'), endsWith(pathEnding)),
                            value instanceof String ?
                                    hasEntry(is('tval_char'), is(value)) :
                                    hasEntry(is('nval_num'), closeTo(value, DELTA))
                    )
                } as Matcher[]
        )

        def r_rko = factsForPatient('GSE8581:RKO')

        assertThat r_rko, hasItems(
                expected_rko.collect { pathEnding, value ->
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
    void testVisualAttributeCorrect() {
        def q = """
                SELECT c_visualattributes
                FROM ${Tables.I2B2}
                WHERE c_fullname = :conceptpath"""

        def res = queryForList q, [conceptpath: topNode]

        assertThat(res[0],
                hasEntry("c_visualattributes", "FA "))
    }

    @Test
    void testPatientCountCorrect() {
        // TODO Looks like everytime concept counts are recalculated the old calculations lives on in the database
        List rows = queryForList("""
            select concept_path, patient_count
            from ${Tables.CONCEPT_COUNTS}
            where concept_path like :path escape '^'
            """, [path: STUDY_BASE_FOLDER + '%'])
        assertThat rows, allOf(
                hasItem(allOf(
                        hasEntry(is('concept_path'), equalTo(STUDY_BASE_FOLDER)),
                        hasEntry(is('patient_count'), isIntegerNumber(NUMBER_OF_PATIENTS))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\Endpoints\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(NUMBER_OF_PATIENTS_GSE8581))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\Endpoints\\Diagnosis\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(NUMBER_OF_PATIENTS_GSE8581))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\Endpoints\\Diagnosis\\carcinoid\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(3L))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\CLUC\\Characteristics\\Cell type\\Epithelial-like cells\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(4L))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\CLUC\\Characteristics\\Race\\Caucasian\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(17L))
                )),
                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\CLUC\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(19L))
                )),

                hasItem(allOf(
                        hasEntry(is('concept_path'), endsWith("\\CLUC\\Characteristics\\Disease\\Prostatic carcinoma: derived from metastatic site - vertebral metastasis\\")),
                        hasEntry(is('patient_count'), isIntegerNumber(1L))
                )),
        )

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

        queryForList q, [patient: patient]
    }


}
