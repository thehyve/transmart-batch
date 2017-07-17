package org.transmartproject.batch.backout

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
import org.transmartproject.batch.clinical.ClinicalDataCleanScenarioTests
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.junit.JobRunningTestTrait
import org.transmartproject.batch.junit.RunJobRule
import org.transmartproject.batch.support.StringUtils
import org.transmartproject.batch.support.TableLists

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Tests private clinical study reupload.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataPartialDeletionTests implements JobRunningTestTrait {

    public static final String STUDY_ID = 'GSE8581'

    static topNode = '\\Private Studies\\GSE8581\\Endpoints\\'

    @ClassRule
    public final static TestRule RUN_JOB_RULES = new RuleChain([
            new RunJobRule("${STUDY_ID}", 'backout', ['-d', 'SECURITY_REQUIRED=Y', '-d',
                                                      'TOP_NODE=' + topNode, '-d', 'INCLUDED_TYPES=clinical']),
            new RunJobRule("${STUDY_ID}", 'clinical', ['-d', 'SECURITY_REQUIRED=Y']),
    ])

    public final static TestRule RUN_JOB_RULE =
            RUN_JOB_RULES.rulesStartingWithInnerMost[0]

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + 'ts_batch.batch_job_instance')
    }

    // Test to see if folders and subfolders in tree are gone
    // Test to see if the observations are removed from endpoints
    // Test to see if the observations from subjects are preserved.
    // Test to see if subjects folder is preserved
    // test concept counts

    @Test
    void testStudyFolderPersists() {
        def conceptPaths = getAllStudyConceptPaths(STUDY_ID)
        assertThat(conceptPaths, hasItem("\\Private Studies\\GSE8581\\Subjects\\"))
    }

    @Test
    void testTopStudyNodePersists() {
        def conceptPaths = getAllStudyConceptPaths(STUDY_ID)
        assertThat(conceptPaths, hasItem(topNode))
    }

    //Is it Observation or fact?
    @Test
    void testObservationsDeleted() {
        def results = getAllObservations(STUDY_ID)
        results.each {
            assertThat(it["concept_path"], not(containsString(topNode)))
        }
    }

    @Test
    void testObservationsRemain() {
        def results = getAllObservations(STUDY_ID)
        def remainedConcept = "\\Private Studies\\GSE8581\\Subjects\\"
        results.each {
            assertThat(it["concept_path"], (containsString(remainedConcept)))
        }
    }

    @Test
    void testConceptCountsRemain() {
        assertThat rowCounter.count(Tables.CONCEPT_COUNTS), is(17L)
    }

    //TODO: Make patients remain for partial deletion while using ClinicalOnly and Full
    @Test
    void testSubjectsRemain() {
        long numPatientDim = rowCounter.count(
                Tables.PATIENT_DIMENSION,
                'sourcesystem_cd LIKE :pat',
                pat: "$STUDY_ID:%")

        assertThat numPatientDim, is(ClinicalDataCleanScenarioTests.NUMBER_OF_PATIENTS)
    }

    def getAllStudyConceptPaths(String studyId) {
        def q = "SELECT concept_path FROM i2b2demodata.concept_dimension WHERE concept_path LIKE :path ESCAPE '\\'"
        queryForList(q, [path: StringUtils.escapeForLike("\\Private Studies\\${studyId}\\") + '%'])*.concept_path
    }

    def getAllObservations(String studyId) {
        def q = """
            SELECT DISTINCT C.concept_path
            FROM ${Tables.OBSERVATION_FACT} O
            INNER JOIN ${Tables.CONCEPT_DIMENSION} C on C.concept_cd = O.concept_cd
            WHERE O.sourcesystem_cd = :ss"""

        queryForList q, [ss: studyId]
    }

}
