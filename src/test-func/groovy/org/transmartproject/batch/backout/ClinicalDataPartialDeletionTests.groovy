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
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.junit.JobRunningTestTrait
import org.transmartproject.batch.junit.RunJobRule
import org.transmartproject.batch.support.TableLists

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.allOf
import static org.hamcrest.Matchers.containsInAnyOrder
import static org.hamcrest.Matchers.empty
import static org.hamcrest.Matchers.hasItem
import static org.hamcrest.Matchers.not
import static org.hamcrest.Matchers.contains

/**
 * Tests private clinical study reupload.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataPartialDeletionTests implements JobRunningTestTrait {

    public static final String STUDY_ID = 'GSE8581'

    static pathToRemove = '\\Private Studies\\GSE8581\\Endpoints\\'

    @ClassRule
    public final static TestRule RUN_JOB_RULE = new RuleChain([
            new RunJobRule("${STUDY_ID}", 'backout',  ['-d', 'SECURITY_REQUIRED=Y', '-d', 'TOP_NODE=' + pathToRemove]),
            new RunJobRule("${STUDY_ID}", 'clinical', ['-d', 'SECURITY_REQUIRED=Y']),
    ])

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
    // test that HD nodes and TOP_NODE remain

    @Test
    void testFolderAndSubfoldersRemoved() {
        def conceptPaths = getAllStudyConceptPaths(STUDY_ID)
        assertThat(conceptPaths, allOf(
                not(empty()),
                not(hasItem(pathToRemove))))
    }

    @Test
    void testStudyFolderPersists() {
        def conceptPaths = getAllStudyConceptPaths(STUDY_ID)
        assertThat(conceptPaths, hasItem("\\Private Studies\\GSE8581\\Subjects\\"))
    }

    @Test
    void testTopStudyNodePersists(){

    }

    def getAllStudyConceptPaths(String studyId) {
        def q = "SELECT concept_path FROM i2b2demodata.concept_dimension WHERE concept_path LIKE '\\\\Private Studies\\\\${studyId}\\\\%'"
        queryForList(q, [:]).collect {
            it.concept_path
        }
    }

}
