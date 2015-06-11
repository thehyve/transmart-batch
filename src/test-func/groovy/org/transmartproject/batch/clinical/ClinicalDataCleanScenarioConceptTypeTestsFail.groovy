package org.transmartproject.batch.clinical

import org.junit.AfterClass
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.batch.core.repository.JobRepository
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.db.TableTruncator
import org.transmartproject.batch.startup.RunJob
import org.transmartproject.batch.support.TableLists
import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Load clinical data for a study not loaded before.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class ClinicalDataCleanScenarioConceptTypeTestsFail {

    public static final String STUDY_ID = 'GSE8581_CONCEPT_TYPES_FAIL'

    @Autowired
    JobRepository repository

    @AfterClass
    static void cleanDatabase() {
        // TODO: implement backout study and call it here
        new AnnotationConfigApplicationContext(
                GenericFunctionalTestConfiguration).getBean(TableTruncator).
                truncate(
                        *TableLists.CLINICAL_TABLES,
                        'ts_batch.batch_job_instance cascade')
    }


    @Test
    void testFailLoadingData() {

        def params = ['-p', 'studies/' + STUDY_ID + '/clinical.params',]

        def runJob = RunJob.createInstance(*params)
        def intResult = runJob.run()
        assertThat 'execution is unsuccessful', intResult, is(1)


    }








}
