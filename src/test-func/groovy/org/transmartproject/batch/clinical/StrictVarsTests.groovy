package org.transmartproject.batch.clinical


import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.Test
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
 * Test strict variables (where variable name and value is usually not the same e.g. Gender = Female) uploading.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class StrictVarsTests implements JobRunningTestTrait {

    public static final String STUDY_ID = 'STRICT_VARS'

    @ClassRule
    public final static TestRule RUN_JOB_RULE = new RunJobRule(STUDY_ID, 'clinical')

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testStrictConceptVariables() {
        def facts = queryForList """
            SELECT O.tval_char
            FROM
                ${Tables.OBSERVATION_FACT} O
                INNER JOIN ${Tables.CONCEPT_DIMENSION} C
                    ON (O.concept_cd = C.concept_cd)
                WHERE C.concept_path = :concept_path
            """,
                [
                        concept_path: '\\Public Studies\\STRICT_VARS\\Demography\\GENDER\\',
                ]

        assertThat facts, not(empty())
        assertThat facts, everyItem(either(hasEntry('tval_char', 'Male')) | (hasEntry('tval_char', 'Female')))
    }


}
