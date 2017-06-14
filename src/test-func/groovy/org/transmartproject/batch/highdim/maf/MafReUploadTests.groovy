package org.transmartproject.batch.highdim.maf

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

/**
 * test MAF data reupload
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class MafReUploadTests implements JobRunningTestTrait {

    private final static String PLATFORM = 'MAF_HG19'
    private final static String STUDY_ID = 'BRCA_TCGA'

    @ClassRule
    public final static TestRule RUN_JOB_RULES = new RuleChain([
            new RunJobRule(STUDY_ID, 'maf', ['-n']),
            new RunJobRule(STUDY_ID, 'maf'),
            new RunJobRule(STUDY_ID, 'clinical'),
    ])

    // needed by the trait
    public final static TestRule RUN_JOB_RULE =
            RUN_JOB_RULES.rulesStartingWithInnerMost[0]

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + TableLists.MAF_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testNumberOfRowsInSSM() {
        long count = rowCounter.count Tables.SUBJ_SAMPLE_MAP,
                'trial_name = :study_id',
                study_id: STUDY_ID

        assert count == 11L
    }

    @Test
    void testNumberOfEvents() {
        def count = rowCounter.count Tables.MAF_MUTATION_EVENT, 'gpl_id = :gpl_id', gpl_id: PLATFORM

        assert count == 10L
    }

    @Test
    void testNumberOfDataEntries() {
        def count = rowCounter.count Tables.MAF_MUTATION

        assert count == 11L
    }
}
