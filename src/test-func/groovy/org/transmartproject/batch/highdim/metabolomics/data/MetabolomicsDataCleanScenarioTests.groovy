package org.transmartproject.batch.highdim.metabolomics.data

import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.RuleChain
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.TableTruncator
import org.transmartproject.batch.junit.JobRunningTestTrait
import org.transmartproject.batch.junit.RunJobRule
import org.transmartproject.batch.support.TableLists

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Test metabolomics data import in the simplest scenario (good data not
 * previously loaded)
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class MetabolomicsDataCleanScenarioTests implements JobRunningTestTrait {

    private final static String STUDY_ID = 'GSE37427'
    private final static String PLATFORM_ID = 'MET998'

    private final static long NUMBER_OF_ASSAYS = 10
    private final static long NUMBER_OF_METABOLITES = 8

    private final static double DELTA = 1e-14d

    @ClassRule
    public final static RuleChain RUN_JOB_RULES = new RuleChain([
            new RunJobRule(STUDY_ID, 'metabolomics'),
            new RunJobRule(PLATFORM_ID, 'metabolomics_annotation'),
            new RunJobRule(STUDY_ID, 'clinical'),
    ])

    // needed by the trait
    public final static TestRule RUN_JOB_RULE =
            RUN_JOB_RULES.rulesStartingWithInnerMost[0]

    @AfterClass
    static void cleanDatabase() {
        new AnnotationConfigApplicationContext(
                GenericFunctionalTestConfiguration).getBean(TableTruncator).
                truncate(TableLists.CLINICAL_TABLES + TableLists.METABOLOMICS_TABLES + 'ts_batch.batch_job_instance')
    }

    @Test
    void testNumberOfRowsInSSM() {
        def count = rowCounter.count Tables.SUBJ_SAMPLE_MAP,
                'trial_name = :study_id',
                study_id: STUDY_ID

        assertThat count, is(equalTo(NUMBER_OF_ASSAYS))
    }

    @Test
    void testNumberOfFacts() {
        def count = rowCounter.count Tables.METAB_DATA,
                'trial_name = :study_id',
                study_id: STUDY_ID

        assertThat count, is(equalTo(NUMBER_OF_ASSAYS * NUMBER_OF_METABOLITES))
    }

    @Test
    void testArbitraryFact() {
        def sampleCode = '9CN'
        def biochemicalName = 'xylitol'
        // expect
        def subjectId = 'GSM918960'

        def q = """
                SELECT
                    D.trial_name,
                    D.subject_id,
                    PD.sourcesystem_cd,
                    raw_intensity,
                    log_intensity,
                    zscore
                FROM ${Tables.METAB_DATA} D
                INNER JOIN ${Tables.METAB_ANNOTATION} A ON (D.metabolite_annotation_id = A.id)
                INNER JOIN ${Tables.SUBJ_SAMPLE_MAP} SSM ON (D.assay_id = SSM.assay_id)
                LEFT JOIN ${Tables.PATIENT_DIMENSION} PD ON (D.patient_id = PD.patient_num)
                WHERE SSM.sample_cd = :sampleCode AND A.biochemical_name = :biochemicalName"""

        def p = [
                sampleCode: sampleCode,
                biochemicalName: biochemicalName,]

        Map<String, Object> r = jdbcTemplate.queryForMap q, p

        double xylitolLog2Mean = 16.7066526621115
        double xylitolLog2StdDev = 2.0594359285213115
        double rawValue = 205043.3d
        double logValue = Math.log(205043.3d) / Math.log(2d)
        double zscore = (logValue - xylitolLog2Mean) / xylitolLog2StdDev

        assertThat r, allOf(
                hasEntry('trial_name', STUDY_ID),
                hasEntry('subject_id', subjectId),
                hasEntry('sourcesystem_cd', "$STUDY_ID:$subjectId" as String),
                hasEntry(is('raw_intensity'), closeTo(rawValue, DELTA)),
                hasEntry(is('log_intensity'), closeTo(logValue, DELTA)),
                hasEntry(is('zscore'), closeTo(zscore, DELTA)),
        )

    }

}
