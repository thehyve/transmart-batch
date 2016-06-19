package org.transmartproject.batch.highdim.vcf

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
 * test VCF data import in the simplest scenario
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class VcfCleanScenarioTests implements JobRunningTestTrait {

    private final static String STUDY_ID = 'CLUC'
    private final static String PLATFORM_ID = 'VCF_HG19'

    private final static long NUMBER_OF_ASSAYS = 6
    private final static long NUMBER_OF_VCF_ROWS = 51

    @ClassRule
    public final static TestRule RUN_JOB_RULES = new RuleChain([
            new RunJobRule(STUDY_ID, 'vcf', ['-n']),
            new RunJobRule(STUDY_ID, 'clinical', ['-n']),
    ])

    @Test
    void testPlatform() {
        def q = """
                SELECT platform, title, organism, marker_type, genome_build
                FROM ${Tables.GPL_INFO}
                WHERE platform = :platform"""
        def p = [platform: PLATFORM_ID]

        Map<String, Object> r = queryForMap q, p
        assertThat r, allOf(
                hasEntry('title', 'VCF platform for hg19'),
                hasEntry('organism', 'Homo Sapiens'),
                hasEntry('marker_type', 'VCF'),
                hasEntry('genome_build', 'hg19'),
        )
    }

    @Test
    void testDataSet() {
        def q = """
                SELECT dataset_id, genome
                FROM ${Tables.VCF_DATASET}
                WHERE gpl_id = :gpl_id"""
        def p = [gpl_id: PLATFORM_ID]

        Map<String, Object> r = queryForMap q, p
        assertThat r, allOf(
                //Hex digits representation of UUID contains 4 slashes
                hasEntry('dataset_id', contains('-', '-', '-', '-')),
                hasEntry('genome', 'hg19'),
        )
    }

    @Test
    void testNumberOfRowsInSSM() {
        def count = rowCounter.count Tables.SUBJ_SAMPLE_MAP,
                'trial_name = :study_id',
                study_id: STUDY_ID

        assertThat count, is(equalTo(NUMBER_OF_ASSAYS))
    }

    @Test
    void testArbitrarySample() {
        def sampleCode = 'VCaP_SNP_hg19_Sample1'
        def subjectId = 'VCaP'

        def q = """
                SELECT
                    PD.sourcesystem_cd as pd_sourcesystem_cd,
                    CD.concept_path as cd_concept_path,
                    assay_id,
                    sample_type,
                    trial_name,
                    tissue_type,
                    timepoint,
                    gpl_id,
                    sample_cd
                FROM ${Tables.SUBJ_SAMPLE_MAP} SSM
                LEFT JOIN ${Tables.PATIENT_DIMENSION} PD ON (SSM.patient_id = PD.patient_num)
                LEFT JOIN ${Tables.CONCEPT_DIMENSION} CD ON (SSM.concept_code = CD.concept_cd)
                WHERE subject_id = :subjectId"""

        Map<String, Object> r = queryForMap q, [subjectId: subjectId]

        assertThat r, allOf(
                hasEntry('pd_sourcesystem_cd', "$STUDY_ID:$subjectId" as String),
                hasEntry('cd_concept_path', '\\Public Studies\\CLUC\\Test\\VCF\\data\\'),
                hasEntry(is('assay_id'), isA(Number)),
                hasEntry('sample_type', 'st'),
                hasEntry('trial_name', STUDY_ID),
                hasEntry('tissue_type', 'tissue'),
                hasEntry('timepoint', 'tp1'),
                hasEntry('gpl_id', PLATFORM_ID),
                hasEntry('sample_cd', sampleCode),
        )
    }

    @Test
    void testNumberOfFacts() {
        def count = rowCounter.count Tables.VCF_DATA

        assertThat count,
                is(equalTo(NUMBER_OF_ASSAYS * NUMBER_OF_VCF_ROWS))
    }

    @Test
    void testArbitraryVcfRow() {
        def chr = '3'
        def pos = 5258280
        def sampleCode = 'VCaP_SNP_hg19_Sample1'

        def q = """
                SELECT
                    VCF.*
                FROM ${Tables.VCF_DATA} VCF
                JOIN ${Tables.SUBJ_SAMPLE_MAP} SSM ON SSM.assay_id = VCF.assay_id
                WHERE SSM.sample_cd = :sample_cd and VCF.chr = chr and VCF.pos = :pos"""

        Map<String, Object> r = queryForMap q, [sample_cd: sampleCode, chr: chr, pos: pos]

        assertThat r, allOf(
                hasEntry(is('variant_subject_summary_id'), notNullValue()),
                //Hex digits representation of UUID contains 4 slashes
//                hasEntry(is('dataset_id'), contains('-', '-', '-', '-')),
//                hasEntry('subject_id', 'VCaP'),
//                hasEntry('rs_id', '.'),

//                hasEntry('variant', 'A/G'), //Is it REF/ALT ? No! What is that then?
//                hasEntry('variant_format', 'R/V'), //Where do we get this value from?
                //single nucleotide, or multiple nucleotide polymorphism
                hasEntry('variant_type', 'SNP'),
                //reference=f?
                hasEntry('allele1', 0),
                hasEntry('allele2', 1),
                hasEntry('ref', 'A'),
                hasEntry('alt', 'G'),

//                hasEntry('gual', nullValue()),
//                hasEntry('filter', 'PASS'),
//                hasEntry('info', 'AC=1;ADP=44;AN=2;GID=ENSG00000134109.6;GS=EDEM1;HET=1;HOM=0;NC=0;SF=5;WT=0'),
//                hasEntry('format', 'GT:ADR:RD:ABQ:ADF:RBQ:AD:RDR:GQ:PVAL:DP:SDP:FREQ:RDF'),
//                hasEntry('variant_value', '0/1:6:31:36:7:37:13:26:43:4.324E-5:44:44:29.55%:5'),

//                hasEntry('gene_name', 'EDEM1'),
//                hasEntry('gene_id', 'ENSG00000134109.6'),

        )
    }

    // needed by the trait
    public final static TestRule RUN_JOB_RULE =
            RUN_JOB_RULES.rulesStartingWithInnerMost[0]

    @AfterClass
    static void cleanDatabase() {
        PersistentContext.truncator.
                truncate(TableLists.CLINICAL_TABLES + TableLists.VCF_TABLES + 'ts_batch.batch_job_instance')
    }

}
