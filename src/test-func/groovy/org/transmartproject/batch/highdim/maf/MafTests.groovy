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
import org.transmartproject.batch.startup.RunJob
import org.transmartproject.batch.support.TableLists

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * test MAF data import
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class MafTests implements JobRunningTestTrait {

    private final static String PLATFORM = 'MAF_HG19'
    private final static String STUDY_ID = 'BRCA_TCGA'

    @ClassRule
    public final static TestRule RUN_JOB_RULES = new RuleChain([
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

    @Test
    void testFields() {
        def actual = queryForMap """
            select e.*, m.* from ${Tables.MAF_MUTATION} m
            inner join ${Tables.MAF_MUTATION_EVENT} e on e.mutation_event_id = m.mutation_event_id
            inner join ${Tables.SUBJ_SAMPLE_MAP} ssm on ssm.assay_id = m.assay_id
            where ssm.sample_cd = :sample_cd
            """,
                [sample_cd: 'TCGA-AN-A0XN-01']

        assert actual.gpl_id == PLATFORM
        assert actual.entrez_gene_id == 7011
        assert actual.chr == '14'
        assert actual.start_position == 20846384
        assert actual.end_position == 20846385
        assert actual.reference_allele == 'TTC'
        assert actual.tumor_seq_allele == 'TTA'
        assert actual.protein_change == 'P243P'
        assert actual.mutation_type == 'Silent'
        assert actual.functional_impact_score == 'neutral'
        assert Math.abs(actual.fis_value - 0.41) < 0.001
        assert actual.link_xvar == 'link_var1'
        assert actual.link_pdb == 'link_pdb1'
        assert actual.link_msa == 'link_msa1'
        assert actual.ncbi_build == 'GRCh37'
        assert actual.strand == '-1'
        assert actual.variant_type == 'SNP'
        assert actual.db_snp_rs == 'dbSNP_RS1'
        assert actual.db_snp_val_status == 'dbSNP_Val_Status1'
        assert actual.oncotator_dbsnp_rs == 'o_dbsnp_rs1'
        assert actual.oncotator_refseq_mrna_id == 'o_mrna_id1'
        assert actual.oncotator_codon_change == 'o_codons1'
        assert actual.oncotator_uniprot_entry_name == 'o_uniprot_entry_name1'
        assert actual.oncotator_uniprot_accession == 'o_uniprot_accession1'
        assert actual.oncotator_protein_pos_start == 13
        assert actual.oncotator_protein_pos_end == 14

        assert actual.center == 'genome.wustl.edu'
        assert actual.sequencer == 'Illumina GAIIx'
        assert actual.mutation_status == 'Somatic'
        assert actual.validation_status == 'Untested'
        assert actual.tumor_seq_allele1 == 'TTC'
        assert actual.tumor_seq_allele2 == 'TTA'
        assert actual.matched_norm_sample_barcode == 'TCGA-AN-A0XN-10'
        assert actual.match_norm_seq_allele1 == 'TTW'
        assert actual.match_norm_seq_allele2 == 'TTZ'
        assert actual.tumor_validation_allele1 == 'TTB'
        assert actual.tumor_validation_allele2 == 'TTE'
        assert actual.match_norm_validation_allele1 == 'TTF'
        assert actual.match_norm_validation_allele2 == 'TTG'
        assert actual.verification_status == 'Unknown'
        assert actual.sequencing_phase == 'Phase_IV'
        assert actual.sequence_source == 'WXS'
        assert actual.validation_method == 'none'
        assert actual.score == '3'
        assert actual.bam_file == 'dbGAP'
        assert actual.tumor_alt_count == 5
        assert actual.tumor_ref_count == 6
        assert actual.normal_alt_count == 7
        assert actual.normal_ref_count == 8
        assert actual.amino_acid_change == 'amino1'
    }

    /*
    @Test
    void testReupload() {
        def params = ['-p', 'studies/' + STUDY_ID + '/maf.params', '-n']

        def runJob = RunJob.createInstance(*params)
        def intResult = runJob.run()
        assert intResult == 1

        def count = rowCounter.count Tables.MAF_MUTATION_EVENT, 'gpl_id = :gpl_id', gpl_id: PLATFORM
        assert count == 10L
    }
    */

}
