package org.transmartproject.batch.highdim.maf

import groovy.util.logging.Slf4j
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.simple.SimpleJdbcInsert
import org.springframework.stereotype.Component
import org.transmartproject.batch.clinical.db.objects.Sequences
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.SequenceReserver
import org.transmartproject.batch.highdim.platform.Platform
import org.transmartproject.batch.highdim.platform.annotationsload.AnnotationEntityMap

/**
 * Writes the maf data to the database table.
 */

@Component
@JobScope
@Slf4j
class MafDataWriter implements ItemWriter<Mutation> {

    @Autowired
    private JdbcTemplate jdbcTemplate

    @Value(Tables.MAF_MUTATION_EVENT)
    SimpleJdbcInsert eventJdbcInsert

    @Value(Tables.MAF_MUTATION)
    SimpleJdbcInsert mutationJdbcInsert

    @Autowired
    Platform platform

    @Autowired
    private SequenceReserver sequenceReserver

    @Value("#{jobExecutionContext['sampleCodeAssayIdMap']}")
    Map<String, Long> sampleCodeAssayIdMap

    @Autowired
    MafMutationEventSet mafMutationEventSet

    @Override
    void write(List<? extends Mutation> items) throws Exception {
        items.each { mutation ->
            def event = mafMutationEventSet[mutation.event.uniqueKey]
            if (!event) {
                mutation.event.mutationEventId = sequenceReserver.getNext(Sequences.MAF_EVENT_ID)
                mutation.event.gplId = platform.id

                eventJdbcInsert.execute(objectToDbRow(mutation.event))
                mafMutationEventSet << mutation.event
            } else {
                mutation.event = event
            }
            mutation.assayId = sampleCodeAssayIdMap[mutation.tumorSampleBarcode]
            mutationJdbcInsert.execute(objectToDbRow(mutation))
        }
    }

    private static Map<String, Object> objectToDbRow(MutationEvent mutationEvent) {
        mutationEvent.with {
            [
                    mutation_event_id           : mutationEventId,
                    gpl_id                      : gplId,
                    entrez_gene_id              : entrezGeneId,
                    chr                         : chr,
                    start_position              : startPosition,
                    end_position                : endPosition,
                    reference_allele            : referenceAllele,
                    tumor_seq_allele            : tumorSeqAllele,
                    protein_change              : proteinChange,
                    mutation_type               : mutationType,
                    functional_impact_score     : functionalImpactScore,
                    fis_value                   : fisValue,
                    link_xvar                   : linkXvar,
                    link_pdb                    : linkPdb,
                    link_msa                    : linkMsa,
                    ncbi_build                  : ncbiBuild,
                    strand                      : strand,
                    variant_type                : variantType,
                    db_snp_rs                   : dbSnpRs,
                    db_snp_val_status           : dbSnpValStatus,
                    oncotator_dbsnp_rs          : oncotatorDbsnpRs,
                    oncotator_refseq_mrna_id    : oncotatorRefseqMrnaId,
                    oncotator_codon_change      : oncotatorCodonChange,
                    oncotator_uniprot_entry_name: oncotatorUniprotEntryName,
                    oncotator_uniprot_accession : oncotatorUniprotAccession,
                    oncotator_protein_pos_start : oncotatorProteinPosStart,
                    oncotator_protein_pos_end   : oncotatorProteinPosEnd,
            ]
        }

    }

    private static Map<String, Object> objectToDbRow(Mutation mutation) {
        mutation.with {
            [
                    mutation_event_id            : mutation.event.mutationEventId,
                    assay_id                     : assayId,
                    center                       : center,
                    sequencer                    : sequencer,
                    mutation_status              : mutationStatus,
                    validation_status            : validationStatus,
                    tumor_seq_allele1            : tumorSeqAllele1,
                    tumor_seq_allele2            : tumorSeqAllele2,
                    tumor_sample_barcode         : tumorSampleBarcode,
                    matched_norm_sample_barcode  : matchedNormSampleBarcode,
                    match_norm_seq_allele1       : matchNormSeqAllele1,
                    match_norm_seq_allele2       : matchNormSeqAllele2,
                    tumor_validation_allele1     : tumorValidationAllele1,
                    tumor_validation_allele2     : tumorValidationAllele2,
                    match_norm_validation_allele1: matchNormValidationAllele1,
                    match_norm_validation_allele2: matchNormValidationAllele2,
                    verification_status          : verificationStatus,
                    sequencing_phase             : sequencingPhase,
                    sequence_source              : sequenceSource,
                    validation_method            : validationMethod,
                    score                        : score,
                    bam_file                     : bamFile,

                    tumor_alt_count              : tumorAltCount,
                    tumor_ref_count              : tumorRefCount,
                    normal_alt_count             : normalAltCount,
                    normal_ref_count             : normalRefCount,

                    amino_acid_change            : aminoAcidChange,

            ]
        }

    }

}
