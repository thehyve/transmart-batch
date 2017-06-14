package org.transmartproject.batch.highdim.maf

import org.springframework.batch.item.file.mapping.FieldSetMapper
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.validation.BindException

/**
 * Instantiate {@see Mutation} and {@see MutationEvent} out of field set read from MAF row.
 * Following logic of cBioPortal
 * https://github.com/cBioPortal/cbioportal/blob/master/core/src/main/java/org/mskcc/cbio/portal/scripts/ImportExtendedMutationData.java
 * https://wiki.nci.nih.gov/display/TCGA/Mutation+Annotation+Format+%28MAF%29+Specification
 */
class MafMutationFieldSetMapper implements FieldSetMapper<Mutation> {

    public static final String PROTEIN_CHANGE_PREFIX = 'p.'
    public static final String DEFAULT_PROTEIN_CHANGE = 'MUTATED'
    public static final Set<String> NA_VARIATIONS = ['NA', '[Not Available]'] as Set

    @Override
    Mutation mapFieldSet(FieldSet fieldSet) throws BindException {
        Mutation mutation = new Mutation()

        Set<String> names = fieldSet.names as Set
        MutationEvent mutationEvent = new MutationEvent()

        String entrezGeneIdString = nullifyNoValues(fieldSet.readString('Entrez_Gene_Id'))
        if (entrezGeneIdString) {
            mutationEvent.entrezGeneId = entrezGeneIdString as Integer
        }
        mutationEvent.chr = nullifyNoValues(fieldSet.readString('Chromosome'))
        String startPositionString = nullifyNoValues(fieldSet.readString('Start_Position'))
        if (startPositionString) {
            mutationEvent.startPosition = startPositionString as Long
        }
        String endPositionString = nullifyNoValues(fieldSet.readString('End_Position'))
        if (endPositionString) {
            mutationEvent.endPosition = endPositionString as Long
        }
        mutationEvent.referenceAllele = nullifyNoValues(fieldSet.readString('Reference_Allele'))
        mutationEvent.tumorSeqAllele = nullifyNoValues(fieldSet.readString('Tumor_Seq_Allele1'))
        if (mutationEvent.tumorSeqAllele == mutationEvent.referenceAllele) {
            mutationEvent.tumorSeqAllele = nullifyNoValues(fieldSet.readString('Tumor_Seq_Allele2'))
        }
        if ('HGVSp_Short' in names) {
            mutationEvent.proteinChange = nullifyNoValues(fieldSet.readString('HGVSp_Short'))
            if (!mutationEvent.proteinChange && 'Amino_Acid_Change' in names) {
                mutationEvent.proteinChange = nullifyNoValues(fieldSet.readString('Amino_Acid_Change'))
            }
            if (!mutationEvent.proteinChange) {
                mutationEvent.proteinChange = DEFAULT_PROTEIN_CHANGE
            }
            if (mutationEvent.proteinChange?.startsWith(PROTEIN_CHANGE_PREFIX)) {
                mutationEvent.proteinChange = mutationEvent.proteinChange.substring(PROTEIN_CHANGE_PREFIX.length())
            }
        }
        mutationEvent.mutationType = nullifyNoValues(fieldSet.readString('Variant_Classification'))
        if (!mutationEvent.mutationType && 'ONCOTATOR_VARIANT_CLASSIFICATION' in names) {
            mutationEvent.mutationType = nullifyNoValues(fieldSet.readString('ONCOTATOR_VARIANT_CLASSIFICATION'))
        }
        if ('MA:FImpact' in names) {
            mutationEvent.functionalImpactScore = nullifyNoValues(fieldSet.readString('MA:FImpact'))
        }
        if ('MA:FIS' in names) {
            String fisValueString = nullifyNoValues(fieldSet.readString('MA:FIS'))
            if (fisValueString) {
                mutationEvent.fisValue = fisValueString as Float
            }
        }
        if ('MA:link.var' in names) {
            mutationEvent.linkXvar = nullifyNoValues(fieldSet.readString('MA:link.var'))
        }
        if ('MA:link.PDB' in names) {
            mutationEvent.linkPdb = nullifyNoValues(fieldSet.readString('MA:link.PDB'))
        }
        if ('MA:link.MSA' in names) {
            mutationEvent.linkMsa = nullifyNoValues(fieldSet.readString('MA:link.MSA'))
        }
        mutationEvent.ncbiBuild = nullifyNoValues(fieldSet.readString('NCBI_Build'))
        mutationEvent.strand = nullifyNoValues(fieldSet.readString('Strand'))
        mutationEvent.variantType = nullifyNoValues(fieldSet.readString('Variant_Type'))
        mutationEvent.dbSnpRs = nullifyNoValues(fieldSet.readString('dbSNP_RS'))
        mutationEvent.dbSnpValStatus = nullifyNoValues(fieldSet.readString('dbSNP_Val_Status'))
        if ('ONCOTATOR_DBSNP_RS' in names) {
            mutationEvent.oncotatorDbsnpRs = nullifyNoValues(fieldSet.readString('ONCOTATOR_DBSNP_RS'))
        }
        if ('RefSeq' in names) {
            mutationEvent.oncotatorRefseqMrnaId = nullifyNoValues(fieldSet.readString('RefSeq'))
        }
        if ('Codons' in names) {
            mutationEvent.oncotatorCodonChange = nullifyNoValues(fieldSet.readString('Codons'))
        }
        if ('ONCOTATOR_UNIPROT_ENTRY_NAME' in names) {
            mutationEvent.oncotatorUniprotEntryName = nullifyNoValues(fieldSet.readString('ONCOTATOR_UNIPROT_ENTRY_NAME'))
        }
        if ('ONCOTATOR_UNIPROT_ACCESSION' in names) {
            mutationEvent.oncotatorUniprotAccession = nullifyNoValues(fieldSet.readString('ONCOTATOR_UNIPROT_ACCESSION'))
        }
        if ('Protein_position' in names) {
            String proteinPosition = nullifyNoValues(fieldSet.readString('Protein_position'))
            if (proteinPosition) {
                String[] proteinPositions = proteinPosition.split('/')
                mutationEvent.oncotatorProteinPosStart = proteinPositions[0] as Long
                if (proteinPositions.length > 1) {
                    mutationEvent.oncotatorProteinPosEnd = proteinPositions[1] as Long
                } else {
                    mutationEvent.oncotatorProteinPosEnd = mutationEvent.oncotatorProteinPosStart
                }
            }
        }

        mutation.event = mutationEvent

        mutation.tumorSampleBarcode = nullifyNoValues(fieldSet.readString('Tumor_Sample_Barcode'))
        mutation.center = nullifyNoValues(fieldSet.readString('Center'))
        mutation.sequencer = nullifyNoValues(fieldSet.readString('Sequencer'))
        mutation.mutationStatus = nullifyNoValues(fieldSet.readString('Mutation_Status'))
        mutation.tumorSeqAllele1 = nullifyNoValues(fieldSet.readString('Tumor_Seq_Allele1'))
        mutation.tumorSeqAllele2 = nullifyNoValues(fieldSet.readString('Tumor_Seq_Allele2'))
        mutation.matchedNormSampleBarcode = nullifyNoValues(fieldSet.readString('Matched_Norm_Sample_Barcode'))
        mutation.matchNormSeqAllele1 = nullifyNoValues(fieldSet.readString('Match_Norm_Seq_Allele1'))
        mutation.matchNormSeqAllele2 = nullifyNoValues(fieldSet.readString('Match_Norm_Seq_Allele2'))
        mutation.tumorValidationAllele1 = nullifyNoValues(fieldSet.readString('Tumor_Validation_Allele1'))
        mutation.tumorValidationAllele2 = nullifyNoValues(fieldSet.readString('Tumor_Validation_Allele2'))
        mutation.matchNormValidationAllele1 = nullifyNoValues(fieldSet.readString('Match_Norm_Validation_Allele1'))
        mutation.matchNormValidationAllele2 = nullifyNoValues(fieldSet.readString('Match_Norm_Validation_Allele2'))
        mutation.verificationStatus = nullifyNoValues(fieldSet.readString('Verification_Status'))
        mutation.sequencingPhase = nullifyNoValues(fieldSet.readString('Sequencing_Phase'))
        mutation.sequenceSource = nullifyNoValues(fieldSet.readString('Sequence_Source'))
        mutation.validationMethod = nullifyNoValues(fieldSet.readString('Validation_Method'))
        mutation.validationStatus = nullifyNoValues(fieldSet.readString('Validation_Status'))
        mutation.score = nullifyNoValues(fieldSet.readString('Score'))
        mutation.bamFile = nullifyNoValues(fieldSet.readString('BAM_File'))
        if ('t_alt_count' in names) {
            String tumorAltCountString = nullifyNoValues(fieldSet.readString('t_alt_count'))
            if (endPositionString) {
                mutation.tumorAltCount = tumorAltCountString as Long
            }
        }
        if ('t_ref_count' in names) {
            String tumorRefCountString = nullifyNoValues(fieldSet.readString('t_ref_count'))
            if (endPositionString) {
                mutation.tumorRefCount = tumorRefCountString as Long
            }
        }
        if ('n_alt_count' in names) {
            String normalAltCountString = nullifyNoValues(fieldSet.readString('n_alt_count'))
            if (endPositionString) {
                mutation.normalAltCount = normalAltCountString as Long
            }
        }
        if ('n_ref_count' in names) {
            String normalRefCountString = nullifyNoValues(fieldSet.readString('n_ref_count'))
            if (endPositionString) {
                mutation.normalRefCount = normalRefCountString as Long
            }
        }
        if ('Amino_Acid_Change' in names) {
            mutation.aminoAcidChange = nullifyNoValues(fieldSet.readString('Amino_Acid_Change'))
        }
        mutation.bamFile = nullifyNoValues(fieldSet.readString('BAM_File'))
        mutation
    }

    private static String nullifyNoValues(String value) {
        if (value in NA_VARIATIONS || !value?.trim()) {
            return null
        } else {
            return value
        }
    }
}
