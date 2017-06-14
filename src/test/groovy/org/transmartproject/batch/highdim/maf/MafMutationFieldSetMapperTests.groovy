package org.transmartproject.batch.highdim.maf

import org.junit.Before
import org.junit.Test
import org.springframework.batch.item.file.transform.DefaultFieldSet
import org.springframework.batch.item.file.transform.FieldSet

/**
 * Tests {@link org.transmartproject.batch.highdim.maf.MafMutationFieldSetMapper}
 */
class MafMutationFieldSetMapperTests {

    MafMutationFieldSetMapper testee

    LinkedHashMap<String, String> defaultMafRow = [
            Hugo_Symbol                     : 'POU4F1',
            Entrez_Gene_Id                  : '5457',
            Center                          : 'genome.wustl.edu',
            NCBI_Build                      : 'GRCh37',
            Chromosome                      : '13',
            Start_Position                  : '79175724',
            End_Position                    : '79175725',
            Strand                          : '-1',
            Variant_Classification          : 'Missense_Mutation',
            Variant_Type                    : 'SNP',
            Reference_Allele                : 'TTC',
            Tumor_Seq_Allele1               : 'test 4',
            Tumor_Seq_Allele2               : 'test 5',
            dbSNP_RS                        : 'test 1',
            dbSNP_Val_Status                : 'test 2',
            Tumor_Sample_Barcode            : 'TCGA-AC-A3QP-01',
            Matched_Norm_Sample_Barcode     : 'TCGA-AC-A3QP-10',
            Match_Norm_Seq_Allele1          : 'test 6',
            Match_Norm_Seq_Allele2          : 'test 7',
            Tumor_Validation_Allele1        : 'test 3',
            Tumor_Validation_Allele2        : 'test 8',
            Match_Norm_Validation_Allele1   : 'test 9',
            Match_Norm_Validation_Allele2   : 'test 10',
            Verification_Status             : 'Unknown',
            Validation_Status               : 'Untested',
            Mutation_Status                 : 'Somatic',
            Sequencing_Phase                : 'Phase_IV',
            Sequence_Source                 : 'WXS',
            Validation_Method               : 'none',
            Score                           : 'test 11',
            BAM_File                        : 'dbGAP',
            Sequencer                       : 'Illumina GAIIx',

            HGVSp_Short                     : 'p.P243P',
            Amino_Acid_Change               : 'p.S338L',
            ONCOTATOR_VARIANT_CLASSIFICATION: 'test 12',

            'MA:FImpact'                    : 'high',
            'MA:FIS'                        : '3.62',
            'MA:link.var'                   : 'getma.org/?cm=var&var=hg19,10,93104,C,T&fts=all',
            'MA:link.PDB'                   : 'getma.org/pdb.php?prot=YI016_HUMAN&from=312&to=372&var=E338K',
            'MA:link.MSA'                   : 'getma.org/?cm=msa&ty=f&p=YI016_HUMAN&rb=312&re=372&var=E338K',
            'MA:protein.change'             : 'E338K',

            ONCOTATOR_REFSEQ_MRNA_ID        : 'test 13',
            RefSeq                          : 'test 21',
            Codons                          : 'Gag/Aag',
            ONCOTATOR_UNIPROT_ENTRY_NAME    : 'test 14',
            ONCOTATOR_UNIPROT_ACCESSION     : 'test 15',
            ONCOTATOR_DBSNP_RS              : 'test 20',
            Protein_position                : '243/444',
            ProteinCange                    : 'test 15',

            t_alt_count                     : '16',
            t_ref_count                     : '17',
            n_alt_count                     : '18',
            n_ref_count                     : '19',
    ]

    Set<String> optionalColumns = [
            'HGVSp_Short',
            'Amino_Acid_Change',
            'ONCOTATOR_VARIANT_CLASSIFICATION',

            'MA:FImpact',
            'MA:FIS',
            'MA:link.var',
            'MA:link.PDB',
            'MA:link.MSA',
            'MA:protein.change',

            'ONCOTATOR_REFSEQ_MRNA_ID',
            'RefSeq',
            'Codons',
            'ONCOTATOR_UNIPROT_ENTRY_NAME',
            'ONCOTATOR_UNIPROT_ACCESSION',
            'ONCOTATOR_DBSNP_RS',
            'Protein_position',
            'ProteinCange',

            't_alt_count',
            't_ref_count',
            'n_alt_count',
            'n_ref_count',
    ] as Set

    Set<String> mandatoryColumns = defaultMafRow.keySet() - optionalColumns

    static mafRowToFieldSet(Map<String, String> mafRow) {
        new DefaultFieldSet(
                mafRow.values() as String[],
                mafRow.keySet() as String[],
        )
    }

    @Before
    void setUp() {
        testee = new MafMutationFieldSetMapper()
    }

    @Test
    void testBasicParsing() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow)
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert mutation

        assert defaultMafRow.Tumor_Sample_Barcode == mutation.tumorSampleBarcode
        assert defaultMafRow.Center == mutation.center
        assert defaultMafRow.Sequencer == mutation.sequencer
        assert defaultMafRow.Mutation_Status == mutation.mutationStatus
        assert defaultMafRow.Tumor_Seq_Allele1 == mutation.tumorSeqAllele1
        assert defaultMafRow.Tumor_Seq_Allele2 == mutation.tumorSeqAllele2
        assert defaultMafRow.Matched_Norm_Sample_Barcode == mutation.matchedNormSampleBarcode
        assert defaultMafRow.Match_Norm_Seq_Allele1 == mutation.matchNormSeqAllele1
        assert defaultMafRow.Match_Norm_Seq_Allele2 == mutation.matchNormSeqAllele2
        assert defaultMafRow.Tumor_Validation_Allele1 == mutation.tumorValidationAllele1
        assert defaultMafRow.Tumor_Validation_Allele2 == mutation.tumorValidationAllele2
        assert defaultMafRow.Match_Norm_Validation_Allele1 == mutation.matchNormValidationAllele1
        assert defaultMafRow.Match_Norm_Validation_Allele2 == mutation.matchNormValidationAllele2
        assert defaultMafRow.Verification_Status == mutation.verificationStatus
        assert defaultMafRow.Sequencing_Phase == mutation.sequencingPhase
        assert defaultMafRow.Sequence_Source == mutation.sequenceSource
        assert defaultMafRow.Validation_Method == mutation.validationMethod
        assert defaultMafRow.Validation_Status == mutation.validationStatus
        assert defaultMafRow.Score == mutation.score
        assert defaultMafRow.BAM_File == mutation.bamFile
        assert defaultMafRow.t_alt_count as Long == mutation.tumorAltCount
        assert defaultMafRow.t_ref_count as Long == mutation.tumorRefCount
        assert defaultMafRow.n_alt_count as Long == mutation.normalAltCount
        assert defaultMafRow.n_ref_count as Long == mutation.normalRefCount
        assert defaultMafRow.Amino_Acid_Change == mutation.aminoAcidChange

        assert mutation.event

        assert defaultMafRow.Entrez_Gene_Id as Integer == mutation.event.entrezGeneId
        assert defaultMafRow.Chromosome == mutation.event.chr
        assert defaultMafRow.Start_Position as Long == mutation.event.startPosition
        assert defaultMafRow.End_Position as Long == mutation.event.endPosition
        assert defaultMafRow.Reference_Allele == mutation.event.referenceAllele

        assert defaultMafRow.Tumor_Seq_Allele1 == mutation.event.tumorSeqAllele
        assert defaultMafRow.HGVSp_Short - 'p.' == mutation.event.proteinChange
        assert defaultMafRow.Variant_Classification == mutation.event.mutationType

        assert defaultMafRow['MA:FImpact'] == mutation.event.functionalImpactScore
        assert defaultMafRow['MA:FIS'] as Float == mutation.event.fisValue
        assert defaultMafRow['MA:link.var'] == mutation.event.linkXvar
        assert defaultMafRow['MA:link.PDB'] == mutation.event.linkPdb
        assert defaultMafRow['MA:link.MSA'] == mutation.event.linkMsa

        assert defaultMafRow.NCBI_Build == mutation.event.ncbiBuild
        assert defaultMafRow.Strand == mutation.event.strand
        assert defaultMafRow.Variant_Type == mutation.event.variantType
        assert defaultMafRow.dbSNP_RS == mutation.event.dbSnpRs
        assert defaultMafRow.dbSNP_Val_Status == mutation.event.dbSnpValStatus
        assert defaultMafRow.ONCOTATOR_DBSNP_RS == mutation.event.oncotatorDbsnpRs
        //It looks like cBio takes this field from the RefSeq column in the MAF file
        assert defaultMafRow.RefSeq == mutation.event.oncotatorRefseqMrnaId
        assert defaultMafRow.Codons == mutation.event.oncotatorCodonChange
        assert defaultMafRow.ONCOTATOR_UNIPROT_ENTRY_NAME == mutation.event.oncotatorUniprotEntryName
        assert defaultMafRow.ONCOTATOR_UNIPROT_ACCESSION == mutation.event.oncotatorUniprotAccession
        assert defaultMafRow.Protein_position.split('/')[0] as Long == mutation.event.oncotatorProteinPosStart
        assert defaultMafRow.Protein_position.split('/')[1] as Long == mutation.event.oncotatorProteinPosEnd
    }

    @Test
    void testParseWithougOptionalFields() {
        FieldSet fieldSet = mafRowToFieldSet(mandatoryColumns.collectEntries { [it, it.hashCode()] })
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert mutation
    }

    @Test
    void testDifferentTumorSeqAlleleSelected() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow + [
                Reference_Allele : 'allele1',
                Tumor_Seq_Allele1: 'allele1',
                Tumor_Seq_Allele2: 'allele2',
        ])
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert 'allele2' == mutation.event.tumorSeqAllele
    }

    @Test
    void testProteinChangeCalculation() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow + [
                HGVSp_Short      : '',
                Amino_Acid_Change: 'p.test',
        ])
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert 'test' == mutation.event.proteinChange
    }

    @Test
    void testDefaultProteinChange() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow + [
                HGVSp_Short      : '',
                Amino_Acid_Change: '',
        ])
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert 'MUTATED' == mutation.event.proteinChange
    }

    @Test
    void testSingleProteinPostion() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow + [
                Protein_position          : '100',
        ])
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert 100L == mutation.event.oncotatorProteinPosStart
        assert 100L == mutation.event.oncotatorProteinPosEnd
    }

    @Test
    void testMutationTypeCalculation() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow + [
                Variant_Classification          : '',
                ONCOTATOR_VARIANT_CLASSIFICATION: 'test',
        ])
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert 'test' == mutation.event.mutationType
    }

    @Test
    void testNasNullified() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow.keySet().collectEntries { [it, 'NA'] })
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert mutation
        def nonNullMutationProperties = mutation.properties.findAll { it.value != null }
        assert nonNullMutationProperties.size() == 2
        assert 'class' in nonNullMutationProperties
        assert 'event' in nonNullMutationProperties

        def nonNullMutationEventProperties = mutation.event.properties.findAll { it.value != null }
        assert nonNullMutationEventProperties.size() == 2
        assert 'class' in nonNullMutationEventProperties
        assert 'MUTATED' == nonNullMutationEventProperties.proteinChange
    }

    @Test
    void testNotAvailableLabelNullified() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow.keySet().collectEntries { [it, '[Not Available]'] })
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert mutation
        def nonNullMutationProperties = mutation.properties.findAll { it.value != null }
        assert nonNullMutationProperties.size() == 2
        assert 'class' in nonNullMutationProperties
        assert 'event' in nonNullMutationProperties

        def nonNullMutationEventProperties = mutation.event.properties.findAll { it.value != null }
        assert nonNullMutationEventProperties.size() == 2
        assert 'class' in nonNullMutationEventProperties
        assert 'MUTATED' == nonNullMutationEventProperties.proteinChange
    }

    @Test
    void testBlankValuesNullified() {
        FieldSet fieldSet = mafRowToFieldSet(defaultMafRow.keySet().collectEntries { [it, ' '] })
        Mutation mutation = testee.mapFieldSet(fieldSet)

        assert mutation
        def nonNullMutationProperties = mutation.properties.findAll { it.value != null }
        assert nonNullMutationProperties.size() == 2
        assert 'class' in nonNullMutationProperties
        assert 'event' in nonNullMutationProperties

        def nonNullMutationEventProperties = mutation.event.properties.findAll { it.value != null }
        assert nonNullMutationEventProperties.size() == 2
        assert 'class' in nonNullMutationEventProperties
        assert 'MUTATED' == nonNullMutationEventProperties.proteinChange
    }

}
