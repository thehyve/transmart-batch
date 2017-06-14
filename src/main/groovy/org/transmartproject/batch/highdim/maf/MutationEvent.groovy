package org.transmartproject.batch.highdim.maf

/**
 * Represents a sample independent (reference) part of MAF data.
 */
class MutationEvent {

    Long mutationEventId
    String gplId

    /**
     * Standard fields
     */
    Integer entrezGeneId
    String chr
    Long startPosition
    Long endPosition
    String referenceAllele
    String tumorSeqAllele
    String proteinChange
    String mutationType

    String ncbiBuild
    String strand
    String variantType
    String dbSnpRs
    String dbSnpValStatus

    /**
     * Custom fields that have MA: prefix in MAF file
     */
    String functionalImpactScore
    Float fisValue
    String linkXvar
    String linkPdb
    String linkMsa

    /**
     * Oncotator fields
     */
    String oncotatorDbsnpRs
    String oncotatorRefseqMrnaId
    String oncotatorCodonChange
    String oncotatorUniprotEntryName
    String oncotatorUniprotAccession
    Long oncotatorProteinPosStart
    Long oncotatorProteinPosEnd

    List getUniqueKey() {
        [ncbiBuild, chr, startPosition, endPosition, tumorSeqAllele, entrezGeneId, proteinChange, mutationType]
    }

}