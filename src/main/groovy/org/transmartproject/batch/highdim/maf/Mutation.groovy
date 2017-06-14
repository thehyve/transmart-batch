package org.transmartproject.batch.highdim.maf

/**
 * Represents a sample bound part of MAF data.
 */
class Mutation {

    MutationEvent event
    Long assayId

    String center
    String sequencer
    String mutationStatus
    String validationStatus
    String tumorSeqAllele1
    String tumorSeqAllele2
    String tumorSampleBarcode
    String matchedNormSampleBarcode
    String matchNormSeqAllele1
    String matchNormSeqAllele2
    String tumorValidationAllele1
    String tumorValidationAllele2
    String matchNormValidationAllele1
    String matchNormValidationAllele2
    String verificationStatus
    String sequencingPhase
    String sequenceSource
    String validationMethod
    String score
    String bamFile

    Long tumorAltCount
    Long tumorRefCount
    Long normalAltCount
    Long normalRefCount

    String aminoAcidChange

}
