package org.transmartproject.batch.clinical

import com.google.common.collect.ImmutableSet
import org.transmartproject.batch.startup.*

/**
 * Parameter and configuration class specification for clinical jobs.
 */
final class ClinicalJobSpecification implements
        JobSpecification, ExternalJobParametersModule {

    public final static String COLUMN_MAP_FILE = 'COLUMN_MAP_FILE'
    public final static String WORD_MAP_FILE = 'WORD_MAP_FILE'
    public final static String RECORD_EXCLUSION_FILE = 'RECORD_EXCLUSION_FILE'
    public final static String XTRIAL_FILE = 'XTRIAL_FILE'
    public final static String TAGS_FILE = 'TAGS_FILE'
    public static final String EMPTY_VALUE = 'x'

    List<? extends ExternalJobParametersModule> jobParametersModules = [
            new StudyJobParametersModule(),
            this,
    ]

    final Class<?> jobPath = ClinicalDataLoadJobConfiguration

    final Set<String> supportedParameters = ImmutableSet.of(
            COLUMN_MAP_FILE,
            WORD_MAP_FILE,
            RECORD_EXCLUSION_FILE,
            XTRIAL_FILE,
            TAGS_FILE)

    void validate(ExternalJobParametersInternalInterface ejp)
            throws InvalidParametersFileException {
        ejp.mandatory COLUMN_MAP_FILE

        def incorectEmptyParameters = [WORD_MAP_FILE,
         RECORD_EXCLUSION_FILE,
         XTRIAL_FILE,
         TAGS_FILE].findAll { ejp[it]?.trim() == '' }

        if (incorectEmptyParameters) {
            throw new InvalidParametersFileException("Following parameters were provided without value:" +
                    " ${incorectEmptyParameters.join(',')}. Please use ${EMPTY_VALUE} as empty value or remove " +
                    "the parameters completely.")
        }
    }

    void munge(ExternalJobParametersInternalInterface ejp)
            throws InvalidParametersFileException {
        ejp[COLUMN_MAP_FILE] = ejp.convertRelativePath COLUMN_MAP_FILE

        [WORD_MAP_FILE,
         RECORD_EXCLUSION_FILE,
         XTRIAL_FILE,
         TAGS_FILE].each { p ->
            if (ejp[p] == EMPTY_VALUE) {
                ejp[p] = null
            } else if(ejp[p]?.trim() != '') {
                ejp[p] = ejp.convertRelativePath p
            }
        }
    }
}
