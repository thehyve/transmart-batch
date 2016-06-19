package org.transmartproject.batch.highdim.platform

import com.google.common.collect.ImmutableSet
import org.transmartproject.batch.startup.ExternalJobParametersInternalInterface
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.InvalidParametersFileException

class AnnotationPlatformFileParametersModule implements ExternalJobParametersModule {
    public final static String ANNOTATIONS_FILE = 'ANNOTATIONS_FILE'

    Set<String> supportedParameters = ImmutableSet.of(ANNOTATIONS_FILE)

    void validate(ExternalJobParametersInternalInterface ejp) throws InvalidParametersFileException {
        mandatory ejp, ANNOTATIONS_FILE
    }

    void munge(ExternalJobParametersInternalInterface ejp) throws InvalidParametersFileException {
        ejp[ANNOTATIONS_FILE] = convertRelativePath ejp, ANNOTATIONS_FILE
    }
}
