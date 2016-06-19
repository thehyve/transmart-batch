package org.transmartproject.batch.highdim.platform

import com.google.common.collect.ImmutableSet
import org.springframework.context.i18n.LocaleContextHolder
import org.transmartproject.batch.startup.ExternalJobParametersInternalInterface
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.InvalidParametersFileException

class PlatformParametersModule implements ExternalJobParametersModule {
    public final static String PLATFORM = 'PLATFORM'
    public final static String TITLE = 'TITLE'
    public final static String ORGANISM = 'ORGANISM'
    public final static String MARKER_TYPE = 'MARKER_TYPE'
    public final static String GENOME_RELEASE = 'GENOME_RELEASE'

    private final static String DEFAULT_ORGANISM = 'Homo Sapiens'

    private final String markerType

    public PlatformParametersModule(String markerType) {
        this.markerType = markerType
    }

    Set<String> supportedParameters = ImmutableSet.of(
            PLATFORM,
            TITLE,
            ORGANISM,
            MARKER_TYPE,
            GENOME_RELEASE,)

    void validate(ExternalJobParametersInternalInterface ejp)
            throws InvalidParametersFileException {
        mandatory ejp, PLATFORM
        mandatory ejp, TITLE
    }

    void munge(ExternalJobParametersInternalInterface ejp)
            throws InvalidParametersFileException {
        ejp[ORGANISM] = ejp[ORGANISM] ?: DEFAULT_ORGANISM

        ejp[MARKER_TYPE] = markerType

        ejp[PLATFORM] = ejp[PLATFORM]?.toUpperCase(LocaleContextHolder.locale)
    }
}
