package org.transmartproject.batch.browsetag

import com.google.common.collect.ImmutableSet
import groovy.transform.TypeChecked
import org.transmartproject.batch.startup.*

/**
 * Processes the exportbrowsetags.params files.
 */
@TypeChecked
final class BrowseTagsExportJobSpecification
        implements ExternalJobParametersModule, JobSpecification {

    public final static String EXPORT_BROWSE_TAG_TYPES_FILE = 'EXPORT_BROWSE_TAG_TYPES_FILE'
    public final static String EXPORT_BROWSE_TAGS_FILE = 'EXPORT_BROWSE_TAGS_FILE'

    final List<? extends ExternalJobParametersModule> jobParametersModules = [this]

    final Class jobPath = BrowseTagsExportJobConfiguration

    final Set<String> supportedParameters = ImmutableSet.of(
            EXPORT_BROWSE_TAGS_FILE,
            EXPORT_BROWSE_TAG_TYPES_FILE
    )

    void munge(ExternalJobParametersInternalInterface ejp)
            throws InvalidParametersFileException {
        if (ejp[EXPORT_BROWSE_TAGS_FILE] == 'x') {
            ejp[EXPORT_BROWSE_TAGS_FILE] == null
        }
        if (ejp[EXPORT_BROWSE_TAGS_FILE]) {
            ejp[EXPORT_BROWSE_TAGS_FILE] = ejp.convertRelativeWritePath EXPORT_BROWSE_TAGS_FILE
        }

        if (ejp[EXPORT_BROWSE_TAG_TYPES_FILE] == 'x') {
            ejp[EXPORT_BROWSE_TAG_TYPES_FILE] == null
        }
        if (ejp[EXPORT_BROWSE_TAG_TYPES_FILE]) {
            ejp[EXPORT_BROWSE_TAG_TYPES_FILE] = ejp.convertRelativeWritePath EXPORT_BROWSE_TAG_TYPES_FILE
        }
    }
}
