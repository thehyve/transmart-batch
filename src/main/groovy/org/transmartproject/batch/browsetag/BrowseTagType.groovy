package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * Represents a tag type, used in the `Browse` tab.
 * This is called a 'tag item' in the database.
 * Tags can be associated with a folder in the hierarchy. Which tag types
 * are available for a certain folder type depends on the <var>folderType</var>
 * of the tag types.
 */
@ToString
@EqualsAndHashCode(includes = ['id', 'code', 'type', 'subType', 'folderType'])
class BrowseTagType implements Serializable {

    private static final long serialVersionUID = 1L

    Long id

    String code

    String displayName

    BrowseFolderType folderType

    Boolean required

    /**
     * E.g., 'FIXED', 'CUSTOM'.
     * Also, 'PROGRAM_TARGET', 'BIO_ASSAY_PLATFORM', 'BIO_DISEASE', 'BIO_MARKER'.
     * 'BIO_ASSAY_ANALYSIS', 'BIO_CONCEPT_CODE', 'BIO_EXPERIMENT', 'BIO_MARKER.GENE', 'BIO_ASSAY_PLATFORM'?
     * <code>select distinct(bio_data_type) from biomart.bio_data_uid;</code>
     */
    String type

    /**
     * E.g., 'FREETEXT', 'FREETEXTAREA', 'PICKLIST', 'MULTIPICKLIST'
     */
    String subType

    /**
     * These reside in BIOMART.BIO_CONCEPT_CODE mainly...
     */
    Collection<String> values

    Integer index

}
