package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 *
 */
@ToString
@EqualsAndHashCode(includes = ['id', 'name', 'fullName'])
class BrowseFolder {
    BrowseFolderType type
    Long id
    String name
    String fullName
}
