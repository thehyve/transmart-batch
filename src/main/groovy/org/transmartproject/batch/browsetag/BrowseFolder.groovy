package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 *
 */
@ToString
@EqualsAndHashCode(includes = ['id', 'level', 'name', 'type', 'parent'])
class BrowseFolder {
    BrowseFolderType type
    Long id
    Integer level
    String name
    String fullName
    String description
    String parent
}
