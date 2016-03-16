package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 * Represents a node type with which tags can be associated.
 * This is called a 'tag template' in the database.
 */
@ToString
@EqualsAndHashCode(includes = ['type', 'displayName'])
class BrowseFolderType implements Serializable {

    private static final long serialVersionUID = 1L

    String type
    String displayName

}
