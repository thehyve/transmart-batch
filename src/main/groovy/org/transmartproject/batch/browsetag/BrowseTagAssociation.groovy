package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 *
 */
@ToString
@EqualsAndHashCode(includes = ['folder', 'value'])
class BrowseTagAssociation {

    BrowseFolder folder
    BrowseTagValue value
    Integer index

}
