package org.transmartproject.batch.browsetag

import groovy.transform.EqualsAndHashCode
import groovy.transform.ToString

/**
 *
 */
@ToString
@EqualsAndHashCode(includes = ['type', 'value', 'name'])
class BrowseTagValue {

    BrowseTagType type
    String value
    String name
    String description

}
