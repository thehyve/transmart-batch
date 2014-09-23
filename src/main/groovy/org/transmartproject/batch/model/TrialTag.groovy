package org.transmartproject.batch.model

import com.google.common.base.Function
import groovy.transform.ToString
import org.transmartproject.batch.support.LineListener
import org.transmartproject.batch.support.MappingHelper

/**
 *
 */
@ToString(includes = ['value','category','path'])
class TrialTag {
    String category
    String path
    String value

    private static fields = ['value','category','path']

    static List<TrialTag> parse(InputStream input, LineListener listener) {
        MappingHelper.parseObjects(input, LINE_MAPPER, listener)
    }

    static Function<String,TrialTag> LINE_MAPPER = new Function<String, TrialTag>() {
        @Override
        TrialTag apply(String input) {
            MappingHelper.parseObject(input, TrialTag.class, fields)
        }
    }

}
