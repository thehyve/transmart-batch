package org.transmartproject.batch.tag

import groovy.util.logging.Slf4j
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.item.validator.ValidationException
import org.springframework.batch.item.validator.Validator
import org.springframework.stereotype.Component

import javax.annotation.Resource

/**
 * Given a {@link Tag}, throws if the concept it refers to doesn't exist.
 * This is a Spring BATCH validator.
 */
@Component
@JobScope
@Slf4j
class TagOptionValidator implements Validator<Tag> {

    @Resource
    TagTypeOptionsMap tagTypeOptionsMap

    @Override
    void validate(Tag tag) throws ValidationException {

        def options = tagTypeOptionsMap.getOptionsForTagType(tag.tagTitle)
        if (options != null) { // the tag is configured in the tag types table.
            // set the tag option id
            tag.tagOptionId = options[tag.tagDescription]
            if (tag.tagOptionId == null) {
                def message = "Tag description not in the set of allowed descriptions for tag '${tag.tagTitle}':" +
                        " ${tag.tagDescription}"
                log.warn message
                throw new InvalidTagDescriptionException(message)
            }
            // clear the tag description field
            tag.tagDescription = null
        }
    }
}
