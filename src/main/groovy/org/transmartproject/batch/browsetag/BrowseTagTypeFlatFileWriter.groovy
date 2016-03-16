package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.transform.DelimitedLineAggregator
import org.springframework.batch.item.file.transform.FieldExtractor
import org.springframework.core.io.Resource

import javax.annotation.PostConstruct

/**
 *
 */
@Slf4j
class BrowseTagTypeFlatFileWriter implements ItemWriter<BrowseTagValue> {

    @Delegate
    FlatFileItemWriter<BrowseTagType> delegate

    DelimitedLineAggregator<Collection<String>> valuesAggregator = new DelimitedLineAggregator<>(
        fieldExtractor: { Collection<String> s -> s as Object[] } as FieldExtractor
    )

    private final Resource resource

    BrowseTagTypeFlatFileWriter(Resource resource) {
        this.resource = resource
    }

    @PostConstruct
    void init() {
        delegate = new FlatFileItemWriter(
                resource: resource,
                lineAggregator: new DelimitedLineAggregator<BrowseTagType>(
                    delimiter: '\t',
                    fieldExtractor: { BrowseTagType type ->
                        [   type.folderType.type,
                            type.displayName,
                            type.code.toLowerCase(),
                            'NON_ANALYZED_STRING',
                            'Y',
                            valuesAggregator.aggregate(type.values),
                            type.index
                        ] as Object[]
                    } as FieldExtractor
                )
        )
        delegate.afterPropertiesSet()
    }

}
