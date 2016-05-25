package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.FlatFileHeaderCallback
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.transform.FieldExtractor
import org.springframework.core.io.Resource
import org.transmartproject.batch.batchartifacts.CsvLineAggregator

import javax.annotation.PostConstruct

/**
 *
 */
@Slf4j
class BrowseTagTypeFlatFileWriter implements ItemWriter<BrowseTagValue> {

    @Delegate
    FlatFileItemWriter<BrowseTagType> delegate

    CsvLineAggregator<Collection<String>> valuesAggregator = new CsvLineAggregator<>(
            lineEnd: '',
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
                lineAggregator: new CsvLineAggregator<BrowseTagType>(
                    separator: '\t',
                    lineEnd: '',
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
                ),
                headerCallback: { writer ->
                    writer.write(
                            ['node_type', 'title', 'solr_field_name', 'value_type', 'shown_if_empty', 'values', 'index']
                                    .join('\t'))
                } as FlatFileHeaderCallback
        )
        delegate.afterPropertiesSet()
    }

}
