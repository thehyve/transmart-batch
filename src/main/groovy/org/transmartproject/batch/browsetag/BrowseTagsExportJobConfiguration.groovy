package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.step.tasklet.TaskletStep
import org.springframework.batch.item.file.FlatFileHeaderCallback
import org.springframework.batch.item.file.FlatFileItemWriter
import org.springframework.batch.item.file.transform.DelimitedLineAggregator
import org.springframework.batch.item.file.transform.FieldExtractor
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.io.FileSystemResource
import org.springframework.core.io.Resource
import org.transmartproject.batch.beans.AbstractJobConfiguration

/**
 * Spring configuration for the browse tag export job.
 */
@Slf4j
@Configuration
@ComponentScan(['org.transmartproject.batch.browsetag'])
class BrowseTagsExportJobConfiguration extends AbstractJobConfiguration {

    static final String JOB_NAME = 'BrowseTagsExportJob'
    static final int CHUNK_SIZE = 100

    @Bean(name = 'BrowseTagsExportJob')
    @Override
    Job job() {
        jobs.get(JOB_NAME)
                .start(tagTypesExportStep())
                .next(tagsExportStep())
                .build()
    }

    @Bean
    Step tagTypesExportStep() {
        TaskletStep s = steps.get('tagTypesExportStep')
                .chunk(CHUNK_SIZE)
                .reader(browseTagTypeReader())
                .writer(browseTagTypeWriter())
                .faultTolerant()
                .processorNonTransactional()
                .retryLimit(0) // do not retry individual items
                .listener(logCountsStepListener())
                .listener(lineOfErrorDetectionListener())
                .build()
        s
    }

    @Bean
    @StepScope
    BrowseTagTypeDatabaseReader browseTagTypeReader() {
        new BrowseTagTypeDatabaseReader()
    }

    @Bean
    @JobScope
    BrowseTagTypeFlatFileWriter browseTagTypeWriter() {
        Resource resource = browseTagTypesFileResource(null)
        new BrowseTagTypeFlatFileWriter(resource)
    }

    @Bean
    @JobScope
    FileSystemResource browseTagTypesFileResource(@Value("#{jobParameters}") Map<String, Object> jobParameters) {
        new FileSystemResource(jobParameters[BrowseTagsExportJobSpecification.EXPORT_BROWSE_TAG_TYPES_FILE])
    }

    @Bean
    Step tagsExportStep() {
        TaskletStep s = steps.get('tagsExportStep')
                .chunk(CHUNK_SIZE)
                .reader(browseTagsReader())
                .writer(browseTagsWriter())
                .faultTolerant()
                .processorNonTransactional()
                .retryLimit(0) // do not retry individual items
                .listener(logCountsStepListener())
                .listener(lineOfErrorDetectionListener())
                .build()
        s
    }

    @Bean
    @JobScope
    BrowseTagAssociationDatabaseReader browseTagsReader() {
        new BrowseTagAssociationDatabaseReader()
    }

    @Bean
    @JobScope
    FlatFileItemWriter browseTagsWriter() {
        Resource resource = browseTagsFileResource(null)
        new FlatFileItemWriter(
                resource: resource,
                lineAggregator: new DelimitedLineAggregator<BrowseTagAssociation>(
                        delimiter: '\t',
                        fieldExtractor: { BrowseTagAssociation item ->
                            ['\\',
                             item.value.type.displayName,
                             item.value.description,
                             0
                            ] as Object[]
                        } as FieldExtractor,
                ),
                headerCallback: { writer ->
                    writer.write(['concept_key', 'tag_title', 'tag_description', 'index'].join('\t'))
                } as FlatFileHeaderCallback
        )
    }

    @Bean
    @JobScope
    FileSystemResource browseTagsFileResource(
            @Value("#{jobParameters}") Map<String, Object> jobParameters) {
        new FileSystemResource(
                jobParameters[BrowseTagsExportJobSpecification.EXPORT_BROWSE_TAGS_FILE])
    }

}
