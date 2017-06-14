package org.transmartproject.batch.highdim.maf

import groovy.util.logging.Slf4j
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.core.step.tasklet.TaskletStep
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemStreamReader
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.springframework.core.io.ClassPathResource
import org.transmartproject.batch.batchartifacts.MultipleItemsLineItemReader
import org.transmartproject.batch.batchartifacts.PutInBeanWriter
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.beans.StepBuildingConfigurationTrait
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.DatabaseImplementationClassPicker
import org.transmartproject.batch.db.DbConfig
import org.transmartproject.batch.db.DeleteByColumnValueWriter
import org.transmartproject.batch.gwas.metadata.GwasMetadataEntry
import org.transmartproject.batch.highdim.assays.CurrentAssayIdsReader
import org.transmartproject.batch.highdim.beans.AbstractTypicalHdDataStepsConfig
import org.transmartproject.batch.highdim.cnv.data.CnvDataMultipleVariablesPerSampleFieldSetMapper
import org.transmartproject.batch.highdim.jobparams.StandardHighDimDataParametersModule
import org.transmartproject.batch.highdim.platform.annotationsload.AnnotationEntity
import org.transmartproject.batch.highdim.platform.annotationsload.AnnotationEntityMap
import org.transmartproject.batch.highdim.platform.annotationsload.GatherAnnotationEntityIdsReader
import org.transmartproject.batch.support.JobParameterFileResource

/**
 * Spring context for MAF data loading steps.
 */
@Configuration
@ComponentScan
@Import(DbConfig)
@Slf4j
class MafStepsConfig implements StepBuildingConfigurationTrait {

    public static final int CHUNK_SIZE = 100

    @Autowired
    DatabaseImplementationClassPicker picker

    @Bean
    Step firstPass() {
        Tasklet noImplementationTasklet = new Tasklet() {
            @Override
            RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                log.info('No first pass.')
            }
        }

        steps.get('firstPass')
                .tasklet(noImplementationTasklet)
                .build()
    }

    @Bean
    Step secondPass() {
        steps.get('secondPass')
                .chunk(CHUNK_SIZE)
                .reader(mafFileReader())
                .writer(mafDataWriter())
                .build()
    }

    @Bean
    Step deleteHdData(CurrentAssayIdsReader currentAssayIdsReader) {
        steps.get('deleteHdData')
                .chunk(100)
                .reader(currentAssayIdsReader)
                .writer(deleteMafDataWriter())
                .build()
    }

    @Bean
    ItemWriter deleteMafDataWriter() {
        new DeleteByColumnValueWriter<Long>(
                table: Tables.MAF_MUTATION,
                column: 'assay_id',
                entityName: 'maf data points')
    }

    @Bean
    Step partitionDataTable() {
        Tasklet noImplementationTasklet = new Tasklet() {
            @Override
            RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                log.info('Data table partition is not supported by this data type.')
            }
        }

        steps.get('partitionDataTable')
                .tasklet(noImplementationTasklet)
                .build()
    }

    @Bean
    Step loadMafMutationEventsStep() {
        steps.get('loadMafMutationEvents')
                .allowStartIfComplete(true)
                .chunk(100)
                .reader(mafMutationEventsReader())
                .writer(new PutInBeanWriter(bean: mafMutationEventSet()))
                .listener(logCountsStepListener())
                .build()
    }

    @Bean
    @JobScope
    MafMutationEventSet mafMutationEventSet() {
        new MafMutationEventSet()
    }

    @Bean
    @JobScope
    MafMutationEventsReader mafMutationEventsReader() {
        new MafMutationEventsReader()
    }

    @Bean
    @JobScopeInterfaced
    org.springframework.core.io.Resource dataFileResource() {
        new JobParameterFileResource(
                parameter: StandardHighDimDataParametersModule.DATA_FILE)
    }

    @Bean
    ItemStreamReader mafFileReader() {
        tsvFileReader(
                dataFileResource(),
                mapper: new MafMutationFieldSetMapper(),
                columnNames: 'auto',
                //Skip MAF header. The fist line with version skipped in any case as it's comment (starts with #)
                linesToSkip: 1,
                saveState: false,
                emptyStringsToNull: true)
    }

    @Bean
    @JobScope
    MafDataWriter mafDataWriter() {
        new MafDataWriter()
    }
}
