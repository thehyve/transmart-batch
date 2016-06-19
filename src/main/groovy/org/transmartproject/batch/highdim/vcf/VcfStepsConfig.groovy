package org.transmartproject.batch.highdim.vcf

import groovy.util.logging.Slf4j
import htsjdk.variant.variantcontext.VariantContext
import org.springframework.batch.core.Step
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.core.step.tasklet.TaskletStep
import org.springframework.batch.item.ItemProcessor
import org.springframework.batch.item.ItemWriter
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.beans.StepBuildingConfigurationTrait
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.DbConfig
import org.transmartproject.batch.db.DeleteByColumnValueWriter
import org.transmartproject.batch.highdim.assays.CurrentAssayIdsReader
import org.transmartproject.batch.highdim.jobparams.StandardHighDimDataParametersModule
import org.transmartproject.batch.support.JobParameterFileResource

/**
 * Spring context for miRNA data loading steps.
 */
@Configuration
@ComponentScan
@Import(DbConfig)
@Slf4j
class VcfStepsConfig implements StepBuildingConfigurationTrait {

    @Bean
    Step firstPass() {
        TaskletStep step = steps.get('firstPass')
                .chunk(100)
                .reader(variantItemReader())
                .processor(new ItemProcessor<VariantContext, VariantContext>() {
            @Override
            VariantContext process(VariantContext item) throws Exception {
                log.info(item.toString())
                item
            }
        })
                .build()

        step
    }

    @Bean
    Step deleteHdData(CurrentAssayIdsReader currentAssayIdsReader) {
        steps.get('deleteHdData')
                .chunk(100)
                .reader(currentAssayIdsReader)
                .writer(deleteCurrentDataWriter)
                .build()
    }

    @Bean
    Step secondPass(InsertVariantWriter insertVariantWriter) {
        TaskletStep step = steps.get('secondPass')
                .chunk(100)
                .reader(variantItemReader())
                .writer(insertVariantWriter)
                .build()

        step
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
    Step loadAnnotationMappings() {
        Tasklet noImplementationTasklet = new Tasklet() {
            @Override
            RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                log.info('No annotations for the vcf data type exist.')
            }
        }

        steps.get('loadAnnotationMappings')
                .tasklet(noImplementationTasklet)
                .build()
    }

    @Bean
    VariantItemReader variantItemReader() {
        new VariantItemReader(vcfResource: dataFileResource())
    }

    @Bean
    @JobScopeInterfaced
    org.springframework.core.io.Resource dataFileResource() {
        new JobParameterFileResource(
                parameter: StandardHighDimDataParametersModule.DATA_FILE)
    }

    @Bean
    ItemWriter getDeleteCurrentDataWriter() {
        new DeleteByColumnValueWriter<Long>(
                table: Tables.VCF_DATA,
                column: 'assay_id',
                entityName: 'vcf data points')
    }
}
