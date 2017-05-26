package org.transmartproject.batch.highdim.vcf

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.support.SimpleFlow
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.transmartproject.batch.highdim.beans.AbstractTypicalHdDataJobConfig
import org.transmartproject.batch.highdim.platform.PlatformStepsConfig

import javax.annotation.Resource

/**
 * Spring context for VCF data loading job.
 */
@Configuration
@Import([
        PlatformStepsConfig,
        VcfStepsConfig,
])
class VcfJobConfig extends AbstractTypicalHdDataJobConfig {

    public static final String JOB_NAME = 'vcfDataLoadJob'

    @Resource
    Step deleteGplInfo
    @Resource
    Step insertGplInfo
    @Resource
    Step insertDataset

    @Resource
    Step loadAnnotationMappings
    @Resource
    Step firstPass
    @Resource
    Step deleteHdData
    @Resource
    Step partitionDataTable
    @Resource
    Step secondPass

    @Bean
    Job vcfDataLoadJob() {
        //fill in deapp.de_variant_subject_idx
        def vcfDataUploadFlow = new FlowBuilder<SimpleFlow>('vcfDataUploadFlow')
                //TODO introduce ensureGplInfo
                .start(deleteGplInfo)
                .next(insertGplInfo)
                //TODO Remove data set record first
                .next(insertDataset)
                .next(typicalHdDataFlow())
                .build()

        jobs.get(JOB_NAME)
                .start(vcfDataUploadFlow)
                .end()
                .build()
    }
}
