package org.transmartproject.batch.highdim.maf

import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.support.SimpleFlow
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.transmartproject.batch.highdim.beans.AbstractTypicalHdDataJobConfig
import org.transmartproject.batch.highdim.platform.PlatformStepsConfig

import javax.annotation.Resource

/**
 * Spring context for MAF data loading job.
 */
@Configuration
@Import([
        PlatformStepsConfig,
        MafStepsConfig,
])
class MafJobConfig extends AbstractTypicalHdDataJobConfig {

    public static final String JOB_NAME = 'mafDataLoadJob'

    @Resource
    Step deleteGplInfo
    @Resource
    Step insertGplInfo

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
    @Resource
    Step checkPlatformNotFound
    @Resource
    Step loadMafMutationEventsStep
    @Autowired
    StepBuilderFactory steps

    @Bean
    Job mafDataLoadJob() {
        SimpleFlow ensurePlatformLoadedFlow = new FlowBuilder<SimpleFlow>('ensurePlatformLoadedFlow')
                .start(checkPlatformNotFound).on('NOT FOUND').to(insertGplInfo)
                .from(checkPlatformNotFound).on(ExitStatus.COMPLETED.exitCode).to(loadMafMutationEventsStep)
                .build()

        Step ensurePlatformLoadedStep = steps.get('ensurePlatformLoadedStep')
                .allowStartIfComplete(true)
                .flow(ensurePlatformLoadedFlow)
                .build()

        def mafDataUploadFlow = new FlowBuilder<SimpleFlow>('mafDataUploadFlow')
                .start(ensurePlatformLoadedStep)
                .next(typicalHdDataFlow())
                .build()

        jobs.get(JOB_NAME)
                .start(mafDataUploadFlow)
                .end()
                .build()
    }

}
