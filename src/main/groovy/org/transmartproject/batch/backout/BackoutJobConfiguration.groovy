package org.transmartproject.batch.backout

import groovy.transform.TypeChecked
import org.springframework.batch.core.ExitStatus
import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.support.SimpleFlow
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.support.CompositeItemWriter
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.FilterType
import org.springframework.context.annotation.Import
import org.transmartproject.batch.batchartifacts.IterableItemReader
import org.transmartproject.batch.batchartifacts.LogCountsStepListener
import org.transmartproject.batch.beans.AbstractJobConfiguration
import org.transmartproject.batch.concept.ConceptStepsConfig
import org.transmartproject.batch.support.ExpressionResolver

import javax.annotation.Resource

import static org.transmartproject.batch.backout.NextBackoutModuleJobExecutionDecider.statusForModule

/**
 * Main configuration for backout job.
 */
@Configuration
@ComponentScan(value = 'org.transmartproject.batch',
        includeFilters = [
                @ComponentScan.Filter(
                        type = FilterType.ASSIGNABLE_TYPE,
                        value = BackoutModule),
                @ComponentScan.Filter(
                        type = FilterType.ANNOTATION,
                        value = BackoutComponent)
        ],
        useDefaultFilters = false
)
@TypeChecked
@Import(ConceptStepsConfig)
class BackoutJobConfiguration extends BackoutJobConfigurationParent {

    public static final String JOB_NAME = 'BackoutJob'
    public static final int DELETE_CONCEPTS_AND_FACTS_CHUNK_SIZE = 200

    @Resource
    Step deleteStudyConceptCountsStep

    @Resource
    Step calclculateAndInsertStudyConceptCountsStep

    @Bean(name = 'BackoutJob')
    @Override
    Job job() {
        jobs.get(JOB_NAME)
                .start(mainFlow(null, null, null, null))
                .end()
                .build()
    }

    @Bean
    Flow mainFlow(List<BackoutModule> backoutModules,
                  Tasklet gatherCurrentConceptsTasklet,
                  Tasklet validateTopNodePreexistenceTasklet,
                  NextBackoutModuleJobExecutionDecider decider) {

        def flowBuilder = new FlowBuilder<SimpleFlow>('mainFlow')
                .start(allowStartStepOf('gatherCurrentConcepts', gatherCurrentConceptsTasklet))
                .next(stepOf('validateTopNode', validateTopNodePreexistenceTasklet))
                .next(deleteStudyConceptCountsStep)
                .next(decider)

        backoutModules.each { BackoutModule mod ->
            def presenceStep = wrapStepWithName(
                    "${mod.dataTypeName}.presence",
                    mod.detectPresenceStep())
            def deleteDataStep = wrapStepWithName(
                    "${mod.dataTypeName}.deleteData",
                    mod.deleteDataStep())

            def subFlow = new FlowBuilder<SimpleFlow>("flow.${mod.dataTypeName}")
                    .start(presenceStep)
                    .on(BackoutModule.FOUND.exitCode).to(deleteDataStep)
                    .from(presenceStep).on(ExitStatus.COMPLETED.exitCode).end()
                    .from(presenceStep).on('*').fail().build()

            flowBuilder
                    .on(statusForModule(mod))
                    .to(subFlow)
                    .on(ExitStatus.COMPLETED.exitCode)
                    .to(decider)
        }

        flowBuilder.on(ExitStatus.COMPLETED.exitCode).to(calclculateAndInsertStudyConceptCountsStep).build()
    }

    @Bean
    @JobScope
    Step deleteConceptsAndFactsStep(
            DeleteFactsWriter deleteFactsWriter,
            DeleteConceptWriter deleteConceptWriter,
            DeleteI2b2TagsWriter deleteI2b2TagsWriter) {

        steps.get('deleteConceptsAndFacts')
                .chunk(DELETE_CONCEPTS_AND_FACTS_CHUNK_SIZE)
                .reader(conceptPathsToDeleteItemReader(null))
                .writer(new CompositeItemWriter(delegates: [
                        deleteFactsWriter,
                        deleteConceptWriter,
                        deleteI2b2TagsWriter,]))
                .listener(new LogCountsStepListener())
                .build()
    }

    @Bean
    @StepScope
    IterableItemReader conceptPathsToDeleteItemReader(
            ExpressionResolver expressionResolver
    ) {
        new IterableItemReader(
                name: 'conceptPathsToDelete',
                expressionResolver: expressionResolver,
                expression: '@backoutContext.conceptPathsToDelete')
    }
}

// because we can't repeat @ComponentScan
@ComponentScan([
        'org.transmartproject.batch.backout',
        'org.transmartproject.batch.secureobject',
        'org.transmartproject.batch.biodata',
        'org.transmartproject.batch.concept'])
abstract class BackoutJobConfigurationParent extends AbstractJobConfiguration {}
