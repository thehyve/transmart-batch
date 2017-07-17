package org.transmartproject.batch.concept

import org.springframework.batch.core.Step
import org.springframework.batch.core.job.builder.FlowBuilder
import org.springframework.batch.core.job.flow.Flow
import org.springframework.batch.core.job.flow.support.SimpleFlow
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.beans.StepBuildingConfigurationTrait
import org.transmartproject.batch.concept.oracle.OracleInsertConceptCountsTasklet
import org.transmartproject.batch.concept.postgresql.PostgresInsertConceptCountsTasklet
import org.transmartproject.batch.db.DatabaseImplementationClassPicker
import org.transmartproject.batch.db.DbConfig
import org.transmartproject.batch.secureobject.SecureObjectConfig

/**
 * Concept spring configuration
 */
@Configuration
@ComponentScan
@Import([DbConfig, SecureObjectConfig])
class ConceptStepsConfig implements StepBuildingConfigurationTrait {

    @Autowired
    DatabaseImplementationClassPicker picker

    @Bean
    Step gatherCurrentConcepts(Tasklet gatherCurrentConceptsTasklet) {
        allowStartStepOf('gatherCurrentConcepts', gatherCurrentConceptsTasklet)
    }

    @Bean
    Step validateTopNodePreexistence(Tasklet validateTopNodePreexistenceTasklet) {
        allowStartStepOf('validateTopNodePreexistence', validateTopNodePreexistenceTasklet)
    }

    @Bean
    Step insertConcepts(Tasklet insertConceptsTasklet) {
        allowStartStepOf('insertConcepts', insertConceptsTasklet)
    }

    @Bean
    Step refreshConceptCounts() {
        Flow refreshConceptCountsFlow = new FlowBuilder<SimpleFlow>('refreshConceptCountsFlow')
                .start(deleteStudyConceptCountsStep(null))
                .next(calclculateAndInsertStudyConceptCountsStep(null))
                .build()

        steps.get('refreshConceptCounts')
                .flow(refreshConceptCountsFlow)
                .build()
    }

    @Bean
    @JobScopeInterfaced
    Tasklet calclculateAndInsertStudyConceptCountsWithSqlTasklet() {
        picker.instantiateCorrectClass(
                OracleInsertConceptCountsTasklet,
                PostgresInsertConceptCountsTasklet)
    }

    @Bean
    Step calclculateAndInsertStudyConceptCountsStep(Tasklet calclculateAndInsertStudyConceptCountsWithSqlTasklet) {
        allowStartStepOf('calclculateAndInsertStudyConceptCountsStep',
                calclculateAndInsertStudyConceptCountsWithSqlTasklet)
    }

    @Bean
    @JobScopeInterfaced
    Tasklet deleteStudyConceptCountsTasklet() {
        new DeleteStudyConceptCountsTasklet()
    }

    @Bean
    Step deleteStudyConceptCountsStep(Tasklet deleteStudyConceptCountsTasklet) {
        allowStartStepOf('deleteStudyConceptCountsStep', deleteStudyConceptCountsTasklet)
    }

}
