package org.transmartproject.batch.backout

import com.google.common.collect.ImmutableSet
import groovy.util.logging.Slf4j
import org.springframework.batch.core.configuration.annotation.StepScope
import org.springframework.batch.item.ExecutionContext
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.transmartproject.batch.concept.ConceptPath

/**
 * Management of some state in the backout job.
 */
@StepScope
@Component
@Slf4j
class BackoutContext {

    public final static String KEY_CONCEPTS_TO_DELETE = 'deleteConceptsAndFacts.listOfConcepts'

    @Value('#{stepExecution.executionContext}')
    ExecutionContext stepExecutionContext

    @Value('#{stepExecution.jobExecution.executionContext}')
    ExecutionContext jobExecutionContext

    void setConceptsToDeleteBeforePromotion(Set<ConceptPath> conceptPaths) {
        // use the ExecutionContextPromotionListener to promote this to the
        // job execution context
        stepExecutionContext.put(KEY_CONCEPTS_TO_DELETE,
                ImmutableSet.copyOf(conceptPaths))
    }

    Set<ConceptPath> getConceptPathsToDelete() {
        ImmutableSet.copyOf(
                jobExecutionContext.get(KEY_CONCEPTS_TO_DELETE))
    }

}
