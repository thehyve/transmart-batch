package org.transmartproject.batch.clinical

import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Value
import org.transmartproject.batch.model.ConceptNode
import org.transmartproject.batch.model.ConceptTree
import org.transmartproject.batch.model.TrialTag
import org.transmartproject.batch.model.WordMapping
import org.transmartproject.batch.support.LineListener
import org.transmartproject.batch.support.LineStepContributionAdapter
import org.transmartproject.batch.support.MappingHelper

/**
 * Tasklet that reads the (cross) trial tags file (if defined), and processes it by setting the tags on the concepts
 */
class ReadTrialTagsTasklet implements Tasklet {

    @Value("#{jobParameters['dataLocation']}")
    String dataLocation

    @Value("#{jobParameters['trialTagFile']}")
    String trialTagFile

    @Value("#{clinicalJobContext.conceptTree}")
    ConceptTree conceptTree

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        File file = MappingHelper.getOptionalFile(dataLocation, trialTagFile)
        if (file) {
            LineListener listener = new LineStepContributionAdapter(contribution)
            List<TrialTag> list = TrialTag.parse(file.newInputStream(), listener)
            list.each {
                //sets the trialTag in the correct concept
                ConceptNode node = conceptTree.study.find(it.category, it.path)
                node.trialTag = it.value
            }
        }

        return RepeatStatus.FINISHED
    }

}
