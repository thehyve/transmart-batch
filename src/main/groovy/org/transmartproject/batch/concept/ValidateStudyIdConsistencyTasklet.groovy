package org.transmartproject.batch.concept

import groovy.util.logging.Slf4j
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.clinical.db.objects.Tables

/**
 * Throws an {@link IllegalStateException} if the study ids don't match between uploaded data and existing study in db.
 */
@Component
@JobScopeInterfaced
@Slf4j
class ValidateStudyIdConsistencyTasklet implements Tasklet {

    @Value("#{jobParameters['TOP_NODE']}")
    ConceptPath topNode

    @Value("#{jobParameters['STUDY_ID']}")
    String studyId

    @Autowired
    ConceptTree conceptTree

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ConceptNode studyNode = conceptTree.findStudyNode(topNode)

        if (!studyNode) {
            log.info("It's a new study. No need to check study ids contradiction.")
            return RepeatStatus.FINISHED
        }

        String studyIdInDb = jdbcTemplate.queryForObject("""
          SELECT sourcesystem_cd FROM ${Tables.I2B2}
        WHERE c_fullname = :c_fullname""",
                [c_fullname: studyNode.path.toString()],
                String)

        if (studyIdInDb != studyId) {
            throw new IllegalStateException(
                    "'${studyNode.path}' node has '${studyIdInDb}' as study id." +
                            "But you try to add a data with '${studyId}' as study id.")
        }
        log.info("Study ids match.")

        RepeatStatus.FINISHED
    }
}
