package org.transmartproject.batch.concept

import groovy.util.logging.Slf4j
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.repeat.RepeatStatus
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.GenericTableUpdateTasklet

import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * Deletes concept counts for a study</br>
 * This will delete counts for all kinds of leaf concepts (both lowdim and highdim)
 */
@Slf4j
class DeleteStudyConceptCountsTasklet extends GenericTableUpdateTasklet {

    final String sql = """
        DELETE
        FROM ${Tables.CONCEPT_COUNTS}
        WHERE concept_path in (select concept_path from ${Tables.CONCEPT_DIMENSION} where sourcesystem_cd = ?)"""

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        log.info("About to delete concept counts for study ${studyId}")
        super.execute(contribution, chunkContext)
    }

    @Override
    void setValues(PreparedStatement ps) throws SQLException {
        ps.setString(1, studyId)
    }
}
