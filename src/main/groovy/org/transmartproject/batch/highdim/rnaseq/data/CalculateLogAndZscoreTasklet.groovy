package org.transmartproject.batch.highdim.rnaseq.data

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
 * Fill `log_normalized_readcount` and `zscore`
 */
@Component
@JobScopeInterfaced
@Slf4j
class CalculateLogAndZscoreTasklet implements Tasklet {

    @Value("#{jobParameters['STUDY_ID']}")
    private String studyId

    int logBase = 2

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        int i = jdbcTemplate.update("""
                UPDATE ${Tables.RNASEQ_DATA}
                SET log_normalized_readcount = log(:logBase, cast(normalized_readcount as numeric))
                WHERE trial_name = :studyId and normalized_readcount > 0
        """, [studyId: studyId, logBase: logBase])


        int j = jdbcTemplate.update("""
                UPDATE ${Tables.RNASEQ_DATA}
                SET zscore = V.d / V.std
                FROM (
                      SELECT region_id, assay_id,
                        (S.log_normalized_readcount - avg(S.log_normalized_readcount)
                        over(PARTITION BY S.region_id)) as d,
                       stddev_samp(S.log_normalized_readcount) over(PARTITION BY S.region_id) as std
                      FROM ${Tables.RNASEQ_DATA} S
                      WHERE S.trial_name = :studyId
                ) as V
                WHERE V.std > 0
                AND V.region_id = ${Tables.RNASEQ_DATA}.region_id
                AND V.assay_id = ${Tables.RNASEQ_DATA}.assay_id
        """, [studyId: studyId])

        log.debug("Updated ${i} ${j} records for ${studyId}")

        contribution.incrementWriteCount(i)

        RepeatStatus.FINISHED
    }
}
