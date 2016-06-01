package org.transmartproject.batch.db.postgres

import groovy.util.logging.Slf4j
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.item.ExecutionContext
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.transmartproject.batch.beans.Postgresql
import org.transmartproject.batch.db.AbstractPartitionTasklet
import org.transmartproject.batch.highdim.assays.SaveAssayIdListener

/**
 * Creates empty partition table
 */
@Slf4j
@Postgresql
class CreateAssayBasedPartitionTableTasklet implements Tasklet {

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate

    String tableName

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ExecutionContext jobExecutionContext =
                chunkContext.stepContext.stepExecution.jobExecution.executionContext

        Map<String, Long> sampleCodeToAssayIdMap =
                (Map) jobExecutionContext.get(SaveAssayIdListener.MAPPINGS_CONTEXT_KEY)

        assert sampleCodeToAssayIdMap

        Collection<Long> assayIds = sampleCodeToAssayIdMap.values()

        assert assayIds

        long minId = assayIds.min()
        long maxId = assayIds.max()

        String partitionTable = "${tableName}_${minId}_${maxId}"

        log.info "Creating table $partitionTable"
        jdbcTemplate.update """
                CREATE TABLE ${partitionTable}(
                ) INHERITS (${tableName})""", [:]

        jobExecutionContext.put(AbstractPartitionTasklet.PARTITION_TABLE_NAME, partitionTable)

        RepeatStatus.FINISHED
    }
}
