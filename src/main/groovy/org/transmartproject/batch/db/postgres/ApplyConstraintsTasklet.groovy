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
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.AbstractPartitionTasklet
import org.transmartproject.batch.highdim.assays.SaveAssayIdListener

/**
 * Adds CHECK constraint and primary key (optionally) to the partition table
 */
@Slf4j
@Postgresql
class ApplyConstraintsTasklet implements Tasklet {

    @Autowired
    NamedParameterJdbcTemplate jdbcTemplate

    String assayIdField = 'assay_id'

    /**
     * The primary keys to create on new partitions. List of column names.
     */
    List<String> primaryKey

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

        String partitionTable = jobExecutionContext.getString(AbstractPartitionTasklet.PARTITION_TABLE_NAME)

        def tableNameWoSchema = Tables.tableName(partitionTable)
        def sql = """
                ALTER TABLE ${partitionTable}
                ADD CONSTRAINT ${tableNameWoSchema}_pn_ck
                CHECK (${assayIdField} >= ${minId} AND ${assayIdField} <= ${maxId})"""

        if (primaryKey) {
            sql += ", ADD CONSTRAINT ${tableNameWoSchema}_pk PRIMARY KEY (${primaryKey.join(', ')})"
        }
        sql += ';'

        jdbcTemplate.update sql, [:]



        RepeatStatus.FINISHED
    }
}
