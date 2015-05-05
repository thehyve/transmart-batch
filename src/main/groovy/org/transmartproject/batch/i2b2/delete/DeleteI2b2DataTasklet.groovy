package org.transmartproject.batch.i2b2.delete

import groovy.util.logging.Slf4j
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.stereotype.Component
import org.springframework.util.Assert
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.clinical.db.objects.Tables

import javax.annotation.PostConstruct

/**
 * Delete all previous data. This is done based on the source system.
 */
@Component
@JobScopeInterfaced
@Slf4j
class DeleteI2b2DataTasklet implements Tasklet {

    private static final String SQL_STATEMENT =
            'DELETE FROM {table} WHERE sourcesystem_cd = :sourceSystem'

    @Value("#{jobParameters[SOURCE_SYSTEM]}")
    String sourceSystem

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate

    @PostConstruct
    void init() {
        Assert.notNull(sourceSystem)
    }

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        def tables = [
                Tables.OBSERVATION_FACT,
                Tables.PATIENT_MAPPING,
                Tables.ENCOUNTER_MAPPING,
                Tables.PATIENT_DIMENSION,
                Tables.VISIT_DIMENSION,
                Tables.PROV_DIMENSION,
        ]

        tables.each { t ->
            int affected = jdbcTemplate.update SQL_STATEMENT.replace('{table}', t),
                    [sourceSystem: sourceSystem]
            log.info("Deleted $affected rows from $t")
            contribution.incrementWriteCount(affected)
        }

        RepeatStatus.FINISHED
    }
}
