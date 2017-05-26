package org.transmartproject.batch.highdim.vcf

import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.step.tasklet.Tasklet
import org.springframework.batch.repeat.RepeatStatus
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.jdbc.core.simple.SimpleJdbcInsert
import org.springframework.stereotype.Component
import org.transmartproject.batch.beans.JobScopeInterfaced
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.highdim.platform.Platform

/**
 * Writes to the vcf data set table.
 */
@Component
@JobScopeInterfaced
class InsertDatasetTasklet implements Tasklet {

    @Value(Tables.VCF_DATASET)
    SimpleJdbcInsert jdbcInsert

    @Autowired
    private Platform platform

    @Value('#{jobExecution.startTime}')
    Date jobStartTime

    @Value("#{jobParameters['STUDY_ID']}")
    String studyId

    @Override
    RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        String datasetId = UUID.randomUUID().toString()
        def jobExecution = chunkContext
                .stepContext
                .stepExecution
                .jobExecution
        jobExecution.executionContext.put('datasetId', datasetId)
        long jobId = jobExecution.id


        int i = jdbcInsert.execute([
                dataset_id: datasetId,
                datasource_id: studyId,
                etl_id: jobId.toString(),
                etl_date: jobStartTime,
                genome: platform.genomeRelease,
                gpl_id: platform.id,
        ])
        contribution.incrementWriteCount(i)

        RepeatStatus.FINISHED
    }

}
