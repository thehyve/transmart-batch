package org.transmartproject.batch.db.postgres

import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.StepContribution
import org.springframework.batch.core.StepExecution
import org.springframework.batch.core.scope.context.ChunkContext
import org.springframework.batch.core.scope.context.StepContext
import org.springframework.batch.item.ExecutionContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.beans.Postgresql
import org.transmartproject.batch.clinical.db.objects.Tables
import org.transmartproject.batch.db.DatabaseImplementationClassPicker
import org.transmartproject.batch.highdim.assays.SaveAssayIdListener

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*
import static org.junit.Assume.assumeTrue

/**
 * Test's postgresql partitioning steps in isolation
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class PostgresTablePartitioningTests {

    @Autowired
    private DatabaseImplementationClassPicker databasePicker

    @Autowired
    protected NamedParameterJdbcTemplate jdbcTemplate

    final String tableName = Tables.MRNA_DATA

    JobExecution jobExecution
    ExecutionContext jobExecutionContext
    StepExecution stepExecution
    ChunkContext chunkContext
    StepContribution stepContribution
    Map<String, Long> sampleToIdMap

    //TODO We might want to come up with solution that the annotations could be applied to the test classes
    @Before
    void before() {
        assumeTrue databasePicker.isCurrentDbms(Postgresql)

        init()
    }

    private void init() {
        jobExecution = new JobExecution(1, new JobParameters(), 'test-job-execution')

        jobExecutionContext = jobExecution.executionContext
        sampleToIdMap = ['a': 100001, b: 100002, c: 100003]
        jobExecutionContext.put(SaveAssayIdListener.MAPPINGS_CONTEXT_KEY, sampleToIdMap)

        stepExecution = new StepExecution('test-step-execution', jobExecution, 1)
        chunkContext = new ChunkContext(new StepContext(stepExecution))
        stepContribution = new StepContribution(stepExecution)
    }

    @After
    void after() {
        dropAllChildTables(tableName)
    }

    @Test
    void testChildTableIsCreated() {
        def tasklet = new CreateAssayBasedPartitionTableTasklet(
                tableName: tableName,
                jdbcTemplate: jdbcTemplate,
        )

        tasklet.execute(stepContribution, chunkContext)

        String partitionTableName = jobExecutionContext.getString('partitionTableName')
        assertThat partitionTableName,
                equalTo("${tableName}_${sampleToIdMap.values().min()}_${sampleToIdMap.values().max()}".toString())
        assertThat getChildTables(tableName), hasItem(equalTo(partitionTableName))
    }

    @Test
    void testApplyConstraints() {
        String partitionTable = "${tableName}_apply_ck"
        List primaryKey = ['assay_id', 'probeset_id']
        jdbcTemplate.update """
                CREATE TABLE ${partitionTable}(
                ) INHERITS (${tableName})""", [:]
        jobExecutionContext.putString('partitionTableName', partitionTable)
        def tasklet = new ApplyConstraintsTasklet(
                jdbcTemplate: jdbcTemplate,
                primaryKey: primaryKey
        )

        tasklet.execute(stepContribution, chunkContext)

        assertThat getTableChecks(partitionTable), contains(allOf(
                containsString("assay_id >= ${sampleToIdMap.values().min()}"),
                containsString("assay_id <= ${sampleToIdMap.values().max()}")))
        assertThat getPrimaryKey(partitionTable), containsInAnyOrder(primaryKey.collect { equalTo(it) })
    }

    void dropAllChildTables(String parentTableName) {
        getChildTables(parentTableName).each { dropTable(it) }
    }

    List<String> getChildTables(String tableName) {
        jdbcTemplate.queryForList('''select cn.nspname || '.' || c.relname as child
                from pg_inherits
                join pg_class as c on inhrelid=c.oid
                join pg_catalog.pg_namespace cn on cn.oid=c.relnamespace
                join pg_class as p ON inhparent=p.oid and p.relname=:parent_relname
                join pg_catalog.pg_namespace pn on pn.oid=p.relnamespace and pn.nspname=:nspname;''',
                [
                        nspname       : Tables.schemaName(tableName),
                        parent_relname: Tables.tableName(tableName)
                ],
                String)
    }

    List<String> getTableChecks(String tableName) {
        jdbcTemplate.queryForList('''select pc.consrc
              from pg_class c
              join pg_catalog.pg_namespace cn on cn.oid=c.relnamespace
              join pg_constraint pc on c.oid = pc.conrelid
              where pc.contype = 'c' and c.relname = :relname and cn.nspname=:nspname;''',
                [
                        nspname: Tables.schemaName(tableName),
                        relname: Tables.tableName(tableName)
                ],
                String)
    }

    List<String> getPrimaryKey(String tableName) {
        jdbcTemplate.queryForList('''select a.attname
              from pg_index i
              join pg_attribute a ON a.attrelid = i.indrelid and a.attnum = any(i.indkey)
              where i.indrelid = :tablename::regclass and i.indisprimary;''',
                [
                        tablename: tableName,
                ],
                String)
    }

    void dropTable(String tableName) {
        jdbcTemplate.update("drop table ${tableName} cascade;", [:])
    }
}
