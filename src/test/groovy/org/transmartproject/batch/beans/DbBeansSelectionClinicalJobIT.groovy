package org.transmartproject.batch.beans

import org.gmock.WithGMock
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.springframework.batch.core.JobExecution
import org.springframework.batch.core.JobParameters
import org.springframework.batch.core.scope.context.JobSynchronizationManager
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.clinical.ClinicalDataLoadJobConfiguration
import org.transmartproject.batch.concept.postgresql.PostgresInsertConceptCountsTasklet

import javax.sql.DataSource

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Test the automatic selection of database specific classes.
 */
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = [ClinicalDataLoadJobConfiguration, DbBeansSelectionClinicalJobITConfiguration])
@WithGMock
class DbBeansSelectionClinicalJobIT {

    @Configuration
    static class DbBeansSelectionClinicalJobITConfiguration {
        @Bean
        DataSource dataSource() {
            [:] as DataSource
        }
    }

    private static final String CONFIG_SOURCE_KEY = 'propertySource'

    @Autowired
    ApplicationContext applicationContext

    @BeforeClass
    static void beforeClass() {
        System.setProperty CONFIG_SOURCE_KEY,
                'classpath:/org/transmartproject/batch/beans/DbBeansSelectionClinicalJobIT_pgsql.properties'
    }
    @AfterClass
    static void afterClass() {
        System.setProperty CONFIG_SOURCE_KEY, ''
    }

    @Test
    void testPostgresBeanIsActive() {
        // this is a singleton that proxies the job scoped tasklet
        def singletonBean = applicationContext.getBean('calclculateAndInsertStudyConceptCountsWithSqlTasklet')
        // and this is the job scoped bean name
        def jobScopedBeanName = singletonBean.targetSource.targetBeanName
        JobSynchronizationManager.register(new JobExecution(-1, new JobParameters()))

        def bean = applicationContext.getBean(jobScopedBeanName)

        assertThat bean, instanceOf(PostgresInsertConceptCountsTasklet)
    }
}
