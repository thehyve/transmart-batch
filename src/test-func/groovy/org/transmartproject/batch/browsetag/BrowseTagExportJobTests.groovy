package org.transmartproject.batch.browsetag

import groovy.util.logging.Slf4j
import org.apache.commons.io.FileUtils
import org.junit.BeforeClass
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.springframework.test.context.ContextConfiguration
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner
import org.transmartproject.batch.beans.GenericFunctionalTestConfiguration
import org.transmartproject.batch.startup.RunJob

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

/**
 * Test export of browse tags and browse tag types.
 */
@Slf4j
@RunWith(SpringJUnit4ClassRunner)
@ContextConfiguration(classes = GenericFunctionalTestConfiguration)
class BrowseTagExportJobTests {

    public static final String STUDY_ID = 'GSE8581'

    static TemporaryFolder temporaryFolder
    static String path

    @BeforeClass
    static void prepare() {
        temporaryFolder = new TemporaryFolder()
        temporaryFolder.create()
        path = temporaryFolder.root.toString() + '/browsetagsexport/'
        log.info("Temp path: $path")
        def clinicalDataJob = RunJob.createInstance(
                '-p',
                'studies/GSE8581/clinical.params',
        )
        clinicalDataJob.run()
        def browseTagsJob = RunJob.createInstance(
                '-p',
                'studies/' + STUDY_ID + '/browsetagsexport.params',
                '-d',
                'EXPORT_BROWSE_TAG_TYPES_FILE=' + path + 'browsetagtypes.export.txt',
                '-d',
                'EXPORT_BROWSE_TAGS_FILE=' + path + 'browsetags.export.txt',
        )
        browseTagsJob.run()
    }

    List<String> readExportedTagTypes() {
        File file = new File(path + 'browsetagtypes.export.txt')
        FileUtils.readLines(file)
    }

    List<String> readExportedTags() {
        File file = new File(path + 'browsetags.export.txt')
        FileUtils.readLines(file)
    }

    @Test
    void testTagTypesAreExported() {
        def expected = [
                'STUDY',
                'Study phase',
                'study_phase',
                'NON_ANALYZED_STRING',
                'Y',
                [
                        'Phase 0',
                        'Not applicable',
                        'Life cycle management',
                        'Post market approval',
                        'Regulatory submission',
                        'Phase III',
                        'Phase II',
                        'Phase I',
                        'Preclinical',
                        'Development candidate',
                        'Lead optimization',
                        'Hit finding',
                        'Target identification / validation'
                ].sort().join(','),
                '9'
        ].join('\t')
        assertThat readExportedTagTypes(), hasItems(
            expected
        )
    }

    @Test
    void testTagsAreExported() {
        def expected = [
                'concept_key','tag_title','tag_description','index'
        ].join('\t')
        assertThat readExportedTags(), hasItems(
                expected
        )
    }

}
