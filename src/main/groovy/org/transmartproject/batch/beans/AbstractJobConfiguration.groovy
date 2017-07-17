package org.transmartproject.batch.beans

import org.springframework.batch.core.Job
import org.springframework.batch.core.Step
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.MessageSource
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import org.springframework.core.convert.converter.Converter
import org.transmartproject.batch.AppConfig
import org.transmartproject.batch.db.DatabaseImplementationClassPicker

import java.nio.file.Path

/**
 * Base class for Spring context configuration classes for Jobs.
 * Each job type should have its own configuration, extended from this class.
 */
@Import(AppConfig)
@ComponentScan([
        'org.transmartproject.batch.db',
        'org.transmartproject.batch.secureobject',
        'org.transmartproject.batch.biodata',
])
abstract class AbstractJobConfiguration implements StepBuildingConfigurationTrait {

    @Autowired
    JobBuilderFactory jobs

    @Autowired
    MessageSource validationMessageSource

    @Autowired
    DatabaseImplementationClassPicker picker

    abstract Job job()

}

/* needed so the runtime can know the generic types */

interface StringToPathConverter extends Converter<String, Path> {}

class OverriddenNameStep implements Step {
    @Delegate
    Step step

    String newName

    @Override
    String getName() {
        newName
    }
}
