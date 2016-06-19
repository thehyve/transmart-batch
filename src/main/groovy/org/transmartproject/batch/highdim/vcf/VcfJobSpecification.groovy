package org.transmartproject.batch.highdim.vcf

import groovy.transform.TypeChecked
import org.transmartproject.batch.highdim.jobparams.StandardAssayParametersModule
import org.transmartproject.batch.highdim.jobparams.StandardHighDimDataParametersModule
import org.transmartproject.batch.highdim.platform.PlatformParametersModule
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.JobSpecification
import org.transmartproject.batch.startup.StudyJobParametersModule

/**
 * External parameters for vcf data.
 */
@TypeChecked
class VcfJobSpecification implements JobSpecification {

    final List<? extends ExternalJobParametersModule> jobParametersModules = [
            new StudyJobParametersModule(),
            new StandardAssayParametersModule(),
            new StandardHighDimDataParametersModule(),
            new PlatformParametersModule('VCF')
    ]

    final Class<?> jobPath = VcfJobConfig
}
