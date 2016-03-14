package org.transmartproject.batch.highdim.cnv.data

import groovy.transform.TypeChecked
import org.transmartproject.batch.highdim.jobparams.StandardAssayParametersModule
import org.transmartproject.batch.highdim.jobparams.StandardHighDimDataParametersModule
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.JobSpecification
import org.transmartproject.batch.startup.StudyJobParametersModule

/**
 * External parameters for the copy number value data.
 */
@TypeChecked
class CnvDataJobSpecification implements JobSpecification {

    final List<? extends ExternalJobParametersModule> jobParametersModules = [
            new StudyJobParametersModule(),
            new StandardAssayParametersModule('Cnv'),
            new StandardHighDimDataParametersModule()
    ]

    final Class<?> jobPath = CnvDataJobConfig
}
