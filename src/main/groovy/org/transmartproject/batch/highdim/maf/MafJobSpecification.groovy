package org.transmartproject.batch.highdim.maf

import groovy.transform.TypeChecked
import org.transmartproject.batch.highdim.jobparams.StandardAssayParametersModule
import org.transmartproject.batch.highdim.jobparams.StandardHighDimDataParametersModule
import org.transmartproject.batch.highdim.mirna.data.MirnaDataJobConfig
import org.transmartproject.batch.highdim.platform.PlatformParametersModule
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.JobSpecification
import org.transmartproject.batch.startup.StudyJobParametersModule

/**
 * External parameters for MAF data upload.
 */
@TypeChecked
class MafJobSpecification implements JobSpecification {

    final List<? extends ExternalJobParametersModule> jobParametersModules = [
            new StudyJobParametersModule(),
            new StandardAssayParametersModule(),
            new StandardHighDimDataParametersModule(),
            new PlatformParametersModule('MAF')
    ]

    final Class<?> jobPath = MafJobConfig
}