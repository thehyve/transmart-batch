package org.transmartproject.batch.highdim.platform

import com.google.common.collect.ImmutableList
import org.transmartproject.batch.startup.ExternalJobParametersModule
import org.transmartproject.batch.startup.JobSpecification

/**
 * Platform for loading an annotation.
 */
abstract class AbstractPlatformJobSpecification implements JobSpecification {

    abstract String getMarkerType()

    final List<? extends ExternalJobParametersModule> jobParametersModules =
            ImmutableList.of(new PlatformParametersModule(markerType), new AnnotationPlatformFileParametersModule())
}
