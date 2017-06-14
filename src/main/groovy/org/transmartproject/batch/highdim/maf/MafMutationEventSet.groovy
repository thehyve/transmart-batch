package org.transmartproject.batch.highdim.maf

import com.google.common.collect.Maps
import org.transmartproject.batch.highdim.platform.annotationsload.AnnotationEntity

/**
 * Collects the MAF events
 */
class MafMutationEventSet {

    Map<List, MutationEvent> map = Maps.newHashMap()

    void leftShift(MutationEvent mutationEvent) {
        map[mutationEvent.uniqueKey] = mutationEvent
    }

    MutationEvent getAt(List key) {
        map[key]
    }
}
