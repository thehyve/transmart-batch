package org.transmartproject.batch.concept

import org.springframework.batch.item.ItemWriter
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.batch.clinical.facts.PatientConceptPair

/**
 * Writer calling registerObservation on BitSetConceptCounts
 */
class BitSetConceptCountsWriter implements ItemWriter<PatientConceptPair> {

    @Autowired
    private BitSetConceptCounts bitSetConceptCounts

    @Override
    void write(List<? extends PatientConceptPair> items) throws Exception {
        items.each { patientConceptPair ->
            bitSetConceptCounts.registerObservation patientConceptPair.patient, patientConceptPair.conceptNode
        }
    }
}
