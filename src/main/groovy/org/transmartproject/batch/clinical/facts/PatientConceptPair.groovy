package org.transmartproject.batch.clinical.facts

import org.transmartproject.batch.concept.ConceptNode
import org.transmartproject.batch.patient.Patient

/**
 * Represents a primary key of the observation fact
 */
class PatientConceptPair {
    Patient patient
    ConceptNode conceptNode
}
