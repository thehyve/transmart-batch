package org.transmartproject.batch.clinical.facts

import com.google.common.collect.Maps
import groovy.transform.TypeChecked
import groovy.util.logging.Slf4j
import org.springframework.batch.core.configuration.annotation.JobScope
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import org.transmartproject.batch.facts.ClinicalFactsRowSet
import org.transmartproject.batch.patient.Patient
import org.transmartproject.batch.clinical.variable.ClinicalVariable
import org.transmartproject.batch.clinical.xtrial.XtrialMappingCollection
import org.transmartproject.batch.clinical.xtrial.XtrialNode
import org.transmartproject.batch.concept.ConceptNode
import org.transmartproject.batch.concept.ConceptTree
import org.transmartproject.batch.concept.ConceptType

/**
 * Creates {@link org.transmartproject.batch.facts.ClinicalFactsRowSet} objects.
 *
 * Because concepts can only be created and assigned types when the data is seen,
 * that is also done here.
 */
@Component
@JobScope
@Slf4j
@TypeChecked
class ClinicalFactsRowSetFactory {

    @Value("#{jobParameters['STUDY_ID']}")
    String studyId

    @Autowired
    ConceptTree tree

    @Autowired
    XtrialMappingCollection xtrialMapping

    private final Map<ConceptNode, XtrialNode> conceptXtrialMap = Maps.newHashMap()

    ClinicalFactsRowSet create(ClinicalDataFileVariables fileVariables,
                               ClinicalDataRow row,
                               Patient patient) {
        ClinicalFactsRowSet result = new ClinicalFactsRowSet()
        result.studyId = studyId

        result.patient = patient
        result.siteId = fileVariables.getSiteId(row)
        result.visitName = fileVariables.getVisitName(row)

        fileVariables.otherVariables.each { ClinicalVariable var ->
            String value = row[var.columnNumber]

            //return on empty string or fully whitespace string
            if (!value || value.trim().length() == 0) {
                return
            }

            //maximum string lenght that we can store is 255.
            //should maybe also trow a warning
            if(value.length() >= 255) { value = value.substring(0,254)  }

            processVariableValue result, var, value
        }

        result
    }


    private void processVariableValue(ClinicalFactsRowSet result,
                                      ClinicalVariable var,
                                      String value) {
        /*
         * Concepts are created and assigned types and ids
         */

        /* we infer the conceptType once we see the first value.
        * Kind of dangerous */
        ConceptType conceptType
        ConceptNode concept = tree[var.conceptPath]

        //System.out.println(var.dataLabel + "\t" + value)

        // if the concept doesn't yet exist (ie first record)
        if(!concept){

            //if no conceptType is set in the columnsfile try to detect the conceptType from the first record
            if(var.conceptType == null)
            {
                conceptType = value.isDouble() ? ConceptType.NUMERICAL : ConceptType.CATEGORICAL
            }
            //if conceptType is set get it from the columnsFile
            else
            {
                conceptType = getConceptTypeFromColumnsFile(var)
            }

            // has the side-effect of assigning type if it's unknown and
            // creating the concept from scratch if it doesn't exist at all
            concept = tree.getOrGenerate(var.conceptPath, conceptType)
        }
        //if the concept does already exist ( ie not first record)
        else
        {
            conceptType = concept.type

            //if the conceptType is detected here (not specified in columns file )check if the current conceptType is the same as the one detected on the first record
            if(var.conceptType == null)
            {
                boolean curValIsNumerical = value.isDouble()

                if (conceptType == ConceptType.NUMERICAL && !curValIsNumerical) {
                    throw new IllegalArgumentException("Variable $var inferred " +
                            "numerical, but got value '$value'" + String.valueOf(value.length()) +"patient " +  String.valueOf(result.patient.id) + var.dataLabel)
                }
            }
        }


        // we need a subnode if the variable is categorical
        if (conceptType == ConceptType.CATEGORICAL) {
            concept = tree.getOrGenerate(var.conceptPath + value, ConceptType.CATEGORICAL)
        }

        tree.reserveIdsFor concept

        result.addValue concept, getXtrialNodeFor(concept), value
    }

    private ConceptType getConceptTypeFromColumnsFile(ClinicalVariable var) {

        ConceptType conceptType
        switch (var.conceptType) {
            case "CATEGORICAL"  : conceptType = ConceptType.CATEGORICAL;
                break;
            case "NUMERICAL"    : conceptType = ConceptType.NUMERICAL;
                break;
            default             : conceptType = ConceptType.UNKNOWN;
                break;
        }
        return conceptType
    }



    XtrialNode getXtrialNodeFor(ConceptNode conceptNode) {
        if (conceptXtrialMap.containsKey(conceptNode)) {
            conceptXtrialMap[conceptNode]
        } else {
            conceptXtrialMap[conceptNode] = xtrialMapping.findMappedXtrialNode(
                    conceptNode.path,
                    conceptNode.type)
        }
    }

}
