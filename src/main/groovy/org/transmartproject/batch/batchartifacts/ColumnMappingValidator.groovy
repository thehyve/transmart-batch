package org.transmartproject.batch.batchartifacts

import org.springframework.validation.Errors
import org.springframework.validation.Validator
import org.transmartproject.batch.clinical.variable.ClinicalVariable

/**
 * Created by wim on 5/6/15.
 */
class ColumnMappingValidator implements Validator {


    @java.lang.Override
    boolean supports(Class<?> clazz) {
       clazz == ClinicalVariable
    }

    @java.lang.Override
    void validate(java.lang.Object target, Errors errors) {

        //accepted conceptTypes in the columnFile are null (ie empty string) or CATEGORICAL or NUMERICAL
        if(target.conceptType != null && target.conceptType != "CATEGORICAL" && target.conceptType != "NUMERICAL" ){
            errors.rejectValue("conceptType","conceptTypeWrong", [target.conceptType] as Object[], null)
        }

    }
}
