package org.transmartproject.batch.highdim.datastd

import org.springframework.batch.item.ItemStreamSupport
import org.springframework.batch.item.file.transform.FieldSet
import org.springframework.batch.item.validator.ValidationException
import org.transmartproject.batch.batchartifacts.AbstractSplittingItemReader

/**
 * Checks that the line for a probe has enough values for the variance to be
 * calculated.
 */
class CheckNumberOfValuesValidatingListener extends ItemStreamSupport implements
        AbstractSplittingItemReader.EagerLineListener<DataPoint> {
	//parameter for how to treat zero values:
	String treatZeroAs
	
    @Override
    void onLine(FieldSet fieldSet, Collection<DataPoint> items) {
        int c = 0
        int totalZeroItems = 0
        for (DataPoint p in items) {
            if (p.value > 0) {
                c++
            }
            if (p.value == 0) {
            	totalZeroItems++
            }

            //two valid scenarios : 
            //(1) there are other 0 values, and zero should be seen as a value
            //(2) there are two or more values
            if (totalZeroItems > 1 && treatZeroAs == 'VALUE') {
            	return
            }            	
            else if (c >= 2) {
                return
            }
            
        }

        String annotation = fieldSet.readString(0)
        throw new ValidationException("The annotation $annotation does not " +
                "have enough values for the statistics to be " +
                "calculated (needs at least 2, got $c). Check your values and/or the 'TREAT_ZERO_AS' setting.")
    }
}
