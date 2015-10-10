package org.transmartproject.batch.highdim.datastd

import org.springframework.batch.item.ItemReader
import org.springframework.beans.factory.annotation.Autowired

/**
 * Calculates log intensity and zscore; converts non-positives to NaNs.
 *
 * No state, so can be singleton.
 *
 * Cannot be a processor because spring batch reads a bunch of items and then
 * passes them one by one to the processor instead of reading one item and
 * passing it immediately to the processor before reading another. By that time,
 * the statistics have probably changed already.
 */
class TripleDataValueWrappingReader implements ItemReader<TripleStandardDataValue> {

    private static final double LOG_2 = Math.log(2)

    ItemReader<TripleStandardDataValue> delegate
	//parameter for how to treat zero values:
  	String treatZeroAs

    @Autowired
    private PerDataRowLog2StatisticsListener statisticsListener


    private double clamp(double lowerBound, double upperBound, double value) {
        Math.min(upperBound, Math.max(lowerBound, value))
    }

    @Override
    TripleStandardDataValue read() throws Exception {
        TripleStandardDataValue item = delegate.read()
        if (item != null) {
            process item
        }
    }

    private TripleStandardDataValue process(TripleStandardDataValue item) throws Exception {
        if ((item.value == 0 && treatZeroAs == 'NULL') || item.value < 0 || Double.isNaN(item.value)) {
            item.value = item.logValue = item.zscore = Double.NaN
            return item
        }
        else if (item.value == 0 && treatZeroAs == 'VALUE') {
        	item.value = 0
        	//don't calculate logValue and zscore of logValue...as this is not possible when value == 0,
        	//but set to null, so that value=0 gets into DB while logValue and zscore are set to null (DB accepts null, not NaN)
        	item.logValue = item.zscore = null
            return item
        }

        item.logValue = Math.log(item.value) / LOG_2
        //NB: !! this is the zscore of the logValue.... and not the zscore!! 
        item.zscore = clamp(-2.5d, 2.5d,
                (item.logValue - statisticsListener.mean) /
                        statisticsListener.standardDeviation)
        //the zscore of the logValue will be NaN if there is another NaN value in the 
        //statisticsListener collection. This can happen when adding the log of a 0 value 
        //to the statisticsListener in PerDataRowLog2StatisticsListener.
        //Here we set it to null, so that it can get into DB. 
        //TODO - perhaps do this at a lower level / closer to the insert statement? But this would mean multiple changes in 
        //        all [Proteomics,Metabolomics,etc]DataConverter classes.
        if (Double.isNaN(item.zscore))
        	item.zscore = null
        item
    }
}
